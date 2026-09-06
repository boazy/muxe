//! Deterministic inventory generator for the pinned Zellij plugin API.
//!
//! The tool parses Rust with `syn`; it never invokes Zellij's private ABI or
//! constructs plugin protobuf messages. Its checked-in policies make public API
//! classification and Action-field converter coverage fail closed on pin drift.

mod classification;

use std::{
    collections::{BTreeMap, BTreeSet},
    env,
    fmt::Write as _,
    fs,
    path::{Path, PathBuf},
    process::{Command, ExitCode},
    str::FromStr,
    time::{SystemTime, UNIX_EPOCH},
};

use eyre::{bail, Result, WrapErr};
use quote::ToTokens;
use serde::Deserialize;
use sha2::{Digest, Sha256};
use syn::{Attribute, Fields, FnArg, Item, ItemEnum, ItemFn, ItemStruct, ItemType, Pat, ReturnType, Type, Variant, Visibility};

use crate::classification::{ConverterClass, FunctionClass};

const DEFAULT_SOURCE_ROOT: &str = "fixtures/zellij/0.46.0/source";
const DEFAULT_PIN: &str = "pins/zellij.toml";
const DEFAULT_FUNCTION_POLICY: &str = "fixtures/zellij/0.46.0/public-functions.policy";
const DEFAULT_CONVERTER_POLICY: &str = "fixtures/zellij/0.46.0/action-converters.policy";
const DEFAULT_OUTPUT: &str = "fixtures/zellij/0.46.0/action-inventory.rs";
const FIXTURE_HASHES: &str = "source-inputs.sha256";
const ACTION_SOURCE: &str = "zellij-utils/src/input/actions.rs";
const SHIM_SOURCE: &str = "zellij-tile/src/shim.rs";

const MIRROR_SOURCES: &[&str] = &[
    ACTION_SOURCE,
    "zellij-utils/src/data.rs",
    "zellij-utils/src/input/command.rs",
    "zellij-utils/src/input/layout.rs",
    "zellij-utils/src/input/mouse.rs",
    "zellij-utils/src/input/options.rs",
    "zellij-utils/src/position.rs",
];

const BACKGROUND_INTEGRATIONS: &[&str] = &[
    "clear_key_presses_intercepts",
    "exec_cmd",
    "generate_web_login_token",
    "intercept_key_presses",
    "load_new_plugin",
    "object_to_stdout",
    "pipe_message_to_plugin",
    "post_message_to",
    "post_message_to_plugin",
    "reload_plugin_with_id",
    "rename_web_token",
    "report_panic",
    "revoke_all_web_tokens",
    "revoke_web_login_token",
    "run_command",
    "run_command_with_env_variables_and_cwd",
    "set_timeout",
    "share_current_session",
    "start_or_reload_plugin",
    "start_web_server",
    "stop_sharing_current_session",
    "stop_web_server",
    "watch_filesystem",
    "web_request",
];

#[derive(Debug)]
struct Arguments {
    source_root: PathBuf,
    pin: PathBuf,
    function_policy: PathBuf,
    converter_policy: PathBuf,
    output: PathBuf,
    check: bool,
    materialize_fixtures: bool,
    verify_fixtures: bool,
    write_function_template: bool,
    write_converter_template: bool,
}

#[derive(Debug)]
struct Pin {
    repository: String,
    revision: String,
    host_version: String,
    crate_name: String,
    source_inputs: Vec<String>,
}

#[derive(Deserialize)]
struct CargoMetadata {
    packages: Vec<CargoPackage>,
}

#[derive(Deserialize)]
struct CargoPackage {
    name: String,
    source: Option<String>,
    manifest_path: PathBuf,
}

#[derive(Debug)]
struct ActionVariant {
    name: String,
    fields: Vec<String>,
}

struct ShimArgument {
    name: String,
    ty: Type,
}

struct ShimFunction {
    name: String,
    arguments: Vec<ShimArgument>,
    return_type: Option<Type>,
    generic_types: BTreeMap<String, Type>,
}

enum ParsedType {
    Struct(ItemStruct),
    Enum(ItemEnum),
    Alias(ItemType),
}

struct TypeDefinition {
    source: String,
    item: ParsedType,
}

struct MirrorSchema {
    definitions: BTreeMap<String, TypeDefinition>,
    selected: BTreeSet<String>,
}

#[derive(Clone, Copy)]
enum ModelFlavor {
    Raw,
    Validated,
}

fn main() -> ExitCode {
    match run() {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("{error:?}");
            ExitCode::FAILURE
        }
    }
}

fn run() -> Result<()> {
    let arguments = parse_arguments(env::args().skip(1))?;
    let pin = parse_pin(&arguments.pin)?;
    for source in MIRROR_SOURCES {
        require_input(&pin, source)?;
    }
    require_input(&pin, SHIM_SOURCE)?;

    if arguments.materialize_fixtures {
        let source_root = resolved_cargo_source_root(&pin)?;
        materialize_fixture_corpus(&source_root, &arguments.source_root, &pin)?;
        return Ok(());
    }

    if arguments.verify_fixtures {
        let source_root = resolved_cargo_source_root(&pin)?;
        let fresh_root = fresh_fixture_root()?;
        materialize_fixture_corpus(&source_root, &fresh_root, &pin)?;
        if let Err(error) = compare_fixture_corpus(&arguments.source_root, &fresh_root, &pin)
            .and_then(|()| {
                let source_hashes = fixture_source_hashes(&fresh_root, &pin)?;
                let generated = generate_from_source(&arguments, &pin, &fresh_root, &source_hashes)?;
                let existing = read(&arguments.output)?;
                (existing == generated).then_some(()).ok_or_else(|| {
                    eyre::eyre!(
                        "{} differs from a freshly materialized pinned fixture corpus at {}",
                        arguments.output.display(),
                        fresh_root.display()
                    )
                })
            })
        {
            return Err(error);
        }
        fs::remove_dir_all(&fresh_root)
            .wrap_err_with(|| format!("could not remove fresh fixture directory {}", fresh_root.display()))?;
        return Ok(());
    }

    let source_hashes = fixture_source_hashes(&arguments.source_root, &pin)?;
    let generated = generate_from_source(&arguments, &pin, &arguments.source_root, &source_hashes)?;
    if arguments.check {
        let existing = read(&arguments.output)?;
        if existing != generated {
            bail!(
                "{} is not reproducible from pin {}; run muxe-zellij-gen",
                arguments.output.display(),
                arguments.pin.display()
            );
        }
    } else {
        write_file(&arguments.output, generated)?;
    }
    Ok(())
}

fn generate_from_source(
    arguments: &Arguments,
    pin: &Pin,
    source_root: &Path,
    source_hashes: &BTreeMap<String, String>,
) -> Result<String> {
    let actions_source = read(&source_root.join(ACTION_SOURCE))?;
    let shim_source = read(&source_root.join(SHIM_SOURCE))?;
    let action_variants = parse_action_variants(&actions_source)?;
    let direct_types = direct_action_types(&actions_source)?;
    let shim_functions = public_function_specs(&shim_source)?;
    let public_functions = shim_functions.keys().cloned().collect::<BTreeSet<_>>();

    if arguments.write_function_template {
        write_policy_template(
            &arguments.function_policy,
            public_functions.iter().map(String::as_str),
            FunctionClass::Unsupported.as_str(),
        )?;
    }
    if arguments.write_converter_template {
        write_policy_template(
            &arguments.converter_policy,
            direct_types.iter().map(String::as_str),
            ConverterClass::Unsupported.as_str(),
        )?;
    }

    let function_policy = read_policy::<FunctionClass>(&arguments.function_policy)?;
    let converter_policy = read_policy::<ConverterClass>(&arguments.converter_policy)?;
    validate_exact_policy("public function", &public_functions, &function_policy)?;
    validate_exact_policy("Action field type", &direct_types, &converter_policy)?;
    reject_unsupported_converters(&converter_policy)?;
    let mut mirror_roots = direct_types.clone();
    mirror_roots.extend(command_type_roots(&shim_functions, &function_policy));
    let mirror_schema = mirror_schema(source_root, &mirror_roots)?;
    validate_unsupported_function_policy(&function_policy)?;

    generate(
        pin,
        &mirror_schema,
        source_hashes,
        &shim_functions,
        &action_variants,
        &direct_types,
        &public_functions,
        &function_policy,
        &converter_policy,
    )
}

fn parse_arguments(arguments: impl IntoIterator<Item = String>) -> Result<Arguments> {
    let mut result = Arguments {
        source_root: PathBuf::from(DEFAULT_SOURCE_ROOT),
        pin: PathBuf::from(DEFAULT_PIN),
        function_policy: PathBuf::from(DEFAULT_FUNCTION_POLICY),
        converter_policy: PathBuf::from(DEFAULT_CONVERTER_POLICY),
        output: PathBuf::from(DEFAULT_OUTPUT),
        check: false,
        materialize_fixtures: false,
        verify_fixtures: false,
        write_function_template: false,
        write_converter_template: false,
    };
    let mut arguments = arguments.into_iter();
    while let Some(argument) = arguments.next() {
        match argument.as_str() {
            "--source-root" => result.source_root = PathBuf::from(next_value(&mut arguments, "--source-root")?),
            "--pin" => result.pin = PathBuf::from(next_value(&mut arguments, "--pin")?),
            "--function-policy" => result.function_policy = PathBuf::from(next_value(&mut arguments, "--function-policy")?),
            "--converter-policy" => result.converter_policy = PathBuf::from(next_value(&mut arguments, "--converter-policy")?),
            "--output" => result.output = PathBuf::from(next_value(&mut arguments, "--output")?),
            "--check" => result.check = true,
            "--materialize-fixtures" => result.materialize_fixtures = true,
            "--verify-fixtures" => result.verify_fixtures = true,
            "--write-function-template" => result.write_function_template = true,
            "--write-converter-template" => result.write_converter_template = true,
            "--help" | "-h" => {
                println!("Usage: muxe-zellij-gen [--source-root PATH] [--pin PATH] [--function-policy PATH] [--converter-policy PATH] [--output PATH] [--check | --materialize-fixtures | --verify-fixtures] [--write-function-template] [--write-converter-template]");
                std::process::exit(0);
            }
            _ => bail!("unknown argument {argument:?}"),
        }
    }
    let special_modes = u8::from(result.materialize_fixtures) + u8::from(result.verify_fixtures);
    if special_modes > 1 {
        bail!("--materialize-fixtures and --verify-fixtures are mutually exclusive");
    }
    if (result.check || special_modes > 0)
        && (result.write_function_template || result.write_converter_template)
    {
        bail!("--check and fixture modes cannot be combined with template-writing options");
    }
    Ok(result)
}

fn next_value(arguments: &mut impl Iterator<Item = String>, option: &str) -> Result<String> {
    arguments
        .next()
        .ok_or_else(|| eyre::eyre!("{option} requires a path"))
}

fn parse_pin(path: &Path) -> Result<Pin> {
    let source = read(path)?;
    let repository = pin_scalar(&source, "repository")?;
    let revision = pin_scalar(&source, "revision")?;
    let host_version = pin_scalar(&source, "host_version")?;
    let crate_name = pin_scalar(&source, "crate")?;
    let source_inputs = pin_array(&source, "source_inputs")?;
    if revision.len() != 40 || !revision.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        bail!("{} has invalid revision {revision:?}", path.display());
    }
    if !repository.starts_with("https://") {
        bail!("{} has a non-HTTPS repository {repository:?}", path.display());
    }
    Ok(Pin {
        repository,
        revision,
        host_version,
        crate_name,
        source_inputs,
    })
}

fn pin_scalar(source: &str, key: &str) -> Result<String> {
    let prefix = format!("{key} = ");
    let line = source
        .lines()
        .find(|line| line.starts_with(&prefix))
        .ok_or_else(|| eyre::eyre!("pin manifest has no {key:?}"))?;
    let value = line[prefix.len()..].trim();
    value
        .strip_prefix('"')
        .and_then(|value| value.strip_suffix('"'))
        .map(str::to_owned)
        .ok_or_else(|| eyre::eyre!("pin manifest {key:?} must be a quoted string"))
}

fn pin_array(source: &str, key: &str) -> Result<Vec<String>> {
    let prefix = format!("{key} = [");
    let start = source
        .find(&prefix)
        .ok_or_else(|| eyre::eyre!("pin manifest has no {key:?}"))?
        + prefix.len();
    let end = source[start..]
        .find(']')
        .map(|offset| start + offset)
        .ok_or_else(|| eyre::eyre!("pin manifest {key:?} array is not closed"))?;
    let values = source[start..end]
        .split(',')
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(|value| {
            value
                .strip_prefix('"')
                .and_then(|value| value.strip_suffix('"'))
                .map(str::to_owned)
                .ok_or_else(|| eyre::eyre!("pin manifest {key:?} contains a non-string value"))
        })
        .collect::<Result<Vec<_>>>()?;
    if values.is_empty() {
        bail!("pin manifest {key:?} must not be empty");
    }
    Ok(values)
}

fn require_input(pin: &Pin, required: &str) -> Result<()> {
    if pin.source_inputs.iter().any(|input| input == required) {
        Ok(())
    } else {
        bail!("pinned source_inputs must explicitly include {required}")
    }
}

fn resolved_cargo_source_root(pin: &Pin) -> Result<PathBuf> {
    let cargo = env::var_os("CARGO").unwrap_or_else(|| "cargo".into());
    let output = Command::new(cargo)
        .args(["metadata", "--locked", "--format-version", "1"])
        .output()
        .wrap_err("could not run cargo metadata for the pinned Zellij SDK")?;
    if !output.status.success() {
        bail!(
            "cargo metadata could not resolve the pinned Zellij SDK: {}",
            String::from_utf8_lossy(&output.stderr).trim()
        );
    }
    let metadata: CargoMetadata =
        serde_json::from_slice(&output.stdout).wrap_err("cargo metadata returned invalid JSON")?;
    let packages = metadata
        .packages
        .iter()
        .filter(|package| package.name == pin.crate_name)
        .collect::<Vec<_>>();
    if packages.len() != 1 {
        bail!(
            "cargo metadata must resolve exactly one pinned SDK package {:?}, found {}",
            pin.crate_name,
            packages.len()
        );
    }
    let package = packages[0];
    let source = package
        .source
        .as_deref()
        .ok_or_else(|| eyre::eyre!("resolved SDK package {:?} is not a git source", pin.crate_name))?;
    let (source_with_query, resolved_revision) = source
        .rsplit_once('#')
        .ok_or_else(|| eyre::eyre!("resolved SDK source has no revision fragment: {source:?}"))?;
    let source_with_query = source_with_query
        .strip_prefix("git+")
        .ok_or_else(|| eyre::eyre!("resolved SDK source is not git: {source:?}"))?;
    let (resolved_repository, query) = source_with_query
        .split_once('?')
        .ok_or_else(|| eyre::eyre!("resolved SDK source has no revision query: {source:?}"))?;
    if resolved_repository.trim_end_matches(".git") != pin.repository.trim_end_matches(".git")
        || query != format!("rev={}", pin.revision)
        || resolved_revision != pin.revision
    {
        bail!(
            "resolved SDK source differs from pins/zellij.toml: expected repository {:?} and revision {:?}, found {source:?}",
            pin.repository,
            pin.revision
        );
    }
    let source_root = package
        .manifest_path
        .parent()
        .and_then(Path::parent)
        .ok_or_else(|| eyre::eyre!("resolved SDK manifest has no repository root: {}", package.manifest_path.display()))?
        .to_owned();
    source_hashes(&source_root, &pin.source_inputs)?;
    Ok(source_root)
}

fn fixture_source_paths() -> BTreeSet<String> {
    MIRROR_SOURCES
        .iter()
        .copied()
        .chain(std::iter::once(SHIM_SOURCE))
        .map(str::to_owned)
        .collect()
}

fn materialize_fixture_corpus(source_root: &Path, fixture_root: &Path, pin: &Pin) -> Result<()> {
    let hashes = source_hashes(source_root, &pin.source_inputs)?;
    for relative in fixture_source_paths() {
        require_input(pin, &relative)?;
        let source = read(&source_root.join(&relative))?;
        let reduced = if relative == SHIM_SOURCE {
            minimized_shim_source(&source, &relative)?
        } else {
            minimized_model_source(&source, &relative)?
        };
        write_file(&fixture_root.join(relative), reduced)?;
    }
    write_file(&fixture_root.join(FIXTURE_HASHES), render_fixture_hashes(&hashes)?)?;
    Ok(())
}

fn minimized_model_source(source: &str, relative: &str) -> Result<String> {
    let file = syn::parse_file(source)
        .wrap_err_with(|| format!("could not parse pinned fixture source {relative}"))?;
    let mut reduced = String::new();
    let mut retained = 0usize;
    for item in file.items {
        match item {
            Item::Struct(item) => {
                writeln!(reduced, "{}\n", item.to_token_stream())?;
                retained += 1;
            }
            Item::Enum(item) => {
                writeln!(reduced, "{}\n", item.to_token_stream())?;
                retained += 1;
            }
            Item::Type(item) => {
                writeln!(reduced, "{}\n", item.to_token_stream())?;
                retained += 1;
            }
            _ => {}
        }
    }
    if retained == 0 {
        bail!("minimized fixture source {relative} would contain no type declarations");
    }
    format_minimized_fixture(reduced, relative)
}

fn minimized_shim_source(source: &str, relative: &str) -> Result<String> {
    let file = syn::parse_file(source)
        .wrap_err_with(|| format!("could not parse pinned fixture source {relative}"))?;
    let mut reduced = String::new();
    let mut retained = 0usize;
    for item in file.items {
        let Item::Fn(function) = item else {
            continue;
        };
        if !matches!(function.vis, Visibility::Public(_)) {
            continue;
        }
        writeln!(reduced, "pub {} {{}}\n", function.sig.to_token_stream())?;
        retained += 1;
    }
    if retained == 0 {
        bail!("minimized fixture source {relative} would contain no public functions");
    }
    format_minimized_fixture(reduced, relative)
}

fn format_minimized_fixture(reduced: String, relative: &str) -> Result<String> {
    let parsed = syn::parse_file(&reduced)
        .wrap_err_with(|| format!("could not format minimized fixture source {relative}"))?;
    Ok(format!(
        "// Minimized from the exact pinned source: {relative}\n\n{}",
        prettyplease::unparse(&parsed)
    ))
}

fn render_fixture_hashes(hashes: &BTreeMap<String, String>) -> Result<String> {
    let mut rendered = String::from(
        "# SHA-256 of every full upstream source input; generated with --materialize-fixtures.\n",
    );
    for (path, hash) in hashes {
        writeln!(rendered, "{path}\t{hash}")?;
    }
    Ok(rendered)
}

fn fixture_source_hashes(fixture_root: &Path, pin: &Pin) -> Result<BTreeMap<String, String>> {
    let path = fixture_root.join(FIXTURE_HASHES);
    let mut hashes = BTreeMap::new();
    for (line_number, line) in read(&path)?.lines().enumerate() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let (source, hash) = line
            .split_once('\t')
            .ok_or_else(|| eyre::eyre!("{}:{} must be SOURCE<TAB>SHA256", path.display(), line_number + 1))?;
        if source.is_empty()
            || hash.len() != 64
            || !hash.bytes().all(|byte| byte.is_ascii_hexdigit())
        {
            bail!("{}:{} has an invalid source hash", path.display(), line_number + 1);
        }
        if hashes.insert(source.to_owned(), hash.to_owned()).is_some() {
            bail!("{}:{} duplicates fixture source {source:?}", path.display(), line_number + 1);
        }
    }
    let expected = pin.source_inputs.iter().cloned().collect::<BTreeSet<_>>();
    let actual = hashes.keys().cloned().collect::<BTreeSet<_>>();
    if expected != actual {
        bail!(
            "{} source set drifts from pins/zellij.toml: missing [{}]; stale [{}]",
            path.display(),
            expected.difference(&actual).cloned().collect::<Vec<_>>().join(", "),
            actual.difference(&expected).cloned().collect::<Vec<_>>().join(", "),
        );
    }
    Ok(hashes)
}

fn compare_fixture_corpus(expected_root: &Path, fresh_root: &Path, pin: &Pin) -> Result<()> {
    for relative in fixture_source_paths()
        .into_iter()
        .chain(std::iter::once(FIXTURE_HASHES.to_owned()))
    {
        let expected = fs::read(expected_root.join(&relative))
            .wrap_err_with(|| format!("could not read checked-in fixture {}", expected_root.join(&relative).display()))?;
        let fresh = fs::read(fresh_root.join(&relative))
            .wrap_err_with(|| format!("could not read fresh fixture {}", fresh_root.join(&relative).display()))?;
        if expected != fresh {
            bail!(
                "checked-in minimized fixture {} differs from fresh materialization at {}",
                expected_root.join(&relative).display(),
                fresh_root.join(&relative).display()
            );
        }
    }
    fixture_source_hashes(expected_root, pin)?;
    Ok(())
}

fn fresh_fixture_root() -> Result<PathBuf> {
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .wrap_err("system clock predates Unix epoch")?
        .as_nanos();
    let root = env::temp_dir().join(format!("muxe-zellij-fixture-{}-{nonce}", std::process::id()));
    fs::create_dir(&root).wrap_err_with(|| format!("could not create fresh fixture directory {}", root.display()))?;
    Ok(root)
}

fn parse_action_variants(source: &str) -> Result<Vec<ActionVariant>> {
    let action = action_enum(source)?;
    let mut variants = Vec::with_capacity(action.variants.len());
    for variant in action.variants {
        let fields = match variant.fields {
            Fields::Unit => Vec::new(),
            Fields::Unnamed(fields) => fields
                .unnamed
                .into_iter()
                .map(|field| type_text(&field.ty))
                .collect(),
            Fields::Named(fields) => fields
                .named
                .into_iter()
                .map(|field| {
                    let name = field.ident.expect("named syn field has ident");
                    format!("{name}: {}", type_text(&field.ty))
                })
                .collect(),
        };
        variants.push(ActionVariant {
            name: variant.ident.to_string(),
            fields,
        });
    }
    if variants.is_empty() {
        bail!("Action enum has no variants");
    }
    variants.sort_by(|left, right| left.name.cmp(&right.name));
    Ok(variants)
}

fn direct_action_types(source: &str) -> Result<BTreeSet<String>> {
    let action = action_enum(source)?;
    let mut types = BTreeSet::new();
    for variant in action.variants {
        for field in variant.fields {
            collect_type_names(&field.ty, &mut types);
        }
    }
    types.retain(|name| !intrinsic_type(name));
    if types.is_empty() {
        bail!("Action enum has no non-builtin field types");
    }
    Ok(types)
}

fn action_enum(source: &str) -> Result<ItemEnum> {
    let file = syn::parse_file(source).wrap_err("could not parse pinned actions.rs with syn")?;
    file.items

        .into_iter()
        .find_map(|item| match item {
            Item::Enum(item) if item.ident == "Action" => Some(item),
            _ => None,
        })
        .ok_or_else(|| eyre::eyre!("pinned actions.rs has no Action enum"))
}
fn mirror_schema(source_root: &Path, roots: &BTreeSet<String>) -> Result<MirrorSchema> {
    let mut definitions = BTreeMap::new();
    for source in MIRROR_SOURCES {
        let source_path = source_root.join(source);
        let file = syn::parse_file(&read(&source_path)?)
            .wrap_err_with(|| format!("could not parse pinned {}", source_path.display()))?;
        for item in file.items {
            let (name, item) = match item {
                Item::Struct(item) => (item.ident.to_string(), ParsedType::Struct(item)),
                Item::Enum(item) => (item.ident.to_string(), ParsedType::Enum(item)),
                Item::Type(item) => (item.ident.to_string(), ParsedType::Alias(item)),
                _ => continue,
            };
            if let Some(previous) = definitions.insert(
                name.clone(),
                TypeDefinition {
                    source: (*source).to_owned(),
                    item,
                },
            ) {
                bail!(
                    "mirror type {name:?} is declared in both {} and {}",
                    previous.source,
                    source
                );
            }
        }
    }

    let mut pending = roots.clone();
    let mut selected = BTreeSet::new();
    while let Some(name) = pending.pop_first() {
        if builtin_type(&name) || !selected.insert(name.clone()) {
            continue;
        }
        let definition = definitions.get(&name).ok_or_else(|| {
            eyre::eyre!(
                "Action mirror type {name:?} has no declaration in the explicitly pinned mirror sources"
            )
        })?;
        for dependency in type_definition_dependencies(definition) {
            if !builtin_type(&dependency) {
                pending.insert(dependency);
            }
        }
    }
    Ok(MirrorSchema {
        definitions,
        selected,
    })
}

fn type_definition_dependencies(definition: &TypeDefinition) -> BTreeSet<String> {
    let mut dependencies = BTreeSet::new();
    match &definition.item {
        ParsedType::Struct(item) => collect_fields_type_names(&item.fields, &mut dependencies),
        ParsedType::Enum(item) => {
            for variant in &item.variants {
                collect_fields_type_names(&variant.fields, &mut dependencies);
            }
        }
        ParsedType::Alias(item) => collect_type_names(&item.ty, &mut dependencies),
    }
    dependencies
}

fn collect_fields_type_names(fields: &Fields, names: &mut BTreeSet<String>) {
    match fields {
        Fields::Named(fields) => {
            for field in &fields.named {
                collect_type_names(&field.ty, names);
            }
        }
        Fields::Unnamed(fields) => {
            for field in &fields.unnamed {
                collect_type_names(&field.ty, names);
            }
        }
        Fields::Unit => {}
    }
}

fn collect_type_names(ty: &Type, names: &mut BTreeSet<String>) {
    match ty {
        Type::Array(array) => collect_type_names(&array.elem, names),
        Type::BareFn(function) => {
            for input in &function.inputs {
                collect_type_names(&input.ty, names);
            }
            if let syn::ReturnType::Type(_, output) = &function.output {
                collect_type_names(output, names);
            }
        }
        Type::Group(group) => collect_type_names(&group.elem, names),
        Type::Paren(paren) => collect_type_names(&paren.elem, names),
        Type::Path(path) => {
            if let Some(segment) = path.path.segments.last() {
                names.insert(segment.ident.to_string());
                if let syn::PathArguments::AngleBracketed(arguments) = &segment.arguments {
                    for argument in &arguments.args {
                        match argument {
                            syn::GenericArgument::Type(ty) => collect_type_names(ty, names),
                            syn::GenericArgument::AssocType(associated) => collect_type_names(&associated.ty, names),
                            syn::GenericArgument::Constraint(_) | syn::GenericArgument::Const(_) | syn::GenericArgument::Lifetime(_) | syn::GenericArgument::AssocConst(_) | _ => {}
                        }
                    }
                }
            }
        }
        Type::Ptr(pointer) => collect_type_names(&pointer.elem, names),
        Type::Reference(reference) => collect_type_names(&reference.elem, names),
        Type::Slice(slice) => collect_type_names(&slice.elem, names),
        Type::Tuple(tuple) => {
            for element in &tuple.elems {
                collect_type_names(element, names);
            }
        }
        Type::ImplTrait(_) | Type::Infer(_) | Type::Macro(_) | Type::Never(_) | Type::TraitObject(_) | Type::Verbatim(_) => {}
        _ => {}
    }
}

fn builtin_type(name: &str) -> bool {
    name == "PathBuf" || intrinsic_type(name)
}

fn intrinsic_type(name: &str) -> bool {
    matches!(
        name,
        "bool"
            | "char"
            | "f32"
            | "f64"
            | "i8"
            | "i16"
            | "i64"
            | "i128"
            | "isize"
            | "str"
            | "String"
            | "u8"
            | "u16"
            | "u32"
            | "u64"
            | "u128"
            | "usize"
            | "Vec"
            | "Option"
            | "Box"
            | "Result"
            | "Arc"
            | "Rc"
            | "HashMap"
            | "BTreeMap"
            | "HashSet"
            | "BTreeSet"
    )
}

fn public_function_specs(source: &str) -> Result<BTreeMap<String, ShimFunction>> {
    let file = syn::parse_file(source).wrap_err("could not parse pinned shim.rs with syn")?;
    let mut functions = BTreeMap::new();
    for item in file.items {
        let Item::Fn(ItemFn { vis: Visibility::Public(_), sig, .. }) = item else {
            continue;
        };
        let name = sig.ident.to_string();
        let generic_types = shim_generic_types(&sig.generics);
        let arguments = sig
            .inputs
            .iter()
            .map(|argument| match argument {
                FnArg::Typed(argument) => {
                    let name = match &*argument.pat {
                        Pat::Ident(identifier) => identifier.ident.to_string(),
                        pattern => pattern.to_token_stream().to_string(),
                    };
                    Ok(ShimArgument {
                        name,
                        ty: (*argument.ty).clone(),
                    })
                }
                FnArg::Receiver(_) => bail!("public shim function {name:?} has an unexpected receiver"),
            })
            .collect::<Result<Vec<_>>>()?;
        let return_type = match &sig.output {
            ReturnType::Default => None,
            ReturnType::Type(_, ty) => Some((**ty).clone()),
        };
        if functions
            .insert(
                name.clone(),
                ShimFunction {
                    name: name.clone(),
                    arguments,
                    return_type,
                    generic_types,
                },
            )
            .is_some()
        {
            bail!("pinned shim.rs has duplicate public function {name:?}");
        }
    }
    if functions.is_empty() {
        bail!("pinned shim.rs has no public functions");
    }
    Ok(functions)
}

fn shim_generic_types(generics: &syn::Generics) -> BTreeMap<String, Type> {
    let mut types = BTreeMap::new();
    for parameter in &generics.params {
        let syn::GenericParam::Type(parameter) = parameter else {
            continue;
        };
        if let Some(ty) = normalized_trait_bounds(&parameter.bounds) {
            types.insert(parameter.ident.to_string(), ty);
        }
    }
    types
}

fn normalized_trait_bounds(
    bounds: &syn::punctuated::Punctuated<syn::TypeParamBound, syn::token::Plus>,
) -> Option<Type> {
    for bound in bounds {
        let syn::TypeParamBound::Trait(bound) = bound else {
            continue;
        };
        let segment = bound.path.segments.last()?;
        match segment.ident.to_string().as_str() {
            "AsRef" | "Into" => {
                let syn::PathArguments::AngleBracketed(arguments) = &segment.arguments else {
                    continue;
                };
                if let Some(syn::GenericArgument::Type(ty)) = arguments.args.first() {
                    return Some(ty.clone());
                }
            }
            "ToString" => return syn::parse_str::<Type>("String").ok(),
            _ => {}
        }
    }
    None
}

fn command_type_roots(
    shim_functions: &BTreeMap<String, ShimFunction>,
    function_policy: &BTreeMap<String, FunctionClass>,
) -> BTreeSet<String> {
    let mut roots = BTreeSet::new();
    for function in shim_functions.values() {
        if function_policy.get(&function.name) != Some(&FunctionClass::Exposed) {
            continue;
        }
        for argument in &function.arguments {
            collect_type_names(&argument.ty, &mut roots);
        }
        if let Some(return_type) = &function.return_type {
            collect_type_names(return_type, &mut roots);
        }
        for generic in function.generic_types.keys() {
            roots.remove(generic);
        }
    }
    roots.retain(|name| !builtin_type(name));
    roots
}

fn read_policy<T>(path: &Path) -> Result<BTreeMap<String, T>>
where
    T: FromStr<Err = String>,
{
    let source = read(path)?;
    let mut policy = BTreeMap::new();
    for (line_number, line) in source.lines().enumerate() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let (name, class) = line
            .split_once('\t')
            .ok_or_else(|| eyre::eyre!("{}:{} must be NAME<TAB>CLASS", path.display(), line_number + 1))?;
        if name.is_empty() || class.is_empty() {
            bail!("{}:{} has an empty name or class", path.display(), line_number + 1);
        }
        let class = T::from_str(class)
            .map_err(|error| eyre::eyre!("{}:{}: {error}", path.display(), line_number + 1))?;
        if policy.insert(name.to_owned(), class).is_some() {
            bail!("{}:{} duplicates policy entry {name:?}", path.display(), line_number + 1);
        }
    }
    Ok(policy)
}

fn write_policy_template<'a>(
    path: &Path,
    names: impl Iterator<Item = &'a str>,
    default_class: &str,
) -> Result<()> {
    let mut template = String::from("# Generated from the pinned Zellij source. Review every entry before use.\n");
    for name in names {
        writeln!(template, "{name}\t{default_class}")?;
    }
    write_file(path, template)
}

fn validate_exact_policy<T>(
    subject: &str,
    discovered: &BTreeSet<String>,
    policy: &BTreeMap<String, T>,
) -> Result<()> {
    let policy_names = policy.keys().cloned().collect::<BTreeSet<_>>();
    let missing = discovered.difference(&policy_names).collect::<Vec<_>>();
    let stale = policy_names.difference(discovered).collect::<Vec<_>>();
    if !missing.is_empty() || !stale.is_empty() {
        bail!(
            "{subject} policy drift: missing [{}]; stale [{}]",
            missing.iter().map(|name| name.as_str()).collect::<Vec<_>>().join(", "),
            stale.iter().map(|name| name.as_str()).collect::<Vec<_>>().join(", "),
        );
    }
    Ok(())
}


fn reject_unsupported_converters(policy: &BTreeMap<String, ConverterClass>) -> Result<()> {
    let unsupported = policy
        .iter()
        .filter_map(|(name, class)| (*class == ConverterClass::Unsupported).then_some(name.as_str()))
        .collect::<Vec<_>>();
    if unsupported.is_empty() {
        return Ok(());
    }
    bail!(
        "Action field types have no converter: {}",
        unsupported.join(", ")
    )
}

fn validate_unsupported_function_policy(
    policy: &BTreeMap<String, FunctionClass>,
) -> Result<()> {
    let actual = policy
        .iter()
        .filter_map(|(name, class)| (*class == FunctionClass::Unsupported).then_some(name.as_str()))
        .collect::<BTreeSet<_>>();
    let expected = BACKGROUND_INTEGRATIONS.iter().copied().collect::<BTreeSet<_>>();
    if actual != expected {
        let missing = expected.difference(&actual).copied().collect::<Vec<_>>();
        let unexpected = actual.difference(&expected).copied().collect::<Vec<_>>();
        bail!(
            "unsupported public functions must be exactly the approved background integrations; missing [{}]; unexpected [{}]",
            missing.join(", "),
            unexpected.join(", ")
        );
    }
    Ok(())
}

fn source_hashes(source_root: &Path, inputs: &[String]) -> Result<BTreeMap<String, String>> {
    let mut hashes = BTreeMap::new();
    for input in inputs {
        if Path::new(input).is_absolute() || input.split('/').any(|component| component == "..") {
            bail!("pin source input must be a relative in-tree path: {input:?}");
        }
        hashes.insert(input.clone(), sha256_hex(read(&source_root.join(input))?.as_bytes()));
    }
    Ok(hashes)
}

fn generate(
    pin: &Pin,
    mirror_schema: &MirrorSchema,
    source_hashes: &BTreeMap<String, String>,
    shim_functions: &BTreeMap<String, ShimFunction>,
    variants: &[ActionVariant],
    direct_types: &BTreeSet<String>,
    public_functions: &BTreeSet<String>,
    function_policy: &BTreeMap<String, FunctionClass>,
    converter_policy: &BTreeMap<String, ConverterClass>,
) -> Result<String> {
    let mut output = String::from("// @generated by tools/muxe-zellij-gen; do not edit by hand.\n");
    output.push_str("// Regenerate with: cargo run --locked -p muxe-zellij-gen --\n");
    output.push_str("// Raw input validates into a typed model; dispatch converts directly without JSON.\n\n");
    writeln!(output, "pub const PINNED_ZELLIJ_REVISION: &str = {:?};", pin.revision)?;
    writeln!(output, "pub const PINNED_ZELLIJ_VERSION: &str = {:?};\n", pin.host_version)?;
    output.push_str("pub const SOURCE_INPUT_SHA256: &[(&str, &str)] = &[\n");
    for (path, hash) in source_hashes {
        writeln!(output, "    ({path:?}, {hash:?}),")?;
    }
    output.push_str("];\n\n");
    output.push_str("pub const MIRROR_TYPE_SOURCES: &[(&str, &str)] = &[\n");
    for name in &mirror_schema.selected {
        let definition = mirror_schema
            .definitions
            .get(name)
            .expect("selected mirror types have declarations");
        writeln!(output, "    ({name:?}, {:?}),", definition.source)?;
    }
    output.push_str("];\n\n");
    output.push_str("pub const ACTION_VARIANTS: &[(&str, &[&str])] = &[\n");
    for variant in variants {
        write!(output, "    ({:?}, &[", variant.name)?;
        for field in &variant.fields {
            write!(output, "{field:?}, ")?;
        }
        output.push_str("]),\n");
    }
    output.push_str("];\n\n");
    output.push_str("pub const ACTION_CONVERTERS: &[(&str, &str)] = &[\n");
    for ty in direct_types {
        let class = converter_policy
            .get(ty)
            .expect("converter policy was exact-validated")
            .as_str();
        writeln!(output, "    ({ty:?}, {class:?}),")?;
    }
    output.push_str("];\n\n");
    output.push_str("pub const ACTION_CONVERTER_HOLES: &[&str] = &[\n");
    for (ty, class) in converter_policy {
        if *class == ConverterClass::Unsupported {
            writeln!(output, "    {ty:?},")?;
        }
    }
    output.push_str("];\n\n");
    output.push_str("pub const PUBLIC_PLUGIN_FUNCTIONS: &[(&str, &str)] = &[\n");
    for function in public_functions {
        let class = function_policy
            .get(function)
            .expect("function policy was exact-validated")
            .as_str();
        writeln!(output, "    ({function:?}, {class:?}),")?;
    }
    output.push_str("];\n\n");
    output.push_str("pub const FUNCTION_POLICY_REASONS: &[(&str, &str)] = &[\n");
    for (function, class) in function_policy {
        writeln!(output, "    ({function:?}, {:?}),", function_policy_reason(*class))?;
    }
    output.push_str("];\n\n");
    output.push_str("pub const UNSUPPORTED_PUBLIC_PLUGIN_FUNCTIONS: &[&str] = &[\n");
    for (function, class) in function_policy {
        if *class == FunctionClass::Unsupported {
            writeln!(output, "    {function:?},")?;
        }
    }
    output.push_str("];\n\n");
    output.push_str("pub const VALIDATION_RULES: &[(&str, &str)] = &[\n");
    output.push_str("    (\"PercentOrFixed::Percent\", \"must not exceed 100, matching the pinned source parser\"),\n");
    output.push_str("    (\"PluginUserConfiguration\", \"must not contain keys the pinned source constructor would silently discard\"),\n");
    output.push_str("];\n\n");
    render_native_command_metadata(shim_functions, function_policy, mirror_schema, &mut output)?;
    render_model_module("raw", "Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize", ModelFlavor::Raw, mirror_schema, &mut output)?;
    render_model_module("validated", "Clone, Debug, PartialEq, Eq, serde::Serialize", ModelFlavor::Validated, mirror_schema, &mut output)?;
    output.push_str("pub use validated::Action as ValidatedAction;\n\n");
    render_validation_error(&mut output)?;
    render_native_command_models(shim_functions, function_policy, mirror_schema, &mut output)?;
    render_raw_to_validated_converters(mirror_schema, &mut output)?;
    render_upstream_converters(mirror_schema, &mut output)?;
    render_native_command_return_converters(shim_functions, function_policy, mirror_schema, &mut output)?;
    Ok(output)
}

fn function_policy_reason(class: FunctionClass) -> &'static str {
    match class {
        FunctionClass::Exposed => "v1 native Zellij user command; state-changing and not a query, bridge primitive, or background integration",
        FunctionClass::Internal => "bridge lifecycle, client targeting, permission, capture, or subscription primitive; never user-dispatched",
        FunctionClass::Query => "source query or input/output helper; v1 native command surface excludes queries",
        FunctionClass::Unsupported => "background integration or subsystem administration; excluded from the v1 native command surface",
    }
}

fn render_native_command_metadata(
    shim_functions: &BTreeMap<String, ShimFunction>,
    function_policy: &BTreeMap<String, FunctionClass>,
    mirror_schema: &MirrorSchema,
    output: &mut String,
) -> Result<()> {
    output.push_str("pub const NATIVE_ZELLIJ_COMMANDS: &[(&str, &str, &str, &str)] = &[\n");
    for function in shim_functions.values() {
        if function_policy.get(&function.name) != Some(&FunctionClass::Exposed) {
            continue;
        }
        let return_type = function
            .return_type
            .as_ref()
            .map(|ty| command_model_type_text(ty, &function.generic_types, ModelFlavor::Validated, mirror_schema))
            .transpose()?
            .unwrap_or_else(|| "()".to_owned());
        writeln!(
            output,
            "    ({:?}, {:?}, {:?}, \"source-to-validated\"),",
            yaml_kebab(&function.name),
            function.name,
            return_type,
        )?;
    }
    output.push_str("];\n\n");
    output.push_str("pub const NATIVE_ZELLIJ_COMMAND_ARGUMENTS: &[(&str, &str, &str, &str)] = &[\n");
    for function in shim_functions.values() {
        if function_policy.get(&function.name) != Some(&FunctionClass::Exposed) {
            continue;
        }
        for argument in &function.arguments {
            let rust_type = command_model_type_text(
                &argument.ty,
                &function.generic_types,
                ModelFlavor::Raw,
                mirror_schema,
            )?;
            writeln!(
                output,
                "    ({:?}, {:?}, {:?}, \"raw-to-validated\"),",
                yaml_kebab(&function.name),
                yaml_kebab(&argument.name),
                rust_type,
            )?;
        }
    }
    output.push_str("];\n\n");
    output.push_str("pub const NATIVE_ZELLIJ_COMMAND_CONVERTER_HOLES: &[(&str, &str, &str)] = &[];\n\n");
    Ok(())
}

fn yaml_kebab(name: &str) -> String {
    name.replace('_', "-")
}


fn render_native_command_models(
    shim_functions: &BTreeMap<String, ShimFunction>,
    function_policy: &BTreeMap<String, FunctionClass>,
    mirror_schema: &MirrorSchema,
    output: &mut String,
) -> Result<()> {
    output.push_str("#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]\n");
    output.push_str("#[serde(tag = \"command\", content = \"arguments\", rename_all = \"kebab-case\", rename_all_fields = \"kebab-case\")]\n");
    output.push_str("pub enum RawNativeCommand {\n");
    for function in shim_functions.values() {
        if function_policy.get(&function.name) != Some(&FunctionClass::Exposed) {
            continue;
        }
        render_command_variant(function, ModelFlavor::Raw, mirror_schema, output)?;
    }
    output.push_str("}\n\n");

    output.push_str("#[derive(Clone, Debug, PartialEq)]\n");
    output.push_str("pub enum ValidatedNativeCommand {\n");
    for function in shim_functions.values() {
        if function_policy.get(&function.name) != Some(&FunctionClass::Exposed) {
            continue;
        }
        render_command_variant(function, ModelFlavor::Validated, mirror_schema, output)?;
    }
    output.push_str("}\n\n");

    output.push_str("impl TryFrom<RawNativeCommand> for ValidatedNativeCommand {\n");
    output.push_str("    type Error = ValidationError;\n");
    output.push_str("    fn try_from(value: RawNativeCommand) -> Result<Self, Self::Error> {\n");
    output.push_str("        match value {\n");
    for function in shim_functions.values() {
        if function_policy.get(&function.name) != Some(&FunctionClass::Exposed) {
            continue;
        }
        let variant = command_variant_name(&function.name);
        if function.arguments.is_empty() {
            writeln!(output, "            RawNativeCommand::{variant} => Ok(Self::{variant}),")?;
            continue;
        }
        let names = function
            .arguments
            .iter()
            .map(|argument| argument.name.as_str())
            .collect::<Vec<_>>();
        writeln!(output, "            RawNativeCommand::{variant} {{ {} }} => Ok(Self::{variant} {{", names.join(", "))?;
        for argument in &function.arguments {
            writeln!(
                output,
                "                {}: {},",
                argument.name,
                command_raw_to_validated_expression(
                    &argument.ty,
                    &argument.name,
                    &function.generic_types,
                    mirror_schema,
                )?
            )?;
        }
        output.push_str("            }),\n");
    }
    output.push_str("        }\n    }\n}\n\n");

    output.push_str("#[derive(Clone, Debug, PartialEq)]\n");
    output.push_str("pub enum NativeCommandDispatch {\n");
    for function in shim_functions.values() {
        if function_policy.get(&function.name) != Some(&FunctionClass::Exposed) {
            continue;
        }
        render_command_variant(function, ModelFlavor::Validated, mirror_schema, output)?;
    }
    output.push_str("}\n\n");
    output.push_str("impl From<ValidatedNativeCommand> for NativeCommandDispatch {\n");
    output.push_str("    fn from(value: ValidatedNativeCommand) -> Self {\n");
    output.push_str("        match value {\n");
    for function in shim_functions.values() {
        if function_policy.get(&function.name) != Some(&FunctionClass::Exposed) {
            continue;
        }
        let variant = command_variant_name(&function.name);
        if function.arguments.is_empty() {
            writeln!(output, "            ValidatedNativeCommand::{variant} => Self::{variant},")?;
        } else {
            let names = function
                .arguments
                .iter()
                .map(|argument| argument.name.as_str())
                .collect::<Vec<_>>();
            writeln!(output, "            ValidatedNativeCommand::{variant} {{ {} }} => Self::{variant} {{ {} }},", names.join(", "), names.join(", "))?;
        }
    }
    output.push_str("        }\n    }\n}\n\n");
    output.push_str("#[derive(Clone, Debug, PartialEq)]\n");
    output.push_str("pub enum NativeCommandReturn {\n");
    for function in shim_functions.values() {
        if function_policy.get(&function.name) != Some(&FunctionClass::Exposed) {
            continue;
        }
        let variant = command_variant_name(&function.name);
        let return_type = function
            .return_type
            .as_ref()
            .map(|ty| command_model_type_text(ty, &function.generic_types, ModelFlavor::Validated, mirror_schema))
            .transpose()?
            .unwrap_or_else(|| "()".to_owned());
        writeln!(output, "    {variant}({return_type}),")?;
    }
    output.push_str("}\n\n");
    Ok(())
}

fn render_native_command_return_converters(
    shim_functions: &BTreeMap<String, ShimFunction>,
    function_policy: &BTreeMap<String, FunctionClass>,
    mirror_schema: &MirrorSchema,
    output: &mut String,
) -> Result<()> {
    let return_roots = command_return_type_roots(shim_functions, function_policy);
    if return_roots.contains("PaneId") {
        output.push_str("#[allow(dead_code)]\n");
        output.push_str("fn from_zellij_pane_id(value: zellij_utils::data::PaneId) -> validated::PaneId {\n");
        output.push_str("    match value {\n");
        output.push_str("        zellij_utils::data::PaneId::Terminal(id) => validated::PaneId::Terminal(id),\n");
        output.push_str("        zellij_utils::data::PaneId::Plugin(id) => validated::PaneId::Plugin(id),\n");
        output.push_str("    }\n}\n\n");
    }
    for function in shim_functions.values() {
        if function_policy.get(&function.name) != Some(&FunctionClass::Exposed) {
            continue;
        }
        let source_return_type = function
            .return_type
            .as_ref()
            .map(|ty| source_command_type_text(ty, &function.generic_types, mirror_schema))
            .transpose()?
            .unwrap_or_else(|| "()".to_owned());
        let expression = function
            .return_type
            .as_ref()
            .map(|ty| source_return_to_validated_expression(ty, "value", &function.generic_types, mirror_schema))
            .transpose()?
            .unwrap_or_else(|| "value".to_owned());
        let variant = command_variant_name(&function.name);
        writeln!(
            output,
            "#[allow(dead_code)]\npub fn native_command_return_{}(value: {source_return_type}) -> NativeCommandReturn {{\n    NativeCommandReturn::{variant}({expression})\n}}\n",
            function.name,
        )?;
    }
    output.push_str("pub fn dispatch_native_command(value: NativeCommandDispatch) -> NativeCommandReturn {\n");
    output.push_str("    match value {\n");
    for function in shim_functions.values() {
        if function_policy.get(&function.name) != Some(&FunctionClass::Exposed) {
            continue;
        }
        let variant = command_variant_name(&function.name);
        let arguments = function
            .arguments
            .iter()
            .map(|argument| {
                command_validated_to_source_expression(
                    &argument.ty,
                    &argument.name,
                    &function.generic_types,
                    mirror_schema,
                )
            })
            .collect::<Result<Vec<_>>>()?;
        let pattern = if function.arguments.is_empty() {
            String::new()
        } else {
            format!(
                " {{ {} }}",
                function
                    .arguments
                    .iter()
                    .map(|argument| argument.name.as_str())
                    .collect::<Vec<_>>()
                    .join(", ")
            )
        };
        writeln!(
            output,
            "        NativeCommandDispatch::{variant}{pattern} => native_command_return_{}(zellij_tile::shim::{}({})),",
            function.name,
            function.name,
            arguments.join(", "),
        )?;
    }
    output.push_str("    }\n}\n\n");
    Ok(())
}

fn command_validated_to_source_expression(
    ty: &Type,
    value: &str,
    generic_types: &BTreeMap<String, Type>,
    mirror_schema: &MirrorSchema,
) -> Result<String> {
    match ty {
        Type::Reference(reference) if matches!(reference.elem.as_ref(), Type::Slice(_)) => {
            command_validated_to_source_expression(&reference.elem, value, generic_types, mirror_schema)
        }
        Type::Reference(reference) => Ok(format!(
            "&{}",
            command_validated_to_source_expression(&reference.elem, value, generic_types, mirror_schema)?
        )),
        Type::Slice(slice) if !source_conversion_needed(&slice.elem, mirror_schema) => {
            Ok(format!("&{value}"))
        }
        Type::Slice(slice) => {
            let item = command_validated_to_source_expression(&slice.elem, "item", generic_types, mirror_schema)?;
            Ok(format!("&{value}.into_iter().map(|item| {item}).collect::<Vec<_>>()"))
        }
        Type::ImplTrait(impl_trait) => {
            let normalized = normalized_trait_bounds(&impl_trait.bounds)
                .ok_or_else(|| eyre::eyre!("unrecognized impl Trait command parameter {}", type_text(ty)))?;
            command_validated_to_source_expression(&normalized, value, generic_types, mirror_schema)
        }
        Type::Path(path) => {
            let segment = path.path.segments.last().expect("path has a segment");
            let name = segment.ident.to_string();
            if let Some(normalized) = generic_types.get(&name) {
                return command_validated_to_source_expression(normalized, value, generic_types, mirror_schema);
            }
            let arguments = type_arguments(segment)?;
            if name == "Option"
                && arguments.len() == 1
                && matches!(
                    arguments[0],
                    Type::Reference(reference)
                        if matches!(
                            reference.elem.as_ref(),
                            Type::Path(path) if path.path.is_ident("str")
                        )
                )
            {
                return Ok(format!("{value}.as_deref()"));
            }
            into_source_expression(ty, value, mirror_schema)
        }
        Type::Tuple(tuple) => {
            let names = (0..tuple.elems.len())
                .map(|index| format!("tuple_{index}"))
                .collect::<Vec<_>>();
            let values = tuple
                .elems
                .iter()
                .zip(&names)
                .map(|(element, name)| command_validated_to_source_expression(element, name, generic_types, mirror_schema))
                .collect::<Result<Vec<_>>>()?;
            Ok(format!(
                "{{ let ({},) = {value}; ({},) }}",
                names.join(", "),
                values.join(", ")
            ))
        }
        Type::Group(group) => command_validated_to_source_expression(&group.elem, value, generic_types, mirror_schema),
        Type::Paren(paren) => command_validated_to_source_expression(&paren.elem, value, generic_types, mirror_schema),
        _ => into_source_expression(ty, value, mirror_schema),
    }
}


fn command_return_type_roots(
    shim_functions: &BTreeMap<String, ShimFunction>,
    function_policy: &BTreeMap<String, FunctionClass>,
) -> BTreeSet<String> {
    let mut roots = BTreeSet::new();
    for function in shim_functions.values() {
        if function_policy.get(&function.name) != Some(&FunctionClass::Exposed) {
            continue;
        }
        if let Some(return_type) = &function.return_type {
            collect_type_names(return_type, &mut roots);
        }
        for generic in function.generic_types.keys() {
            roots.remove(generic);
        }
    }
    roots.retain(|name| !builtin_type(name));
    roots
}

fn source_command_type_text(
    ty: &Type,
    generic_types: &BTreeMap<String, Type>,
    mirror_schema: &MirrorSchema,
) -> Result<String> {
    match ty {
        Type::Reference(reference) => source_command_type_text(&reference.elem, generic_types, mirror_schema),
        Type::Slice(slice) => Ok(format!(
            "Vec < {} >",
            source_command_type_text(&slice.elem, generic_types, mirror_schema)?
        )),
        Type::ImplTrait(impl_trait) => {
            let normalized = normalized_trait_bounds(&impl_trait.bounds)
                .ok_or_else(|| eyre::eyre!("unrecognized impl Trait command return {}", type_text(ty)))?;
            source_command_type_text(&normalized, generic_types, mirror_schema)
        }
        Type::Path(path) => {
            let segment = path.path.segments.last().expect("path has a segment");
            let name = segment.ident.to_string();
            if let Some(normalized) = generic_types.get(&name) {
                return source_command_type_text(normalized, generic_types, mirror_schema);
            }
            let arguments = type_arguments(segment)?;
            if let Some(definition) = selected_definition(&name, mirror_schema) {
                return source_type_path(definition, &name);
            }
            match name.as_str() {
                "Option" | "Vec" | "Box" | "Result" | "BTreeSet" | "HashSet" | "BTreeMap" | "HashMap" => Ok(format!(
                    "{} < {} >",
                    name,
                    arguments
                        .iter()
                        .map(|argument| source_command_type_text(argument, generic_types, mirror_schema))
                        .collect::<Result<Vec<_>>>()?
                        .join(", ")
                )),
                "Path" | "PathBuf" => Ok("std::path::PathBuf".to_owned()),
                "String" | "bool" | "char" | "usize" | "u8" | "u16" | "u32" | "u64" | "i32" | "i64" | "f64" => Ok(name),
                _ => bail!("no typed source return converter for {}", type_text(ty)),
            }
        }
        Type::Tuple(tuple) => {
            if tuple.elems.is_empty() {
                return Ok("()".to_owned());
            }
            let elements = tuple
                .elems
                .iter()
                .map(|element| source_command_type_text(element, generic_types, mirror_schema))
                .collect::<Result<Vec<_>>>()?
                .join(", ");
            Ok(format!("({elements}{})", if tuple.elems.len() == 1 { "," } else { "" }))
        }
        Type::Group(group) => source_command_type_text(&group.elem, generic_types, mirror_schema),
        Type::Paren(paren) => source_command_type_text(&paren.elem, generic_types, mirror_schema),
        _ => bail!("no typed source return converter for {}", type_text(ty)),
    }
}

fn source_return_to_validated_expression(
    ty: &Type,
    value: &str,
    generic_types: &BTreeMap<String, Type>,
    mirror_schema: &MirrorSchema,
) -> Result<String> {
    if !source_conversion_needed(ty, mirror_schema) {
        return Ok(value.to_owned());
    }
    match ty {
        Type::Reference(reference) => source_return_to_validated_expression(&reference.elem, value, generic_types, mirror_schema),
        Type::Slice(slice) => {
            let item = source_return_to_validated_expression(&slice.elem, "item", generic_types, mirror_schema)?;
            Ok(format!("{value}.into_iter().map(|item| {item}).collect()"))
        }
        Type::ImplTrait(impl_trait) => {
            let normalized = normalized_trait_bounds(&impl_trait.bounds)
                .ok_or_else(|| eyre::eyre!("unrecognized impl Trait command return {}", type_text(ty)))?;
            source_return_to_validated_expression(&normalized, value, generic_types, mirror_schema)
        }
        Type::Path(path) => {
            let segment = path.path.segments.last().expect("path has a segment");
            let name = segment.ident.to_string();
            if let Some(normalized) = generic_types.get(&name) {
                return source_return_to_validated_expression(normalized, value, generic_types, mirror_schema);
            }
            if selected_definition(&name, mirror_schema).is_some() {
                return match name.as_str() {
                    "PaneId" => Ok(format!("from_zellij_pane_id({value})")),
                    _ => bail!("no source-to-validated command return converter for {name}"),
                };
            }
            let arguments = type_arguments(segment)?;
            match name.as_str() {
                "Option" => {
                    let item = source_return_to_validated_expression(arguments[0], "item", generic_types, mirror_schema)?;
                    Ok(format!("{value}.map(|item| {item})"))
                }
                "Vec" | "BTreeSet" | "HashSet" => {
                    let item = source_return_to_validated_expression(arguments[0], "item", generic_types, mirror_schema)?;
                    Ok(format!("{value}.into_iter().map(|item| {item}).collect()"))
                }
                "Result" => {
                    let ok = source_return_to_validated_expression(arguments[0], "item", generic_types, mirror_schema)?;
                    let error = source_return_to_validated_expression(arguments[1], "error", generic_types, mirror_schema)?;
                    Ok(format!("{value}.map(|item| {ok}).map_err(|error| {error})"))
                }
                "BTreeMap" | "HashMap" => {
                    let key = source_return_to_validated_expression(arguments[0], "key", generic_types, mirror_schema)?;
                    let item = source_return_to_validated_expression(arguments[1], "item", generic_types, mirror_schema)?;
                    Ok(format!("{value}.into_iter().map(|(key, item)| ({key}, {item})).collect()"))
                }
                "Box" => {
                    let item = source_return_to_validated_expression(arguments[0], &format!("*{value}"), generic_types, mirror_schema)?;
                    Ok(format!("Box::new({item})"))
                }
                _ => Ok(value.to_owned()),
            }
        }
        Type::Tuple(tuple) => {
            if tuple.elems.is_empty() {
                return Ok(value.to_owned());
            }
            let names = (0..tuple.elems.len())
                .map(|index| format!("tuple_{index}"))
                .collect::<Vec<_>>();
            let values = tuple
                .elems
                .iter()
                .zip(&names)
                .map(|(element, name)| source_return_to_validated_expression(element, name, generic_types, mirror_schema))
                .collect::<Result<Vec<_>>>()?;
            Ok(format!(
                "{{ let ({},) = {value}; ({},) }}",
                names.join(", "),
                values.join(", ")
            ))
        }
        Type::Group(group) => source_return_to_validated_expression(&group.elem, value, generic_types, mirror_schema),
        Type::Paren(paren) => source_return_to_validated_expression(&paren.elem, value, generic_types, mirror_schema),
        _ => Ok(value.to_owned()),
    }
}

fn render_command_variant(
    function: &ShimFunction,
    flavor: ModelFlavor,
    mirror_schema: &MirrorSchema,
    output: &mut String,
) -> Result<()> {
    let variant = command_variant_name(&function.name);
    if function.arguments.is_empty() {
        writeln!(output, "    {variant},")?;
        return Ok(());
    }
    writeln!(output, "    {variant} {{")?;
    for argument in &function.arguments {
        writeln!(
            output,
            "        {}: {},",
            argument.name,
            command_model_type_text(
                &argument.ty,
                &function.generic_types,
                flavor,
                mirror_schema,
            )?
        )?;
    }
    output.push_str("    },\n");
    Ok(())
}

fn command_model_type_text(
    ty: &Type,
    generic_types: &BTreeMap<String, Type>,
    flavor: ModelFlavor,
    mirror_schema: &MirrorSchema,
) -> Result<String> {
    match ty {
        Type::Reference(reference) => command_model_type_text(&reference.elem, generic_types, flavor, mirror_schema),
        Type::Slice(slice) => Ok(format!(
            "Vec < {} >",
            command_model_type_text(&slice.elem, generic_types, flavor, mirror_schema)?
        )),
        Type::ImplTrait(impl_trait) => {
            let normalized = normalized_trait_bounds(&impl_trait.bounds)
                .ok_or_else(|| eyre::eyre!("unrecognized impl Trait command parameter {}", type_text(ty)))?;
            command_model_type_text(&normalized, generic_types, flavor, mirror_schema)
        }
        Type::Path(path) => {
            let segment = path.path.segments.last().expect("path has a segment");
            let name = segment.ident.to_string();
            if let Some(normalized) = generic_types.get(&name) {
                return command_model_type_text(normalized, generic_types, flavor, mirror_schema);
            }
            let arguments = type_arguments(segment)?;
            if name == "str" {
                return Ok("String".to_owned());
            }
            if selected_definition(&name, mirror_schema).is_some() {
                return Ok(format!(
                    "{}::{}",
                    match flavor {
                        ModelFlavor::Raw => "raw",
                        ModelFlavor::Validated => "validated",
                    },
                    name
                ));
            }
            match name.as_str() {
                "Option" | "Vec" | "Box" | "Result" => Ok(format!(
                    "{} < {} >",
                    name,
                    arguments
                        .iter()
                        .map(|argument| command_model_type_text(argument, generic_types, flavor, mirror_schema))
                        .collect::<Result<Vec<_>>>()?
                        .join(", ")
                )),
                "BTreeSet" | "HashSet" => Ok(format!(
                    "std::collections::{} < {} >",
                    name,
                    arguments
                        .iter()
                        .map(|argument| command_model_type_text(argument, generic_types, flavor, mirror_schema))
                        .collect::<Result<Vec<_>>>()?
                        .join(", ")
                )),
                "BTreeMap" | "HashMap" if matches!(flavor, ModelFlavor::Raw) => {
                    if arguments.len() != 2 {
                        bail!("{name} command parameter needs key and value types");
                    }
                    Ok(format!(
                        "Vec < raw::MapEntry < {}, {} > >",
                        command_model_type_text(arguments[0], generic_types, flavor, mirror_schema)?,
                        command_model_type_text(arguments[1], generic_types, flavor, mirror_schema)?,
                    ))
                }
                "BTreeMap" | "HashMap" => Ok(format!(
                    "std::collections::{} < {} >",
                    name,
                    arguments
                        .iter()
                        .map(|argument| command_model_type_text(argument, generic_types, flavor, mirror_schema))
                        .collect::<Result<Vec<_>>>()?
                        .join(", ")
                )),
                "Path" | "PathBuf" => Ok("std::path::PathBuf".to_owned()),
                "String" | "bool" | "usize" | "u8" | "u16" | "u32" | "u64" | "i32" | "i64" | "f64" => Ok(name),
                _ => bail!("no typed command converter for {}", type_text(ty)),
            }
        }
        Type::Tuple(tuple) => {
            if tuple.elems.is_empty() {
                return Ok("()".to_owned());
            }
            let elements = tuple
                .elems
                .iter()
                .map(|element| command_model_type_text(element, generic_types, flavor, mirror_schema))
                .collect::<Result<Vec<_>>>()?
                .join(", ");
            Ok(format!("({elements}{})", if tuple.elems.len() == 1 { "," } else { "" }))
        }
        Type::Group(group) => command_model_type_text(&group.elem, generic_types, flavor, mirror_schema),
        Type::Paren(paren) => command_model_type_text(&paren.elem, generic_types, flavor, mirror_schema),
        _ => bail!("no typed command converter for {}", type_text(ty)),
    }
}

fn command_raw_to_validated_expression(
    ty: &Type,
    value: &str,
    generic_types: &BTreeMap<String, Type>,
    mirror_schema: &MirrorSchema,
) -> Result<String> {
    match ty {
        Type::Reference(reference) => command_raw_to_validated_expression(&reference.elem, value, generic_types, mirror_schema),
        Type::Slice(slice) => {
            let item = command_raw_to_validated_expression(&slice.elem, "item", generic_types, mirror_schema)?;
            Ok(format!("{value}.into_iter().map(|item| -> Result<_, ValidationError> {{ Ok({item}) }}).collect::<Result<_, ValidationError>>()?"))
        }
        Type::ImplTrait(impl_trait) => {
            let normalized = normalized_trait_bounds(&impl_trait.bounds)
                .ok_or_else(|| eyre::eyre!("unrecognized impl Trait command parameter {}", type_text(ty)))?;
            command_raw_to_validated_expression(&normalized, value, generic_types, mirror_schema)
        }
        Type::Path(path) => {
            let name = path.path.segments.last().expect("path has a segment").ident.to_string();
            if let Some(normalized) = generic_types.get(&name) {
                return command_raw_to_validated_expression(normalized, value, generic_types, mirror_schema);
            }
            raw_to_validated_expression(ty, value, mirror_schema)
        }
        Type::Tuple(tuple) => {
            let names = (0..tuple.elems.len())
                .map(|index| format!("tuple_{index}"))
                .collect::<Vec<_>>();
            let values = tuple
                .elems
                .iter()
                .zip(&names)
                .map(|(element, name)| command_raw_to_validated_expression(element, name, generic_types, mirror_schema))
                .collect::<Result<Vec<_>>>()?;
            Ok(format!(
                "{{ let ({},) = {value}; ({},) }}",
                names.join(", "),
                values.join(", ")
            ))
        }
        Type::Group(group) => command_raw_to_validated_expression(&group.elem, value, generic_types, mirror_schema),
        Type::Paren(paren) => command_raw_to_validated_expression(&paren.elem, value, generic_types, mirror_schema),
        _ => raw_to_validated_expression(ty, value, mirror_schema),
    }
}

fn command_variant_name(name: &str) -> String {
    name.split('_')
        .map(|part| {
            let mut characters = part.chars();
            let first = characters.next().expect("function name segments are non-empty");
            format!("{}{}", first.to_ascii_uppercase(), characters.as_str())
        })
        .collect()
}
fn render_model_module(
    name: &str,
    base_derives: &str,
    flavor: ModelFlavor,
    mirror_schema: &MirrorSchema,
    output: &mut String,
) -> Result<()> {
    writeln!(output, "pub mod {name} {{")?;
    output.push_str(match flavor {
        ModelFlavor::Raw => "    use std::{collections::BTreeSet, path::PathBuf};\n\n",
        ModelFlavor::Validated => "    use std::{collections::{BTreeMap, BTreeSet}, path::PathBuf};\n\n",
    });
    if matches!(flavor, ModelFlavor::Raw) {
        output.push_str("    #[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]\n");
        output.push_str("    pub struct MapEntry<K, V> { pub key: K, pub value: V }\n\n");
    }
    for type_name in &mirror_schema.selected {
        let definition = mirror_schema
            .definitions
            .get(type_name)
            .expect("selected mirror types have declarations");
        render_model_type(definition, base_derives, flavor, output)?;
        output.push('\n');
    }
    output.push_str("}\n\n");
    Ok(())
}

fn render_model_type(
    definition: &TypeDefinition,
    base_derives: &str,
    flavor: ModelFlavor,
    output: &mut String,
) -> Result<()> {
    match &definition.item {
        ParsedType::Struct(item) => {
            writeln!(output, "    #[derive({})]", model_derives(base_derives, &item.attrs))?;
            render_serde_attributes(&item.attrs, "    ", output)?;
            write!(output, "    pub struct {}", item.ident)?;
            render_struct_fields(&item.fields, flavor, output)?;
        }
        ParsedType::Enum(item) => {
            writeln!(output, "    #[derive({})]", model_derives(base_derives, &item.attrs))?;
            render_serde_attributes(&item.attrs, "    ", output)?;
            writeln!(output, "    pub enum {} {{", item.ident)?;
            for variant in &item.variants {
                render_enum_variant(variant, flavor, output)?;
            }
            output.push_str("    }\n");
        }
        ParsedType::Alias(item) => {
            writeln!(output, "    pub type {} = {};", item.ident, model_type_text(&item.ty, flavor)?)?;
        }
    }
    Ok(())
}

fn model_derives(base: &str, attributes: &[Attribute]) -> String {
    if has_derive(attributes, "Ord") {
        format!("{base}, PartialOrd, Ord")
    } else {
        base.to_owned()
    }
}

fn has_derive(attributes: &[Attribute], expected: &str) -> bool {
    attributes.iter().any(|attribute| {
        attribute.path().is_ident("derive")
            && attribute
                .meta
                .require_list()
                .map(|list| list.tokens.to_string().split(',').any(|item| item.trim() == expected))
                .unwrap_or(false)
    })
}

fn render_struct_fields(fields: &Fields, flavor: ModelFlavor, output: &mut String) -> Result<()> {
    match fields {
        Fields::Unit => output.push_str(";\n"),
        Fields::Named(fields) => {
            output.push_str(" {\n");
            for field in &fields.named {
                render_serde_attributes(&field.attrs, "        ", output)?;
                let name = field.ident.as_ref().expect("named syn field has ident");
                writeln!(output, "        pub {name}: {},", model_type_text(&field.ty, flavor)?)?;
            }
            output.push_str("    }\n");
        }
        Fields::Unnamed(fields) => {
            output.push('(');
            for field in &fields.unnamed {
                write!(output, "pub {}, ", model_type_text(&field.ty, flavor)?)?;
            }
            output.push_str(");\n");
        }
    }
    Ok(())
}

fn render_enum_variant(variant: &Variant, flavor: ModelFlavor, output: &mut String) -> Result<()> {
    render_serde_attributes(&variant.attrs, "        ", output)?;
    match &variant.fields {
        Fields::Unit => writeln!(output, "        {},", variant.ident)?,
        Fields::Named(fields) => {
            writeln!(output, "        {} {{", variant.ident)?;
            for field in &fields.named {
                render_serde_attributes(&field.attrs, "            ", output)?;
                let name = field.ident.as_ref().expect("named syn field has ident");
                writeln!(output, "            {name}: {},", model_type_text(&field.ty, flavor)?)?;
            }
            output.push_str("        },\n");
        }
        Fields::Unnamed(fields) => {
            write!(output, "        {}(", variant.ident)?;
            for field in &fields.unnamed {
                write!(output, "{}, ", model_type_text(&field.ty, flavor)?)?;
            }
            output.push_str("),\n");
        }
    }
    Ok(())
}

fn model_type_text(ty: &Type, flavor: ModelFlavor) -> Result<String> {
    match ty {
        Type::Path(path) => {
            let segment = path.path.segments.last().expect("path has a segment");
            let name = segment.ident.to_string();
            let arguments = type_arguments(segment)?;
            if matches!(flavor, ModelFlavor::Raw) && matches!(name.as_str(), "BTreeMap" | "HashMap") {
                if arguments.len() != 2 {
                    bail!("{name} must have key and value types");
                }
                return Ok(format!(
                    "Vec < MapEntry < {}, {} > >",
                    model_type_text(arguments[0], flavor)?,
                    model_type_text(arguments[1], flavor)?
                ));
            }
            if arguments.is_empty() {
                return Ok(type_text(ty));
            }
            Ok(format!(
                "{} < {} >",
                name,
                arguments
                    .iter()
                    .map(|argument| model_type_text(argument, flavor))
                    .collect::<Result<Vec<_>>>()?
                    .join(", ")
            ))
        }
        Type::Tuple(tuple) => {
            if tuple.elems.is_empty() {
                return Ok("()".to_owned());
            }
            let elements = tuple
                .elems
                .iter()
                .map(|element| model_type_text(element, flavor))
                .collect::<Result<Vec<_>>>()?
                .join(", ");
            Ok(format!("({elements}{})", if tuple.elems.len() == 1 { "," } else { "" }))
        }
        Type::Group(group) => model_type_text(&group.elem, flavor),
        Type::Paren(paren) => model_type_text(&paren.elem, flavor),
        _ => Ok(type_text(ty)),
    }
}

fn render_serde_attributes(attributes: &[Attribute], indent: &str, output: &mut String) -> Result<()> {
    for attribute in attributes {
        if attribute.path().is_ident("serde") {
            writeln!(output, "{indent}{}", attribute.to_token_stream())?;
        }
    }
    Ok(())
}

fn render_validation_error(output: &mut String) -> Result<()> {
    output.push_str("#[derive(Clone, Debug, Eq, PartialEq)]\n");
    output.push_str("pub struct ValidationError { pub type_name: &'static str, pub field: &'static str, pub message: String }\n");
    output.push_str("impl ValidationError { fn new(type_name: &'static str, field: &'static str, message: impl Into<String>) -> Self { Self { type_name, field, message: message.into() } } }\n");
    output.push_str("impl std::fmt::Display for ValidationError { fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result { write!(f, \"{}::{}: {}\", self.type_name, self.field, self.message) } }\n");
    output.push_str("impl std::error::Error for ValidationError {}\n\n");
    Ok(())
}

fn render_raw_to_validated_converters(mirror_schema: &MirrorSchema, output: &mut String) -> Result<()> {
    for type_name in &mirror_schema.selected {
        let definition = mirror_schema
            .definitions
            .get(type_name)
            .expect("selected mirror types have declarations");
        match &definition.item {
            ParsedType::Alias(_) => {}
            ParsedType::Struct(_) | ParsedType::Enum(_) if type_name == "PluginUserConfiguration" => {
                render_plugin_user_configuration_validation(output)?;
            }
            ParsedType::Struct(_) | ParsedType::Enum(_) if type_name == "PercentOrFixed" => {
                render_percent_or_fixed_validation(output)?;
            }
            ParsedType::Struct(item) => render_struct_validation(type_name, item, mirror_schema, output)?,
            ParsedType::Enum(item) => render_enum_validation(type_name, item, mirror_schema, output)?,
        }
        if !matches!(definition.item, ParsedType::Alias(_)) {
            output.push('\n');
        }
    }
    Ok(())
}

fn render_plugin_user_configuration_validation(output: &mut String) -> Result<()> {
    output.push_str("impl TryFrom<raw::PluginUserConfiguration> for validated::PluginUserConfiguration {\n");
    output.push_str("    type Error = ValidationError;\n");
    output.push_str("    fn try_from(raw::PluginUserConfiguration(configuration): raw::PluginUserConfiguration) -> Result<Self, Self::Error> {\n");
    output.push_str("        const RESERVED: &[&str] = &[\"hold_on_close\", \"hold_on_start\", \"cwd\", \"name\", \"direction\", \"floating\", \"move_to_focused_tab\", \"launch_new\", \"payload\", \"skip_cache\", \"title\", \"in_place\", \"skip_plugin_cache\"];\n");
    output.push_str("        let mut converted = std::collections::BTreeMap::new();\n");
    output.push_str("        for raw::MapEntry { key, value } in configuration {\n");
    output.push_str("            if RESERVED.contains(&key.as_str()) {\n");
    output.push_str("                return Err(ValidationError::new(\"PluginUserConfiguration\", \"configuration\", format!(\"reserved key {key:?} would be discarded by the pinned Zellij constructor\")));\n");
    output.push_str("            }\n");
    output.push_str("            if converted.insert(key, value).is_some() {\n");
    output.push_str("                return Err(ValidationError::new(\"PluginUserConfiguration\", \"configuration\", \"duplicate semantic key\"));\n");
    output.push_str("            }\n");
    output.push_str("        }\n");
    output.push_str("        Ok(Self(converted))\n");
    output.push_str("    }\n");
    output.push_str("}\n");
    Ok(())
}

fn render_percent_or_fixed_validation(output: &mut String) -> Result<()> {
    output.push_str("impl TryFrom<raw::PercentOrFixed> for validated::PercentOrFixed {\n");
    output.push_str("    type Error = ValidationError;\n");
    output.push_str("    fn try_from(value: raw::PercentOrFixed) -> Result<Self, Self::Error> {\n");
    output.push_str("        match value {\n");
    output.push_str("            raw::PercentOrFixed::Percent(percent) if percent <= 100 => Ok(Self::Percent(percent)),\n");
    output.push_str("            raw::PercentOrFixed::Percent(percent) => Err(ValidationError::new(\"PercentOrFixed\", \"percent\", format!(\"{percent} exceeds 100\"))),\n");
    output.push_str("            raw::PercentOrFixed::Fixed(fixed) => Ok(Self::Fixed(fixed)),\n");
    output.push_str("        }\n");
    output.push_str("    }\n");
    output.push_str("}\n");
    Ok(())
}

fn render_struct_validation(
    type_name: &str,
    item: &ItemStruct,
    mirror_schema: &MirrorSchema,
    output: &mut String,
) -> Result<()> {
    writeln!(output, "impl TryFrom<raw::{type_name}> for validated::{type_name} {{")?;
    output.push_str("    type Error = ValidationError;\n");
    writeln!(output, "    fn try_from(value: raw::{type_name}) -> Result<Self, Self::Error> {{")?;
    match &item.fields {
        Fields::Unit => output.push_str("        let _ = value;\n        Ok(Self)\n"),
        Fields::Named(fields) => {
            let names = fields
                .named
                .iter()
                .map(|field| field.ident.as_ref().expect("named field").to_string())
                .collect::<Vec<_>>();
            writeln!(output, "        let raw::{type_name} {{ {} }} = value;", names.join(", "))?;
            output.push_str("        Ok(Self {\n");
            for field in &fields.named {
                let name = field.ident.as_ref().expect("named field").to_string();
                writeln!(
                    output,
                    "            {name}: {},",
                    raw_to_validated_expression(&field.ty, &name, mirror_schema)?
                )?;
            }
            output.push_str("        })\n");
        }
        Fields::Unnamed(fields) => {
            let names = (0..fields.unnamed.len())
                .map(|index| format!("field_{index}"))
                .collect::<Vec<_>>();
            writeln!(output, "        let raw::{type_name}({}) = value;", names.join(", "))?;
            output.push_str("        Ok(Self(");
            for (field, name) in fields.unnamed.iter().zip(&names) {
                write!(output, "{}, ", raw_to_validated_expression(&field.ty, name, mirror_schema)?)?;
            }
            output.push_str("))\n");
        }
    }
    output.push_str("    }\n}\n");
    Ok(())
}

fn render_enum_validation(
    type_name: &str,
    item: &ItemEnum,
    mirror_schema: &MirrorSchema,
    output: &mut String,
) -> Result<()> {
    writeln!(output, "impl TryFrom<raw::{type_name}> for validated::{type_name} {{")?;
    output.push_str("    type Error = ValidationError;\n");
    writeln!(output, "    fn try_from(value: raw::{type_name}) -> Result<Self, Self::Error> {{")?;
    output.push_str("        match value {\n");
    for variant in &item.variants {
        match &variant.fields {
            Fields::Unit => {
                writeln!(output, "            raw::{type_name}::{} => Ok(Self::{}),", variant.ident, variant.ident)?;
            }
            Fields::Named(fields) => {
                let names = fields
                    .named
                    .iter()
                    .map(|field| field.ident.as_ref().expect("named field").to_string())
                    .collect::<Vec<_>>();
                writeln!(
                    output,
                    "            raw::{type_name}::{} {{ {} }} => Ok(Self::{} {{",
                    variant.ident,
                    names.join(", "),
                    variant.ident
                )?;
                for field in &fields.named {
                    let name = field.ident.as_ref().expect("named field").to_string();
                    writeln!(
                        output,
                        "                {name}: {},",
                        raw_to_validated_expression(&field.ty, &name, mirror_schema)?
                    )?;
                }
                output.push_str("            }),\n");
            }
            Fields::Unnamed(fields) => {
                let names = (0..fields.unnamed.len())
                    .map(|index| format!("field_{index}"))
                    .collect::<Vec<_>>();
                writeln!(
                    output,
                    "            raw::{type_name}::{}({}) => Ok(Self::{}(",
                    variant.ident,
                    names.join(", "),
                    variant.ident
                )?;
                for (field, name) in fields.unnamed.iter().zip(&names) {
                    writeln!(
                        output,
                        "                {},",
                        raw_to_validated_expression(&field.ty, name, mirror_schema)?
                    )?;
                }
                output.push_str("            )),\n");
            }
        }
    }
    output.push_str("        }\n    }\n}\n");
    Ok(())
}

fn raw_to_validated_expression(
    ty: &Type,
    value: &str,
    mirror_schema: &MirrorSchema,
) -> Result<String> {
    if !raw_validation_conversion_needed(ty, mirror_schema) {
        return Ok(value.to_owned());
    }
    match ty {
        Type::Path(path) => {
            let segment = path.path.segments.last().expect("path has a segment");
            let name = segment.ident.to_string();
            if let Some(definition) = selected_definition(&name, mirror_schema) {
                if let ParsedType::Alias(alias) = &definition.item {
                    return raw_to_validated_expression(&alias.ty, value, mirror_schema);
                }
                return Ok(format!("{value}.try_into()?"));
            }
            let arguments = type_arguments(segment)?;
            match name.as_str() {
                "Option" => {
                    let item = raw_to_validated_expression(arguments[0], "item", mirror_schema)?;
                    Ok(format!("{value}.map(|item| -> Result<_, ValidationError> {{ Ok({item}) }}).transpose()?"))
                }
                "Vec" | "BTreeSet" | "HashSet" => {
                    let item = raw_to_validated_expression(arguments[0], "item", mirror_schema)?;
                    Ok(format!("{value}.into_iter().map(|item| -> Result<_, ValidationError> {{ Ok({item}) }}).collect::<Result<_, ValidationError>>()?"))
                }
                "BTreeMap" => {
                    let key = raw_to_validated_expression(arguments[0], "key", mirror_schema)?;
                    let map_value = raw_to_validated_expression(arguments[1], "map_value", mirror_schema)?;
                    Ok(format!(
                        "{{ let mut converted = std::collections::BTreeMap::new(); for raw::MapEntry {{ key, value: map_value }} in {value} {{ let key = {key}; let map_value = {map_value}; if converted.insert(key, map_value).is_some() {{ return Err(ValidationError::new(\"BTreeMap\", \"key\", \"duplicate semantic key\")); }} }} converted }}"
                    ))
                }
                "HashMap" => {
                    let key = raw_to_validated_expression(arguments[0], "key", mirror_schema)?;
                    let map_value = raw_to_validated_expression(arguments[1], "map_value", mirror_schema)?;
                    Ok(format!(
                        "{{ let mut converted = std::collections::HashMap::new(); for raw::MapEntry {{ key, value: map_value }} in {value} {{ let key = {key}; let map_value = {map_value}; if converted.insert(key, map_value).is_some() {{ return Err(ValidationError::new(\"HashMap\", \"key\", \"duplicate semantic key\")); }} }} converted }}"
                    ))
                }
                "Box" => {
                    let item = raw_to_validated_expression(
                        arguments[0],
                        &format!("(*{value})"),
                        mirror_schema,
                    )?;
                    Ok(format!("Box::new({item})"))
                }
                _ => Ok(value.to_owned()),
            }
        }
        Type::Tuple(tuple) => {
            let names = (0..tuple.elems.len())
                .map(|index| format!("tuple_{index}"))
                .collect::<Vec<_>>();
            let values = tuple
                .elems
                .iter()
                .zip(&names)
                .map(|(item, name)| raw_to_validated_expression(item, name, mirror_schema))
                .collect::<Result<Vec<_>>>()?;
            Ok(format!(
                "{{ let ({},) = {value}; ({},) }}",
                names.join(", "),
                values.join(", ")
            ))
        }
        Type::Group(group) => raw_to_validated_expression(&group.elem, value, mirror_schema),
        Type::Paren(paren) => raw_to_validated_expression(&paren.elem, value, mirror_schema),
        _ => Ok(value.to_owned()),
    }
}

fn raw_validation_conversion_needed(ty: &Type, mirror_schema: &MirrorSchema) -> bool {
    match ty {
        Type::Path(path) => {
            let segment = path.path.segments.last().expect("path has a segment");
            let name = segment.ident.to_string();
            if let Some(definition) = selected_definition(&name, mirror_schema) {
                return match &definition.item {
                    ParsedType::Alias(alias) => raw_validation_conversion_needed(&alias.ty, mirror_schema),
                    ParsedType::Struct(_) | ParsedType::Enum(_) => true,
                };
            }
            if matches!(name.as_str(), "BTreeMap" | "HashMap") {
                return true;
            }
            type_arguments(segment)
                .map(|arguments| arguments.into_iter().any(|argument| raw_validation_conversion_needed(argument, mirror_schema)))
                .unwrap_or(true)
        }
        Type::Array(array) => raw_validation_conversion_needed(&array.elem, mirror_schema),
        Type::Reference(reference) => raw_validation_conversion_needed(&reference.elem, mirror_schema),
        Type::Slice(slice) => raw_validation_conversion_needed(&slice.elem, mirror_schema),
        Type::Tuple(tuple) => tuple.elems.iter().any(|element| raw_validation_conversion_needed(element, mirror_schema)),
        Type::Group(group) => raw_validation_conversion_needed(&group.elem, mirror_schema),
        Type::Paren(paren) => raw_validation_conversion_needed(&paren.elem, mirror_schema),
        _ => false,
    }
}

fn source_conversion_needed(ty: &Type, mirror_schema: &MirrorSchema) -> bool {
    match ty {
        Type::Path(path) => {
            let segment = path.path.segments.last().expect("path has a segment");
            let name = segment.ident.to_string();
            if let Some(definition) = selected_definition(&name, mirror_schema) {
                return match &definition.item {
                    ParsedType::Alias(alias) => source_conversion_needed(&alias.ty, mirror_schema),
                    ParsedType::Struct(_) | ParsedType::Enum(_) => true,
                };
            }
            type_arguments(segment)
                .map(|arguments| arguments.into_iter().any(|argument| source_conversion_needed(argument, mirror_schema)))
                .unwrap_or(true)
        }
        Type::Array(array) => source_conversion_needed(&array.elem, mirror_schema),
        Type::Reference(reference) => source_conversion_needed(&reference.elem, mirror_schema),
        Type::Slice(slice) => source_conversion_needed(&slice.elem, mirror_schema),
        Type::Tuple(tuple) => tuple.elems.iter().any(|element| source_conversion_needed(element, mirror_schema)),
        Type::Group(group) => source_conversion_needed(&group.elem, mirror_schema),
        Type::Paren(paren) => source_conversion_needed(&paren.elem, mirror_schema),
        _ => false,
    }
}
fn render_upstream_converters(mirror_schema: &MirrorSchema, output: &mut String) -> Result<()> {
    for type_name in &mirror_schema.selected {
        let definition = mirror_schema
            .definitions
            .get(type_name)
            .expect("selected mirror types have declarations");
        if matches!(definition.item, ParsedType::Alias(_)) {
            continue;
        }
        match &definition.item {
            ParsedType::Struct(item) => render_upstream_struct_converter(type_name, item, definition, mirror_schema, output)?,
            ParsedType::Enum(item) => render_upstream_enum_converter(type_name, item, definition, mirror_schema, output)?,
            ParsedType::Alias(_) => unreachable!("aliases were skipped"),
        }
        output.push('\n');
    }
    output.push_str("impl From<ValidatedAction> for zellij_utils::input::actions::Action {\n");
    output.push_str("    fn from(value: ValidatedAction) -> Self { into_zellij_action(value) }\n");
    output.push_str("}\n");
    Ok(())
}

fn render_upstream_struct_converter(
    type_name: &str,
    item: &ItemStruct,
    definition: &TypeDefinition,
    mirror_schema: &MirrorSchema,
    output: &mut String,
) -> Result<()> {
    let source_type = source_type_path(definition, type_name)?;
    output.push_str("#[allow(dead_code)]\n");
    writeln!(
        output,
        "fn into_zellij_{}(value: validated::{type_name}) -> {source_type} {{",
        snake_case(type_name)
    )?;
    match type_name {
        "PluginTag" => {
            output.push_str("    zellij_utils::data::PluginTag::new(value.0)\n");
        }
        "PluginUserConfiguration" => {
            output.push_str("    zellij_utils::input::layout::PluginUserConfiguration::new(value.0)\n");
        }
        _ => match &item.fields {
            Fields::Unit => writeln!(output, "    {source_type}")?,
            Fields::Named(fields) => {
                let names = fields
                    .named
                    .iter()
                    .map(|field| field.ident.as_ref().expect("named field").to_string())
                    .collect::<Vec<_>>();
                writeln!(output, "    let validated::{type_name} {{ {} }} = value;", names.join(", "))?;
                writeln!(output, "    {source_type} {{")?;
                for field in &fields.named {
                    let name = field.ident.as_ref().expect("named field").to_string();
                    writeln!(
                        output,
                        "        {name}: {},",
                        into_source_expression(&field.ty, &name, mirror_schema)?
                    )?;
                }
                output.push_str("    }\n");
            }
            Fields::Unnamed(fields) => {
                let names = (0..fields.unnamed.len())
                    .map(|index| format!("field_{index}"))
                    .collect::<Vec<_>>();
                writeln!(output, "    let validated::{type_name}({}) = value;", names.join(", "))?;
                write!(output, "    {source_type}(")?;
                for (field, name) in fields.unnamed.iter().zip(&names) {
                    write!(output, "{}, ", into_source_expression(&field.ty, name, mirror_schema)?)?;
                }
                output.push_str(")\n");
            }
        },
    }
    output.push_str("}\n");
    Ok(())
}

fn render_upstream_enum_converter(
    type_name: &str,
    item: &ItemEnum,
    definition: &TypeDefinition,
    mirror_schema: &MirrorSchema,
    output: &mut String,
) -> Result<()> {
    let source_type = source_type_path(definition, type_name)?;
    output.push_str("#[allow(dead_code)]\n");
    writeln!(
        output,
        "fn into_zellij_{}(value: validated::{type_name}) -> {source_type} {{",
        snake_case(type_name)
    )?;
    output.push_str("    match value {\n");
    for variant in &item.variants {
        match &variant.fields {
            Fields::Unit => {
                writeln!(output, "        validated::{type_name}::{} => {source_type}::{},", variant.ident, variant.ident)?;
            }
            Fields::Named(fields) => {
                let names = fields
                    .named
                    .iter()
                    .map(|field| field.ident.as_ref().expect("named field").to_string())
                    .collect::<Vec<_>>();
                writeln!(
                    output,
                    "        validated::{type_name}::{} {{ {} }} => {source_type}::{} {{",
                    variant.ident,
                    names.join(", "),
                    variant.ident
                )?;
                for field in &fields.named {
                    let name = field.ident.as_ref().expect("named field").to_string();
                    writeln!(
                        output,
                        "            {name}: {},",
                        into_source_expression(&field.ty, &name, mirror_schema)?
                    )?;
                }
                output.push_str("        },\n");
            }
            Fields::Unnamed(fields) => {
                let names = (0..fields.unnamed.len())
                    .map(|index| format!("field_{index}"))
                    .collect::<Vec<_>>();
                writeln!(
                    output,
                    "        validated::{type_name}::{}({}) => {source_type}::{}(",
                    variant.ident,
                    names.join(", "),
                    variant.ident
                )?;
                for (field, name) in fields.unnamed.iter().zip(&names) {
                    writeln!(
                        output,
                        "            {},",
                        into_source_expression(&field.ty, name, mirror_schema)?
                    )?;
                }
                output.push_str("        ),\n");
            }
        }
    }
    output.push_str("    }\n}\n");
    Ok(())
}

fn into_source_expression(ty: &Type, value: &str, mirror_schema: &MirrorSchema) -> Result<String> {
    if !source_conversion_needed(ty, mirror_schema) {
        return Ok(value.to_owned());
    }
    match ty {
        Type::Path(path) => {
            let segment = path.path.segments.last().expect("path has a segment");
            let name = segment.ident.to_string();
            if let Some(definition) = selected_definition(&name, mirror_schema) {
                if let ParsedType::Alias(alias) = &definition.item {
                    return into_source_expression(&alias.ty, value, mirror_schema);
                }
                return Ok(format!("into_zellij_{}({value})", snake_case(&name)));
            }
            let arguments = type_arguments(segment)?;
            match name.as_str() {
                "Option" => {
                    let item = into_source_expression(arguments[0], "item", mirror_schema)?;
                    Ok(format!("{value}.map(|item| {item})"))
                }
                "Vec" | "BTreeSet" | "HashSet" => {
                    let item = into_source_expression(arguments[0], "item", mirror_schema)?;
                    Ok(format!("{value}.into_iter().map(|item| {item}).collect()"))
                }
                "BTreeMap" | "HashMap" => {
                    let key = into_source_expression(arguments[0], "key", mirror_schema)?;
                    let item = into_source_expression(arguments[1], "item", mirror_schema)?;
                    Ok(format!("{value}.into_iter().map(|(key, item)| ({key}, {item})).collect()"))
                }
                "Box" => {
                    let item = into_source_expression(arguments[0], &format!("*{value}"), mirror_schema)?;
                    Ok(format!("Box::new({item})"))
                }
                _ => Ok(value.to_owned()),
            }
        }
        Type::Tuple(tuple) => {
            let names = (0..tuple.elems.len())
                .map(|index| format!("tuple_{index}"))
                .collect::<Vec<_>>();
            let values = tuple
                .elems
                .iter()
                .zip(&names)
                .map(|(item, name)| into_source_expression(item, name, mirror_schema))
                .collect::<Result<Vec<_>>>()?;
            Ok(format!(
                "{{ let ({},) = {value}; ({},) }}",
                names.join(", "),
                values.join(", ")
            ))
        }
        Type::Group(group) => into_source_expression(&group.elem, value, mirror_schema),
        Type::Paren(paren) => into_source_expression(&paren.elem, value, mirror_schema),
        _ => Ok(value.to_owned()),
    }
}

fn selected_definition<'a>(name: &str, mirror_schema: &'a MirrorSchema) -> Option<&'a TypeDefinition> {
    mirror_schema
        .selected
        .contains(name)
        .then(|| mirror_schema.definitions.get(name))
        .flatten()
}

fn type_arguments<'a>(segment: &'a syn::PathSegment) -> Result<Vec<&'a Type>> {
    match &segment.arguments {
        syn::PathArguments::None => Ok(Vec::new()),
        syn::PathArguments::AngleBracketed(arguments) => Ok(arguments
            .args
            .iter()
            .filter_map(|argument| match argument {
                syn::GenericArgument::Type(ty) => Some(ty),
                _ => None,
            })
            .collect()),
        _ => bail!("unsupported path arguments in {}", segment.to_token_stream()),
    }
}

fn source_type_path(definition: &TypeDefinition, type_name: &str) -> Result<String> {
    let module = match definition.source.as_str() {
        "zellij-utils/src/data.rs" => "zellij_utils::data",
        "zellij-utils/src/input/actions.rs" => "zellij_utils::input::actions",
        "zellij-utils/src/input/command.rs" => "zellij_utils::input::command",
        "zellij-utils/src/input/layout.rs" => "zellij_utils::input::layout",
        "zellij-utils/src/input/mouse.rs" => "zellij_utils::input::mouse",
        "zellij-utils/src/input/options.rs" => "zellij_utils::input::options",
        "zellij-utils/src/position.rs" => "zellij_utils::position",
        source => bail!("no Rust module mapping for mirror source {source:?}"),
    };
    Ok(format!("{module}::{type_name}"))
}

fn snake_case(name: &str) -> String {
    let mut result = String::new();
    for (index, character) in name.chars().enumerate() {
        if character.is_ascii_uppercase() {
            if index != 0 {
                result.push('_');
            }
            result.push(character.to_ascii_lowercase());
        } else {
            result.push(character);
        }
    }
    result
}
fn type_text(ty: &Type) -> String {
    ty.to_token_stream().to_string()
}

fn sha256_hex(bytes: &[u8]) -> String {
    let mut digest = Sha256::new();
    digest.update(bytes);
    digest
        .finalize()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

fn read(path: &Path) -> Result<String> {
    fs::read_to_string(path).wrap_err_with(|| format!("could not read {}", path.display()))
}

fn write_file(path: &Path, contents: String) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).wrap_err_with(|| format!("could not create {}", parent.display()))?;
    }
    fs::write(path, contents).wrap_err_with(|| format!("could not write {}", path.display()))
}
