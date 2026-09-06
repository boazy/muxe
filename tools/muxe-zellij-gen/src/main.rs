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
    process::ExitCode,
    str::FromStr,
};

use eyre::{bail, Result, WrapErr};
use quote::ToTokens;
use sha2::{Digest, Sha256};
use syn::{Attribute, Fields, Item, ItemEnum, ItemFn, ItemStruct, ItemType, Type, Variant, Visibility};

use crate::classification::{ConverterClass, FunctionClass};

const DEFAULT_SOURCE_ROOT: &str = ".local/pins/zellij/checkout";
const DEFAULT_PIN: &str = "pins/zellij.toml";
const DEFAULT_FUNCTION_POLICY: &str = "fixtures/zellij/0.46.0/public-functions.policy";
const DEFAULT_CONVERTER_POLICY: &str = "fixtures/zellij/0.46.0/action-converters.policy";
const DEFAULT_OUTPUT: &str = "fixtures/zellij/0.46.0/action-inventory.rs";
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

#[derive(Debug)]
struct Arguments {
    source_root: PathBuf,
    pin: PathBuf,
    function_policy: PathBuf,
    converter_policy: PathBuf,
    output: PathBuf,
    check: bool,
    write_function_template: bool,
    write_converter_template: bool,
}

#[derive(Debug)]
struct Pin {
    revision: String,
    host_version: String,
    source_inputs: Vec<String>,
}

#[derive(Debug)]
struct ActionVariant {
    name: String,
    fields: Vec<String>,
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

    let actions_path = arguments.source_root.join(ACTION_SOURCE);
    let shim_path = arguments.source_root.join(SHIM_SOURCE);
    let actions_source = read(&actions_path)?;
    let shim_source = read(&shim_path)?;
    let action_variants = parse_action_variants(&actions_source)?;
    let direct_types = direct_action_types(&actions_source)?;
    let public_functions = public_functions(&shim_source)?;

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
    let mirror_schema = mirror_schema(&arguments.source_root, &direct_types)?;


    let source_hashes = source_hashes(&arguments.source_root, &pin.source_inputs)?;
    let generated = generate(
        &pin,
        &mirror_schema,
        &source_hashes,
        &action_variants,
        &direct_types,
        &public_functions,
        &function_policy,
        &converter_policy,
    )?;

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

fn parse_arguments(arguments: impl IntoIterator<Item = String>) -> Result<Arguments> {
    let mut result = Arguments {
        source_root: PathBuf::from(DEFAULT_SOURCE_ROOT),
        pin: PathBuf::from(DEFAULT_PIN),
        function_policy: PathBuf::from(DEFAULT_FUNCTION_POLICY),
        converter_policy: PathBuf::from(DEFAULT_CONVERTER_POLICY),
        output: PathBuf::from(DEFAULT_OUTPUT),
        check: false,
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
            "--write-function-template" => result.write_function_template = true,
            "--write-converter-template" => result.write_converter_template = true,
            "--help" | "-h" => {
                println!("Usage: muxe-zellij-gen [--source-root PATH] [--pin PATH] [--function-policy PATH] [--converter-policy PATH] [--output PATH] [--check] [--write-function-template] [--write-converter-template]");
                std::process::exit(0);
            }
            _ => bail!("unknown argument {argument:?}"),
        }
    }
    if result.check && (result.write_function_template || result.write_converter_template) {
        bail!("--check cannot be combined with a template-writing option");
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
    let revision = pin_scalar(&source, "revision")?;
    let host_version = pin_scalar(&source, "host_version")?;
    let source_inputs = pin_array(&source, "source_inputs")?;
    if revision.len() != 40 || !revision.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        bail!("{} has invalid revision {revision:?}", path.display());
    }
    Ok(Pin {
        revision,
        host_version,
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
            | "Arc"
            | "Rc"
            | "HashMap"
            | "BTreeMap"
            | "HashSet"
            | "BTreeSet"
    )
}

fn public_functions(source: &str) -> Result<BTreeSet<String>> {
    let file = syn::parse_file(source).wrap_err("could not parse pinned shim.rs with syn")?;
    let mut functions = BTreeSet::new();
    for item in file.items {
        if let Item::Fn(ItemFn { vis: Visibility::Public(_), sig, .. }) = item {
            functions.insert(sig.ident.to_string());
        }
    }
    if functions.is_empty() {
        bail!("pinned shim.rs has no public functions");
    }
    Ok(functions)
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
    variants: &[ActionVariant],
    direct_types: &BTreeSet<String>,
    public_functions: &BTreeSet<String>,
    function_policy: &BTreeMap<String, FunctionClass>,
    converter_policy: &BTreeMap<String, ConverterClass>,
) -> Result<String> {
    let mut output = String::from("// @generated by tools/muxe-zellij-gen; do not edit by hand.\n");
    output.push_str("// Regenerate with: cargo run --locked -p muxe-zellij-gen --\n");
    output.push_str("// Integration requires serde, serde_json, and the pinned zellij-utils crate.\n\n");
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
    output.push_str("pub const PUBLIC_PLUGIN_FUNCTIONS: &[(&str, &str)] = &[\n");
    for function in public_functions {
        let class = function_policy
            .get(function)
            .expect("function policy was exact-validated")
            .as_str();
        writeln!(output, "    ({function:?}, {class:?}),")?;
    }
    output.push_str("];\n\n");
    output.push_str("pub const UNSUPPORTED_PUBLIC_PLUGIN_FUNCTIONS: &[&str] = &[\n");
    for (function, class) in function_policy {
        if *class == FunctionClass::Unsupported {
            writeln!(output, "    {function:?},")?;
        }
    }
    output.push_str("];\n\n");
    output.push_str("pub mod action_mirror {\n");
    output.push_str("    use serde::{Deserialize, Serialize};\n");
    output.push_str("    use std::{collections::{BTreeMap, BTreeSet, HashMap, HashSet}, path::PathBuf};\n\n");
    for name in &mirror_schema.selected {
        let definition = mirror_schema
            .definitions
            .get(name)
            .expect("selected mirror types have declarations");
        render_mirror_type(definition, &mut output)?;
        output.push('\n');
    }
    output.push_str("}\n\n");
    output.push_str("pub use action_mirror::Action as ZellijActionMirror;\n\n");
    output.push_str("impl TryFrom<&zellij_utils::input::actions::Action> for ZellijActionMirror {\n");
    output.push_str("    type Error = serde_json::Error;\n\n");
    output.push_str("    fn try_from(action: &zellij_utils::input::actions::Action) -> Result<Self, Self::Error> {\n");
    output.push_str("        serde_json::from_value(serde_json::to_value(action)?)\n");
    output.push_str("    }\n");
    output.push_str("}\n\n");
    output.push_str("impl ZellijActionMirror {\n");
    output.push_str("    pub fn try_into_zellij(&self) -> Result<zellij_utils::input::actions::Action, serde_json::Error> {\n");
    output.push_str("        serde_json::from_value(serde_json::to_value(self)?)\n");
    output.push_str("    }\n");
    output.push_str("}\n");
    Ok(output)
}

fn render_mirror_type(definition: &TypeDefinition, output: &mut String) -> Result<()> {
    match &definition.item {
        ParsedType::Struct(item) => {
            render_serde_attributes(&item.attrs, "", output)?;
            writeln!(output, "    #[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]")?;
            write!(output, "    pub struct {}", item.ident)?;
            render_struct_fields(&item.fields, output)?;
        }
        ParsedType::Enum(item) => {
            render_serde_attributes(&item.attrs, "", output)?;
            writeln!(output, "    #[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]")?;
            writeln!(output, "    pub enum {} {{", item.ident)?;
            for variant in &item.variants {
                render_enum_variant(variant, output)?;
            }
            output.push_str("    }\n");
        }
        ParsedType::Alias(item) => {
            render_serde_attributes(&item.attrs, "", output)?;
            writeln!(output, "    pub type {} = {};", item.ident, type_text(&item.ty))?;
        }
    }
    Ok(())
}

fn render_struct_fields(fields: &Fields, output: &mut String) -> Result<()> {
    match fields {
        Fields::Unit => output.push_str(";\n"),
        Fields::Named(fields) => {
            output.push_str(" {\n");
            for field in &fields.named {
                render_serde_attributes(&field.attrs, "        ", output)?;
                let name = field.ident.as_ref().expect("named syn field has ident");
                writeln!(output, "        pub {name}: {},", type_text(&field.ty))?;
            }
            output.push_str("    }\n");
        }
        Fields::Unnamed(fields) => {
            output.push('(');
            for field in &fields.unnamed {
                render_serde_attributes(&field.attrs, "        ", output)?;
                write!(output, "pub {}, ", type_text(&field.ty))?;
            }
            output.push_str(");\n");
        }
    }
    Ok(())
}

fn render_enum_variant(variant: &Variant, output: &mut String) -> Result<()> {
    render_serde_attributes(&variant.attrs, "        ", output)?;
    match &variant.fields {
        Fields::Unit => writeln!(output, "        {},", variant.ident)?,
        Fields::Named(fields) => {
            writeln!(output, "        {} {{", variant.ident)?;
            for field in &fields.named {
                render_serde_attributes(&field.attrs, "            ", output)?;
                let name = field.ident.as_ref().expect("named syn field has ident");
                writeln!(output, "            {name}: {},", type_text(&field.ty))?;
            }
            output.push_str("        },\n");
        }
        Fields::Unnamed(fields) => {
            write!(output, "        {}(", variant.ident)?;
            for field in &fields.unnamed {
                write!(output, "{}, ", type_text(&field.ty))?;
            }
            output.push_str("),\n");
        }
    }
    Ok(())
}

fn render_serde_attributes(attributes: &[Attribute], indent: &str, output: &mut String) -> Result<()> {
    for attribute in attributes {
        if attribute.path().is_ident("serde") {
            writeln!(output, "{indent}{}", attribute.to_token_stream())?;
        }
    }
    Ok(())
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
