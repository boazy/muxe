#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.14"
# dependencies = ["PyYAML==6.0.3"]
# ///
"""Collect palette files from recorded commits and reproduce a compressed token table.

Collection revisions and file-inventory hashes live in assets/catalog.json.
Downloads use one commit archive per collection repository and recorded canonical URLs.
--offline reuses preserved raw inputs without network access. Neither mode runs
Muxe, modifies its assets, or establishes visual palette acceptance.
"""

from __future__ import annotations

import argparse
from collections import Counter
import csv
import gzip
import hashlib
import io
import json
from pathlib import Path, PurePosixPath
import re
import shutil
import tarfile
from urllib.parse import quote
from urllib.request import Request, urlopen

import yaml
from yaml.nodes import MappingNode, Node, ScalarNode, SequenceNode


# The source registry is catalog.json; these are only format adapters.
FORMATS = {
    "gogh": ("Gogh", "gogh-yaml", "themes"),
    "tinted-base24-spec-0.11": ("Tinted Theming Base24", "base24-yaml", "base24"),
    "iterm2-kitty": ("iTerm2-Color-Schemes Kitty", "kitty-conf", "kitty"),
    "iterm2-ghostty": ("iTerm2-Color-Schemes Ghostty", "ghostty-conf", "ghostty"),
}
FREE_SPACE_FLOOR = 25 * 1024**3
TOKEN_COLUMNS = [
    "source_id", "source_family", "repo", "requested_ref", "revision",
    "source_path", "source_url", "source_blob_sha", "source_sha256", "source_format",
    "source_name", "normalized_identity", "candidate_identity", "variant",
    "variant_identity", "variant_axis", "name_variant_identity", "name_variant_axis",
    "variant_relation", "identity_key", "author", "token_name", "token_path",
    "token_kind", "canonical_role", "role_mapping_basis", "state", "original_token",
    "normalized_hex", "hex_components", "raw_line", "line_number",
]
HEX3 = re.compile(r"^#([0-9a-fA-F]{3})$")
HEX4 = re.compile(r"^#([0-9a-fA-F]{4})$")
HEX6 = re.compile(r"^#([0-9a-fA-F]{6})$")
HEX8 = re.compile(r"^#([0-9a-fA-F]{8})$")
VARIANT_WORDS = (
    "dark", "light", "dawn", "day", "night", "moon", "storm", "wave", "dragon",
    "lotus", "latte", "frappe", "macchiato", "mocha", "hard", "medium", "soft",
    "high-contrast", "patched",
)


def yaml_pairs(node: Node) -> list[tuple[str, Node]]:
    if not isinstance(node, MappingNode):
        return []
    pairs: list[tuple[str, Node]] = []
    for key_node, value_node in node.value:
        key = key_node.value if isinstance(key_node, ScalarNode) else str(key_node)
        pairs.append((str(key), value_node))
    return pairs


def yaml_get(node: Node, name: str) -> Node | None:
    for key, value in yaml_pairs(node):
        if key == name:
            return value
    return None


def yaml_scalar(node: Node | None) -> str:
    if isinstance(node, ScalarNode):
        return node.value
    return ""


def yaml_line(text: str, node: Node) -> str:
    lines = text.splitlines()
    if not lines:
        return ""
    start = node.start_mark.line
    end = node.end_mark.line
    return "\n".join(lines[start:min(end + 1, len(lines))])


def yaml_leaves(node: Node, path: str = ""):
    if isinstance(node, MappingNode):
        for key, value in yaml_pairs(node):
            child = f"{path}.{key}" if path else key
            yield from yaml_leaves(value, child)
    elif isinstance(node, SequenceNode):
        for idx, value in enumerate(node.value):
            child = f"{path}[{idx}]"
            yield from yaml_leaves(value, child)
    else:
        yield path, node

def normalize_identity(name: str) -> str:
    cleaned = re.sub(r"\.(?:conf|yaml|yml)$", "", name.strip(), flags=re.IGNORECASE)
    return re.sub(r"[^a-z0-9]+", "-", cleaned.casefold()).strip("-")


def candidate_identity(name: str) -> str:
    """Return a permissive comparison key; provenance names remain untouched."""
    cleaned = name.strip()
    for prefix in ("Builtin ", "iTerm2 "):
        if cleaned.casefold().startswith(prefix.casefold()):
            cleaned = cleaned[len(prefix):].strip()
    return normalize_identity(cleaned)


def infer_variant(name: str, explicit: str = "") -> str:
    if explicit.strip():
        return re.sub(r"[^a-z0-9]+", "-", explicit.casefold()).strip("-")
    identity = normalize_identity(name)
    words = identity.split("-")
    for word in reversed(words):
        if word in VARIANT_WORDS:
            return word
    if "higher-contrast" in identity:
        return "higher-contrast"
    return "unspecified"


def variant_axis(label: str) -> str:
    normalized = label.casefold().replace("_", "-")
    if normalized in {"dark", "light", "night", "day"}:
        return "polarity"
    if normalized in {"latte", "frappe", "macchiato", "mocha"}:
        return "flavor"
    if normalized in {"hard", "medium", "soft", "high-contrast"}:
        return "contrast"
    return "named-variant-or-unclassified"


def variant_relation(left: str, right: str) -> str:
    if left == right:
        return "same-variant-label"
    left_axis = variant_axis(left)
    right_axis = variant_axis(right)
    if left_axis == right_axis:
        return f"different-labels-same-{left_axis}-axis"
    return f"different-labels-different-axes:{left_axis}/{right_axis}"


def identity_key(identity: str, variant: str, source_path: str) -> str:
    source_path_key = quote(source_path, safe="")
    return f"{identity}::{variant or 'unspecified'}::{source_path_key}"


def normalized_hex(value: str) -> str:
    token = value.strip().strip("\"'")
    m = HEX3.fullmatch(token)
    if m:
        return "#" + "".join(c * 2 for c in m.group(1).lower())
    m = HEX4.fullmatch(token)
    if m:
        return "#" + "".join(c * 2 for c in m.group(1).lower())
    if HEX6.fullmatch(token) or HEX8.fullmatch(token):
        return token.lower()
    m = re.fullmatch(r"0x([0-9a-fA-F]{6})", token)
    if m:
        return "#" + m.group(1).lower()
    return ""


def role_for(source_id: str, token_name: str, source_format: str) -> tuple[str, str, str]:
    name = token_name.strip()
    low = name.casefold().replace("-", "_")
    if source_id == "tinted-base24-spec-0.11" and re.fullmatch(r"base[0-1][0-9a-f]", low):
        role = "base.background" if low == "base00" else "base.text" if low == "base05" else f"base24.{low}"
        basis = "Tinted Base24 spec-0.11 slot (base00/background, base05/foreground); remaining slots stay format-specific"
        return "palette", role, basis
    if source_id == "gogh" and re.fullmatch(r"color_\d{2}", low):
        idx = int(low[-2:]) - 1
        return "ansi", f"ansi.color{idx}", "Gogh color_01..color_16 indexed ANSI palette"
    if source_id in {"iterm2-kitty", "iterm2-ghostty"}:
        if re.fullmatch(r"color\d+", low):
            idx = int(low[5:])
            return "ansi", f"ansi.color{idx}", "iTerm2 terminal color index"
        if re.fullmatch(r"palette\[\d+\]", low):
            idx = int(re.search(r"\d+", low).group(0))
            return "ansi", f"ansi.color{idx}", "Ghostty palette index"
    if source_id == "solarized-official" and re.fullmatch(r"color\d+", low):
        return "ansi", f"ansi.color{int(low[5:])}", "Solarized Xresources ANSI color index"
    if low in {"background", "background_color", "backgroundcolor"}:
        return "appearance", "base.background", "explicit source background token"
    if low in {"foreground", "foreground_color", "foregroundcolor"}:
        return "appearance", "base.text", "explicit source foreground token"
    if low in {"cursor", "cursor_color", "cursorcolor"}:
        return "appearance", "terminal.cursor", "explicit source cursor token"
    if low in {"cursor_text_color", "cursor_text", "cursor-text"}:
        return "appearance", "terminal.cursor_text", "explicit source cursor-text token"
    if low in {"selection_background", "selection-background"}:
        return "appearance", "terminal.selection_background", "explicit source selection-background token"
    if low in {"selection_foreground", "selection-foreground"}:
        return "appearance", "terminal.selection_foreground", "explicit source selection-foreground token"
    if low.startswith("s_"):
        return "named-color", f"solarized.named.{name}", "explicit Solarized named color macro"
    if low in {"include", "unset"}:
        return "directive", f"directive.{low}", "explicit source configuration directive"
    return "palette", f"palette.{name}", "source token name retained; no cross-format semantic inference"


def source_metadata(spec: dict, source_path: str, source_name: str, author: str, variant: str) -> dict:
    ident = normalize_identity(source_name)
    candidate = candidate_identity(source_name)
    v = infer_variant(source_name, variant)
    return {
        "source_id": spec["source_id"], "source_family": spec["source_family"],
        "repo": spec["repo"], "requested_ref": spec["requested_ref"],
        "revision": spec["revision"], "source_path": source_path,
        "source_url": f"https://raw.githubusercontent.com/{spec['repo']}/{spec['revision']}/{quote(source_path, safe='/')}",
        "source_blob_sha": "", "source_sha256": "", "source_format": spec["format"],
        "source_name": source_name, "normalized_identity": ident,
        "candidate_identity": candidate,
        "variant": variant or v, "variant_identity": v,
        "variant_axis": variant_axis(v),
        "name_variant_identity": infer_variant(source_name),
        "name_variant_axis": variant_axis(infer_variant(source_name)),
        "variant_relation": variant_relation(v, infer_variant(source_name)),
        "identity_key": identity_key(ident, v, source_path), "author": author,
    }


def add_token(rows: list[dict], metadata: dict, token_name: str, token_value: str, *, token_path: str = "", token_kind: str | None = None, state: str = "set", raw_line: str = "", line_number: int | str = "", mapping_basis: str | None = None, hex_value: str | None = None) -> None:
    kind, role, basis = role_for(metadata["source_id"], token_name, metadata["source_format"])
    original = token_value.strip()
    color_hex = hex_value if hex_value is not None else normalized_hex(original)
    components = "RGBA" if len(color_hex) == 9 else "RGB" if len(color_hex) == 7 else ""
    rows.append({
        **metadata,
        "token_name": token_name, "token_path": token_path or token_name,
        "token_kind": token_kind or kind, "canonical_role": role,
        "role_mapping_basis": mapping_basis or basis, "state": state,
        "original_token": original, "normalized_hex": color_hex,
        "hex_components": components, "raw_line": raw_line, "line_number": line_number,
    })


def parse_yaml_file(spec: dict, entry: dict, out: Path, downloaded: dict) -> tuple[list[dict], list[dict]]:
    path = entry["path"]
    raw_path = out / downloaded["local_path"]
    text = raw_path.read_text(encoding="utf-8")
    nodes = [node for node in yaml.compose_all(text, Loader=yaml.BaseLoader) if node is not None]
    if len(nodes) != 1:
        raise ValueError(f"expected one YAML document, got {len(nodes)}")
    root = nodes[0]
    if not isinstance(root, MappingNode):
        raise ValueError(f"expected YAML mapping root, got {type(root).__name__}")
    source_name = yaml_scalar(yaml_get(root, "name")) or Path(path).stem
    author = yaml_scalar(yaml_get(root, "author"))
    explicit_variant = yaml_scalar(yaml_get(root, "variant"))
    metadata = source_metadata(spec, path, source_name, author, explicit_variant)
    metadata["source_blob_sha"] = downloaded["source_blob_sha"]
    metadata["source_sha256"] = downloaded["sha256"]
    tokens: list[dict] = []
    palette = yaml_get(root, "palette")
    if palette is not None:
        for token_path, node in yaml_leaves(palette, "palette"):
            token_name = token_path.removeprefix("palette.")
            value = yaml_scalar(node)
            state = "unset" if (not value or node.tag.endswith(":null") or value.casefold() in {"null", "none", "unset"}) else "set"
            if token_name.casefold() == "include":
                state = "directive"
            add_token(tokens, metadata, token_name, value, token_path=token_path, state=state, raw_line=yaml_line(text, node), line_number=node.start_mark.line + 1)
    else:
        for key, node in yaml_pairs(root):
            low = key.casefold()
            if re.fullmatch(r"color_\d{2}", low) or low in {"background", "foreground", "cursor", "cursor_color", "selection", "selection_background", "selection_foreground", "cursor_text_color"}:
                value = yaml_scalar(node)
                state = "unset" if (not value or node.tag.endswith(":null") or value.casefold() in {"null", "none", "unset"}) else "set"
                add_token(tokens, metadata, key, value, state=state, raw_line=yaml_line(text, node), line_number=node.start_mark.line + 1)
    for key, node in yaml_pairs(root):
        low = key.casefold()
        if low not in {"include", "unset"}:
            continue
        leaves = list(yaml_leaves(node, key))
        if not leaves:
            add_token(tokens, metadata, low, "", token_kind="directive", state="directive" if low == "include" else "unset", raw_line=yaml_line(text, node), line_number=node.start_mark.line + 1)
        for token_path, child in leaves:
            value = yaml_scalar(child)
            if low == "include":
                token_name, token_value, state = "include", value, "directive"
            elif value:
                token_name, token_value, state = value, "", "unset"
            else:
                target = token_path.rsplit(".", 1)[-1].split("[", 1)[0]
                token_name, token_value, state = (target if target != "unset" else "unset"), "", "unset"
            add_token(tokens, metadata, token_name, token_value, token_path=token_path, token_kind="directive", state=state, raw_line=yaml_line(text, child), line_number=child.start_mark.line + 1)
    if not tokens:
        raise ValueError("YAML file contains no recognized palette, color, include, or unset entries")
    return tokens, [metadata]


def parse_config_file(spec: dict, entry: dict, out: Path, downloaded: dict) -> tuple[list[dict], list[dict]]:
    path = entry["path"]
    raw_path = out / downloaded["local_path"]
    text = raw_path.read_text(encoding="utf-8")
    source_name = Path(path).stem if spec["format"] == "kitty-conf" else Path(path).name
    metadata = source_metadata(spec, path, source_name, "", "")
    metadata["source_blob_sha"] = downloaded["source_blob_sha"]
    metadata["source_sha256"] = downloaded["sha256"]
    tokens: list[dict] = []
    line_re = re.compile(r"^\s*([A-Za-z_][A-Za-z0-9_.-]*)\s*(?:=\s*|\s+)(.*?)\s*$")
    for no, line in enumerate(text.splitlines(), 1):
        stripped = line.strip()
        state = "set"
        candidate = line
        if not stripped:
            continue
        if stripped.startswith(("#", "!")):
            candidate = stripped[1:].lstrip()
            state = "commented"
            # Ordinary prose comments are not configuration tokens.
            if not line_re.match(candidate):
                continue
        match = line_re.match(candidate)
        if not match:
            if stripped.casefold().startswith("unset "):
                target = stripped.split(None, 1)[1].strip()
                add_token(tokens, metadata, target, "", token_kind="directive", state="unset", raw_line=line, line_number=no)
            continue
        key, value = match.group(1), match.group(2).strip()
        if key.casefold() == "include":
            add_token(tokens, metadata, key, value.strip("\"'"), token_kind="directive", state="directive", raw_line=line, line_number=no)
            continue
        if key.casefold() == "unset":
            target = value.strip("\"'")
            add_token(tokens, metadata, target, "", token_kind="directive", state="unset", raw_line=line, line_number=no)
            continue
        token_name = key
        token_value = value
        if key.casefold() == "palette":
            pair = re.match(r"\s*(\d+)\s*=\s*(.*?)\s*$", value)
            if pair:
                token_name = f"palette[{pair.group(1)}]"
                token_value = pair.group(2).strip().strip("\"'")
        token_value = token_value.strip().strip("\"'")
        if token_value.casefold() in {"unset", "none", "null"}:
            state = "unset"
            token_value = "" if token_value.casefold() in {"unset", "none", "null"} else token_value
        add_token(tokens, metadata, token_name, token_value, state=state, raw_line=line, line_number=no)
    if not tokens:
        raise ValueError("configuration file contains no recognized entries")
    return tokens, [metadata]


def parse_official_solarized(out: Path, official_record: dict, spec: dict) -> tuple[list[dict], list[dict], dict]:
    source_path = spec["path"]
    raw = (out / official_record["local_path"]).read_bytes()
    text = raw.decode("utf-8")
    lines = text.splitlines()
    macros: dict[str, dict[str, str]] = {"dark": {}, "light": {}, "shared": {}}
    macro_rows: list[dict] = []
    current_section = "shared"
    macro_re = re.compile(r"^\s*(!\s*)?#define\s+(S_[A-Za-z0-9_]+)\s+(#[0-9a-fA-F]{3,8})\s*$")
    role_re = re.compile(r"^\s*\*([A-Za-z0-9_.-]+):\s*(\S+)\s*$")
    scheme_records = []
    for variant in ("dark", "light"):
        name = f"Solarized {variant.title()}"
        meta = source_metadata(spec, source_path, name, "Ethan Schoonover", variant)
        meta["source_blob_sha"] = official_record["source_blob_sha"]
        meta["source_sha256"] = official_record["sha256"]
        scheme_records.append(meta)
    for idx, line in enumerate(lines, 1):
        if re.search(r"^\s*!\s*Dark\s*$", line, re.IGNORECASE):
            current_section = "dark"
        elif re.search(r"^\s*!\s*Light\s*$", line, re.IGNORECASE):
            current_section = "light"
        match = macro_re.match(line)
        if match:
            commented = bool(match.group(1))
            macro, value = match.group(2), match.group(3)
            scope = current_section if macro.startswith("S_base") else "shared"
            state = "commented" if commented else "set"
            macro_rows.append({"macro": macro, "value": value, "scope": scope, "state": state, "line": line, "line_number": idx})
            if scope == "shared" and not commented:
                macros["shared"][macro] = value
            elif scope in {"dark", "light"}:
                macros[scope][macro] = value
            continue
    tokens: list[dict] = []
    for meta in scheme_records:
        variant = meta["variant_identity"]
        # Shared chromatic aliases occur unchanged in both explicit Solarized variants.
        for macro in sorted(macros["shared"]):
            match_row = next(r for r in macro_rows if r["macro"] == macro and r["scope"] == "shared" and r["state"] == "set")
            add_token(tokens, meta, macro, match_row["value"], token_path=f"macros.shared.{macro}", token_kind="named-color", raw_line=match_row["line"], line_number=match_row["line_number"])
        for macro, value in sorted(macros[variant].items()):
            match_row = next(r for r in macro_rows if r["macro"] == macro and r["scope"] == variant)
            add_token(tokens, meta, macro, value, token_path=f"macros.{variant}.{macro}", token_kind="named-color", state=match_row["state"], raw_line=match_row["line"], line_number=match_row["line_number"])
        alias_map = {**macros["shared"], **macros[variant]}
        for idx, line in enumerate(lines, 1):
            role_match = role_re.match(line)
            if not role_match:
                continue
            token_name, value = role_match.groups()
            alias = value.strip()
            resolved = alias_map.get(alias, "")
            hex_value = normalized_hex(value) or normalized_hex(resolved)
            state = "set" if variant == "dark" else "variant-projected-from-commented-light-definitions"
            if not hex_value and alias.startswith("S_"):
                state = "unresolved-reference"
            add_token(tokens, meta, token_name, value, token_path=f"xresources.{token_name}", raw_line=line, line_number=idx, hex_value=hex_value, mapping_basis="Source Xresources role alias; dark uses active macros and light resolves the source's explicitly commented light macro definitions")
            tokens[-1]["state"] = state
    return tokens, scheme_records, {"macro_definitions": len(macro_rows), "macro_scopes": {k: len(v) for k, v in macros.items()}}


def sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def blob_sha(data: bytes) -> str:
    return hashlib.sha1(f"blob {len(data)}\0".encode() + data).hexdigest()


def output_path(root: Path, relative: str) -> Path:
    path = PurePosixPath(relative)
    if path.is_absolute() or ".." in path.parts:
        raise ValueError(f"output path must be relative and contain no '..' components: {relative}")
    target = root.joinpath(*path.parts)
    if not target.resolve().is_relative_to(root):
        raise ValueError(f"output path resolves outside the output directory: {relative}")
    return target


def save_bytes(path: Path, data: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(data)


def save_json(path: Path, value: object) -> None:
    save_bytes(path, (json.dumps(value, indent=2, ensure_ascii=False, sort_keys=True) + "\n").encode())


def fetch(url: str) -> bytes:
    request = Request(url, headers={"User-Agent": "muxe-palette-collector/1"})
    with urlopen(request, timeout=90) as response:
        return response.read()


def collect_archives(out: Path, specs: list[dict]) -> None:
    repositories = sorted({(s["repo"], s["revision"]) for s in specs})
    for repo, revision in repositories:
        archive = fetch(f"https://codeload.github.com/{repo}/tar.gz/{revision}")
        selected_specs = [s for s in specs if (s["repo"], s["revision"]) == (repo, revision)]
        with tarfile.open(fileobj=io.BytesIO(archive), mode="r:gz") as tree:
            for member in tree:
                if not member.isfile():
                    continue
                parts = PurePosixPath(member.name).parts
                relative = PurePosixPath(*parts[1:])
                for spec in selected_specs:
                    prefix = spec["prefix"]
                    if not relative.parts or relative.parts[0] != prefix:
                        continue
                    if spec["format"].endswith("yaml") and relative.suffix.lower() not in {".yaml", ".yml"}:
                        continue
                    leaf = relative.relative_to(prefix).as_posix()
                    target = output_path(out, spec["evidence_root"] + "/" + leaf)
                    stream = tree.extractfile(member)
                    if stream is None:
                        raise ValueError(f"unreadable archive member: {member.name}")
                    with stream:
                        save_bytes(target, stream.read())


def raw_record(out: Path, path: Path, source_id: str, source_path: str) -> dict:
    data = path.read_bytes()
    return {
        "source_id": source_id, "source_path": source_path,
        "local_path": path.relative_to(out).as_posix(),
        "sha256": sha256(data), "source_blob_sha": blob_sha(data),
        "size_bytes": len(data),
    }


def canonical_inputs(out: Path, catalog: dict, offline: bool) -> list[dict]:
    unique = {}
    for variant in catalog["variants"]:
        for source in variant["sources"]:
            record = unique.setdefault(source["url"], {**source, "variant_ids": []})
            record["variant_ids"].append(variant["id"])
    records = []
    for url, source in sorted(unique.items()):
        path = output_path(out, source["evidence_path"])
        if not offline:
            save_bytes(path, fetch(url))
        record = raw_record(out, path, "muxe-canonical", source["source_path"])
        if record["sha256"] != source["sha256"]:
            raise ValueError(f"canonical source SHA-256 does not match the catalog: {url}")
        records.append({**source, **record})
    save_json(out / "canonical-sources.json", {"files": records})
    return records


def muxe_rows(repo: Path, out: Path, catalog: dict) -> tuple[list[dict], list[dict]]:
    rows, records = [], []
    for variant in catalog["variants"]:
        source_path = "crates/muxe-core/assets/color-schemes/" + variant["id"] + ".yml"
        data = (repo / source_path).read_bytes()
        target = output_path(out, "raw/muxe-reference/color-schemes/" + variant["id"] + ".yml")
        save_bytes(target, data)
        record = raw_record(out, target, "muxe-reference", source_path)
        records.append(record)
        node = yaml.compose(data.decode(), Loader=yaml.BaseLoader)
        spec = {
            "source_id": "muxe-reference", "source_family": "Muxe built-ins",
            "repo": "Muxe asset snapshot", "requested_ref": "content-addressed working copy",
            "revision": "sha256:" + record["sha256"], "format": "muxe-yaml",
        }
        meta = source_metadata(spec, source_path, variant["title"], "", infer_variant(variant["id"]))
        meta["source_url"] = ""
        meta["source_blob_sha"] = record["source_blob_sha"]
        meta["source_sha256"] = record["sha256"]
        palette = {}
        for name, value_node in yaml_pairs(yaml_get(node, "palette")):
            value = yaml_scalar(value_node)
            palette[name] = value
            add_token(rows, meta, name, value, token_path="palette." + name,
                      raw_line=yaml_line(data.decode(), value_node),
                      line_number=value_node.start_mark.line + 1,
                      mapping_basis="Muxe embedded named palette; additions are documented in catalog.json")
        for role, value_node in yaml_pairs(yaml_get(node, "colors")):
            alias = yaml_scalar(value_node)
            add_token(rows, meta, alias, alias, token_path="colors." + role,
                      token_kind="semantic-role-projection", hex_value=normalized_hex(palette[alias]),
                      raw_line=yaml_line(data.decode(), value_node),
                      line_number=value_node.start_mark.line + 1,
                      mapping_basis="Muxe YAML semantic alias resolved through its explicit palette")
            rows[-1]["canonical_role"] = role
    return rows, records


def compressed_table(path: Path, rows: list[dict]) -> dict:
    buffer = io.StringIO(newline="")
    writer = csv.DictWriter(buffer, fieldnames=TOKEN_COLUMNS, delimiter="\t", lineterminator="\n", extrasaction="ignore")
    writer.writeheader()
    writer.writerows(rows)
    plain = buffer.getvalue().encode()
    compressed = gzip.compress(plain, compresslevel=9, mtime=0)
    save_bytes(path, compressed)
    return {"path": path.name, "rows": len(rows), "columns": TOKEN_COLUMNS,
            "sha256": sha256(compressed), "uncompressed_sha256": sha256(plain)}


def collect(repo: Path, out: Path, offline: bool) -> dict:
    catalog_path = repo / "crates/muxe-core/assets/catalog.json"
    catalog_bytes = catalog_path.read_bytes()
    catalog = json.loads(catalog_bytes)
    variant_ids = [v["id"] for v in catalog["variants"]]
    embedded_ids = {p.stem for p in (catalog_path.parent / "color-schemes").glob("*.yml")}
    if len(variant_ids) != len(set(variant_ids)) or set(variant_ids) != embedded_ids:
        raise ValueError("catalog variant identifiers are duplicated or differ from embedded YAML filenames")
    if {s["source_id"] for s in catalog["requested_collections"]} != set(FORMATS):
        raise ValueError("catalog requested_collections must contain exactly the four supported palette collections")
    specs = []
    for source in catalog["requested_collections"]:
        family, format_name, prefix = FORMATS[source["source_id"]]
        if not re.fullmatch(r"[0-9a-f]{40}", source["revision"]):
            raise ValueError(f"collection revision must be a 40-character lowercase Git commit hash: {source['source_id']}")
        specs.append({**source, "source_family": family, "format": format_name, "prefix": prefix})
    if not offline:
        collect_archives(out, specs)
    rows, summaries = [], []
    for spec in specs:
        root = output_path(out, spec["evidence_root"])
        paths = sorted(p for p in root.rglob("*") if p.is_file())
        inventory = "".join(f"{p.relative_to(root).as_posix()}\t{sha256(p.read_bytes())}\n" for p in paths)
        if len(paths) != spec["file_count"] or sha256(inventory.encode()) != spec["inventory_sha256"]:
            raise ValueError(f"collection file count or inventory SHA-256 does not match the catalog: {spec['source_id']}")
        token_count = 0
        for path in paths:
            source_path = spec["prefix"] + "/" + path.relative_to(root).as_posix()
            record = raw_record(out, path, spec["source_id"], source_path)
            parser = parse_yaml_file if spec["format"].endswith("yaml") else parse_config_file
            try:
                tokens, _ = parser(spec, {"path": source_path}, out, record)
            except Exception as error:
                raise ValueError(f"{spec['source_id']}:{source_path}: {error}") from error
            rows.extend(tokens)
            token_count += len(tokens)
        summaries.append({"source_id": spec["source_id"], "repo": spec["repo"],
                          "requested_ref": spec["requested_ref"], "revision": spec["revision"],
                          "file_count": len(paths), "token_count": token_count,
                          "inventory_sha256": spec["inventory_sha256"]})
    canonical = canonical_inputs(out, catalog, offline)
    solarized = next(s for v in catalog["variants"] if v["id"] == "solarized-dark" for s in v["sources"])
    official = next(r for r in canonical if r["url"] == solarized["url"])
    spec = {"source_id": "solarized-official", "source_family": "Solarized official reference",
            "repo": "altercation/solarized", "requested_ref": solarized["revision"],
            "revision": solarized["revision"], "path": solarized["source_path"], "format": "xresources"}
    official_rows, _, _ = parse_official_solarized(out, official, spec)
    rows.extend(official_rows)
    reference_rows, reference_records = muxe_rows(repo, out, catalog)
    rows.extend(reference_rows)
    rows.sort(key=lambda r: (r["source_id"], r["normalized_identity"], r["variant_identity"],
                             r["source_path"], r["token_path"], str(r["line_number"])))
    table = compressed_table(out / "tokens.tsv.gz", rows)
    return {
        "status": "complete", "schema_version": 1,
        "collector_sha256": sha256(Path(__file__).read_bytes()),
        "catalog_sha256": sha256(catalog_bytes), "requested_collections": summaries,
        "requested_scheme_count": sum(s["file_count"] for s in summaries),
        "official_solarized_variants": 2, "muxe_variant_count": len(variant_ids),
        "token_states": dict(sorted(Counter(r["state"] for r in rows).items())),
        "table": table,
        "canonical_sources": {"path": "canonical-sources.json", "file_count": len(canonical),
                              "sha256": sha256((out / "canonical-sources.json").read_bytes())},
        "muxe_assets": reference_records,
        "identity_policy": "Source name/path/revision remain separate. Prefix-normalized candidate names do not prove identity. Flavor, polarity and contrast labels are different axes; variant_relation describes labels, not scheme equivalence.",
        "token_policy": "Original tokens, raw lines, locations and explicit include/unset/commented states are retained. Missing fields have no token row. Short hex expands without changing channel values; eight-digit hex retains source alpha/component bytes and is not composited. Solarized light roles are explicitly projected from commented source macros.",
        "reproduction_scope": "Deterministic for identical raw inputs, embedded assets, collector and Python/PyYAML versions; compressed table has zero gzip timestamp. This checks provenance, not visual acceptance.",
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True, help="Collection output directory outside tracked source; raw/ is retained for the independent checker")
    parser.add_argument("--repo", type=Path, default=Path(__file__).resolve().parents[1])
    parser.add_argument("--offline", action="store_true", help="Use existing raw inputs in --output; make no network requests")
    args = parser.parse_args()
    out = args.output.expanduser().resolve()
    out.mkdir(parents=True, exist_ok=True)
    if shutil.disk_usage(out).free < FREE_SPACE_FLOOR:
        raise SystemExit("collection stopped: output filesystem has less than 25 GiB free")
    save_json(out / "manifest.json", {"status": "incomplete"})
    try:
        manifest = collect(args.repo.expanduser().resolve(), out, args.offline)
    except Exception as error:
        save_json(out / "manifest.json", {"status": "failed", "error": f"{type(error).__name__}: {error}"})
        raise
    save_json(out / "manifest.json", manifest)
    print(f"Collected {manifest['requested_scheme_count']} schemes from the catalog's recorded commits plus 2 official Solarized variants and {manifest['muxe_variant_count']} Muxe assets; {manifest['table']['rows']} token rows in {out / 'tokens.tsv.gz'}")


if __name__ == "__main__":
    main()
