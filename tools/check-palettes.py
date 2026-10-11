#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.14"
# dependencies = ["PyYAML==6.0.3"]
# ///
"""Reproduce built-in palette provenance and contrast from pinned raw evidence.

Run with --evidence /path/to/muxe-color-evidence. No network, renderer, host,
configuration, or repository writes. This checks data, not visual acceptance.
"""

from __future__ import annotations

import argparse
import colorsys
import hashlib
import json
import math
from pathlib import Path
import re

import yaml


HEX = re.compile(r"#[0-9a-fA-F]{6}\Z")
FAMILIES = {
    "catppuccin": r"catppuccin", "nord": r"nord", "dracula": r"dracula",
    "solarized": r"solarized|solardark", "gruvbox": r"gruvbox", "tokyo": r"tokyo",
    "rose": r"ros[eé].?pine", "everforest": r"everforest",
    "one": r"(?:^|[^a-z])(?:atom[ -]?)?one[ _-]?(?:dark|light)",
    "kanagawa": r"kanagawa",
}


def require(condition: bool, message: str) -> None:
    if not condition:
        raise ValueError(message)


def digest(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def channels(value: str) -> tuple[int, int, int]:
    require(bool(HEX.fullmatch(value)), f"invalid RGB: {value}")
    return tuple(int(value[i:i + 2], 16) for i in (1, 3, 5))


def hex_rgb(values) -> str:
    return "#" + "".join(f"{max(0, min(255, math.floor(c + 0.5))):02x}" for c in values)


def luminance(value: str) -> float:
    linear = [c / 255 / 12.92 if c / 255 <= 0.04045 else ((c / 255 + 0.055) / 1.055) ** 2.4 for c in channels(value)]
    return sum(c * w for c, w in zip(linear, (0.2126, 0.7152, 0.0722)))


def contrast(a: str, b: str) -> float:
    x, y = luminance(a), luminance(b)
    return (max(x, y) + 0.05) / (min(x, y) + 0.05)


def flatten_lua_colors(text: str) -> dict[str, str]:
    """Read the generated colors table, never execute upstream Lua."""
    result = {}
    stack = []
    for line in text.splitlines():
        line = line.split("--", 1)[0].strip()
        if line.startswith("}"):
            if not stack:
                break
            stack.pop()
            continue
        match = re.match(r'(\w+)\s*=\s*"(#[0-9a-fA-F]{6})"', line)
        if match:
            result[".".join(stack + [match[1]])] = match[2].lower()
            continue
        match = re.match(r"(\w+)\s*=\s*\{(.*)", line)
        if match:
            if "}" in match[2]:
                for index, value in enumerate(re.findall(r'"(#[0-9a-fA-F]{6})"', match[2]), 1):
                    result[".".join(stack + [match[1], str(index)])] = value.lower()
            else:
                stack.append(match[1])
    return result


def less_palette(text: str) -> dict[str, str]:
    """Evaluate only the color-expression grammar used by the pinned Atom files.

    HSL arithmetic keeps unrounded channels. At the CSS boundary Less rounds
    RGB channels; alpha colors then composite those channels over syntax-bg.
    """
    declarations = dict(re.findall(r"^@([\w-]+):\s*(.*?);", text, re.M))
    cache = {}

    def evaluate(expression):
        expression = expression.strip()
        if expression.startswith("@"):
            name = expression[1:]
            if name not in cache:
                cache[name] = evaluate(declarations[name])
            return cache[name]
        if expression == "white":
            return (255.0, 255.0, 255.0, 1.0)
        if re.fullmatch(r"[\d.]+%?", expression):
            return float(expression.rstrip("%"))
        match = re.fullmatch(r"(hsl|hsla|darken|lighten|fade)\((.*)\)", expression)
        require(match is not None, f"unsupported pinned Less expression: {expression}")
        operation, body = match.groups()
        args = [evaluate(arg) for arg in body.split(",")]
        if operation in {"hsl", "hsla"}:
            hue, saturation, lightness = args[:3]
            return (*[c * 255 for c in colorsys.hls_to_rgb(hue / 360, lightness / 100, saturation / 100)], args[3] if len(args) == 4 else 1.0)
        red, green, blue, alpha = args[0]
        if operation == "fade":
            return (red, green, blue, args[1] / 100)
        hue, lightness, saturation = colorsys.rgb_to_hls(red / 255, green / 255, blue / 255)
        lightness = max(0, min(1, lightness + args[1] / 100 * (1 if operation == "lighten" else -1)))
        return (*[c * 255 for c in colorsys.hls_to_rgb(hue, lightness, saturation)], alpha)

    background = channels(hex_rgb(evaluate("@syntax-bg")[:3]))
    result = {}
    for name in declarations:
        value = evaluate("@" + name)
        if not isinstance(value, tuple):
            continue
        rgb = channels(hex_rgb(value[:3]))
        result[name] = hex_rgb(c * value[3] + b * (1 - value[3]) for c, b in zip(rgb, background))
    return result


def canonical_palette(variant: dict, source_text: dict[str, str]) -> dict[str, str]:
    name = variant["id"]
    texts = [source_text[source["url"]] for source in variant["sources"]]
    text = texts[0]
    if name.startswith("catppuccin-"):
        flavor = json.loads(text)[name.split("-")[1]]
        result = {key: value["hex"].lower() for key, value in flavor["colors"].items()}
        result.update({f"ansiColors.{key}.{weight}": value[weight]["hex"].lower() for key, value in flavor["ansiColors"].items() for weight in ("normal", "bright")})
        return result
    if name == "nord":
        return {k: v.lower() for k, v in re.findall(r"--(nord\d+):\s*(#[0-9a-fA-F]{6})", text)}
    if name == "dracula":
        rows = re.findall(r"^\|\s*([A-Za-z ]+)\s*\|\s*`(#[0-9a-fA-F]{6})`", text, re.M)
        # The same README also contains a separate light palette.
        return {key.strip().lower().replace(" ", "_"): value.lower() for key, value in rows[:12]}
    if name.startswith("solarized-"):
        return {key: value.lower() for key, value in re.findall(r"^#define S_(\w+)\s+(#[0-9a-fA-F]{6})", text, re.M)}
    if name.startswith("gruvbox-"):
        result = {key: value.lower() for key, value in re.findall(r"^let s:gb\.(\w+)\s*=\s*\['(#[0-9a-fA-F]{6})'", text, re.M)}
        dark = "-dark" in name
        bg, fg = ("dark", "light") if dark else ("light", "dark")
        for index in range(5):
            result[f"bg{index}"] = result[f"{bg}{index}" + ("_hard" if index == 0 and name.endswith("-hard") else "")]
            result[f"fg{index}"] = result[f"{fg}{index}"]
        result["fg4_256"] = result[f"{fg}4_256"]
        result["gray"] = result["gray_245" if dark else "gray_244"]
        for accent in ("red", "green", "yellow", "blue", "purple", "aqua", "orange"):
            result[accent] = result[("bright_" if dark else "faded_") + accent]
        return result
    if name.startswith("tokyo-night"):
        return flatten_lua_colors(text.split("local colors = {", 1)[1])
    if name.startswith("rose-pine"):
        variant_name = {"rose-pine": "main", "rose-pine-moon": "moon", "rose-pine-dawn": "dawn"}[name]
        block = re.search(r"\b" + variant_name + r"\s*=\s*\{(.*?)\n\s*\}", text, re.S)[1]
        return {key: value.lower() for key, value in re.findall(r'(\w+)\s*=\s*"(#[0-9a-fA-F]{6})"', block)}
    if name.startswith("everforest-"):
        text = next(t for t in texts if "function! everforest#get_palette" in t)
        blocks = re.findall(r"let palette[12] = \{(.*?)\\ \}", text, re.S)
        require(len(blocks) == 8, "Everforest pinned palette block structure changed")
        _, mode, weight = name.split("-")
        offset = {"hard": 0, "medium": 2, "soft": 4}[weight] + (mode == "light")
        chosen = blocks[offset] + blocks[6 + (mode == "light")]
        return {key: value.lower() for key, value in re.findall(r"'(\w+)'\s*:\s*\['(#[0-9a-fA-F]{6})'", chosen)}
    if name.startswith("one-"):
        return less_palette("\n".join(texts))
    if name.startswith("kanagawa-"):
        text = next(t for t in texts if "local palette = {" in t)
        return flatten_lua_colors(text.split("local palette = {", 1)[1])
    raise ValueError(f"unrecognized canonical source adapter: {name}")


def port_palette(path: Path, collection: str) -> dict[str, str]:
    text = path.read_text()
    if collection in {"gogh", "tinted-base24-spec-0.11"}:
        document = yaml.safe_load(text)
        palette = document.get("palette", document)
        return {key: value.lower() for key, value in palette.items() if isinstance(value, str) and HEX.fullmatch(value)}
    result = {}
    for line in text.splitlines():
        match = re.fullmatch(r"\s*([\w_-]+)\s*(?:=\s*|\s+)(#[0-9a-fA-F]{6})\s*", line)
        indexed = re.fullmatch(r"\s*palette\s*=\s*(\d+)\s*=\s*(#[0-9a-fA-F]{6})\s*", line)
        if indexed:
            result[f"palette[{indexed[1]}]"] = indexed[2].lower()
        elif match:
            result[match[1]] = match[2].lower()
    require(bool(result), f"no RGB tokens: {path}")
    return result


def derived_color(item: dict, palette: dict[str, str]) -> str:
    source = palette[item["source_alias"]]
    require(source == item["source_hex"], "derived source RGB changed")
    rgb, parameter = channels(source), item["parameter"]
    if item["operation"] == "scale_channels":
        return hex_rgb(c * parameter for c in rgb)
    if item["operation"] == "mix_channels":
        second = palette[item["second_alias"]]
        require(second == item["second_hex"], "derived second source RGB changed")
        return hex_rgb(a * (1 - parameter) + b * parameter for a, b in zip(rgb, channels(second)))
    if item["operation"] == "set_hsl_hue":
        _, lightness, saturation = colorsys.rgb_to_hls(*(c / 255 for c in rgb))
        return hex_rgb(c * 255 for c in colorsys.hls_to_rgb(parameter / 360, lightness, saturation))
    raise ValueError(f"unsupported derivation: {item['operation']}")


def check_contrast(variant: dict, palette: dict[str, str]) -> None:
    roles = variant["semantic"]
    colors = {role: palette[alias] for role, alias in roles.items()}
    background = colors["base.background"]
    require(background == palette[variant["upstream_roles"]["background"]], f"{variant['id']}: iconic source background changed")
    require(len({colors[r] for r in ("base.text", "base.muted", "menu.separator")}) == 3, f"{variant['id']}: collapsed neutral roles")
    require(contrast(colors["base.text"], background) > contrast(colors["base.muted"], background), f"{variant['id']}: secondary hierarchy reversed")
    require(len({value for key, value in colors.items() if key.startswith("status.")}) == 5, f"{variant['id']}: collapsed states")
    record = variant["contrast"]
    for role, slot in record["slots"].items():
        measured = contrast(colors[role], background)
        require(abs(measured - slot["ratio"]) < 0.000001, f"{variant['id']}: stale {role} contrast")
        require(slot["below_normal_text_reference"] == (measured < record["normal_text_reference"]), f"{variant['id']}: stale contrast disclosure")
    for key, role in (("chevron_badge", "menu.hotkey"), ("disabled_chevron_badge", "base.muted")):
        measured = contrast(background, colors[role])
        require(abs(measured - record[key]["ratio"]) < 0.000001, f"{variant['id']}: stale {key} contrast")
        require(record[key]["below_normal_text_reference"] == (measured < record["normal_text_reference"]), f"{variant['id']}: stale {key} disclosure")
    for pair, recorded in record["neutral_pair_contrast"].items():
        first, second = pair.split(" / ")
        require(abs(contrast(colors[first], colors[second]) - recorded) < 0.000001, f"{variant['id']}: stale neutral-pair contrast")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--evidence", type=Path, required=True, help="Preserved collection root containing raw/ and canonical-sources.json")
    parser.add_argument("--repo", type=Path, default=Path(__file__).resolve().parents[1])
    args = parser.parse_args()
    assets = args.repo / "crates/muxe-core/assets"
    catalog = json.loads((assets / "catalog.json").read_text())
    variant_ids = [variant["id"] for variant in catalog["variants"]]
    embedded_ids = {path.stem for path in (assets / "color-schemes").glob("*.yml")}
    require(len(variant_ids) == len(set(variant_ids)), "duplicate catalog variant identifiers")
    require(set(variant_ids) == embedded_ids,
            f"catalog coverage differs from embedded schemes: missing={embedded_ids - set(variant_ids)}, extra={set(variant_ids) - embedded_ids}")
    source_text = {}
    for variant in catalog["variants"]:
        for source in variant["sources"]:
            if source["url"] not in source_text:
                data = (args.evidence / source["evidence_path"]).read_bytes()
                require(digest(data) == source["sha256"], f"canonical digest: {source['url']}")
                source_text[source["url"]] = data.decode()

    source_palettes = {}
    for collection in catalog["requested_collections"]:
        root = args.evidence / collection["evidence_root"]
        paths = sorted(p for p in root.rglob("*") if p.is_file())
        inventory = "".join(f"{p.relative_to(root).as_posix()}\t{digest(p.read_bytes())}\n" for p in paths)
        require(len(paths) == collection["file_count"], f"collection count: {collection['source_id']}")
        require(digest(inventory.encode()) == collection["inventory_sha256"], f"collection inventory: {collection['source_id']}")
        candidate_paths = {p.relative_to(root).as_posix() for p in paths if any(re.search(pattern, p.name, re.I) for pattern in FAMILIES.values())}
        records = [s for s in catalog["comparison_sources"] if s["collection"] == collection["source_id"]]
        require(candidate_paths == {s["relative_path"] for s in records}, f"incomplete family candidate coverage: {collection['source_id']}")
        for source in records:
            path = root / source["relative_path"]
            require(digest(path.read_bytes()) == source["sha256"], f"comparison digest: {source['source_path']}")
            palette = port_palette(path, collection["source_id"])
            require(palette == source["palette"], f"comparison normalization: {source['source_path']}")
            source_palettes[(source["collection"], source["source_path"])] = palette

    for variant in catalog["variants"]:
        palette = canonical_palette(variant, source_text)
        for item in variant.get("derived_colors", []):
            value = derived_color(item, palette)
            require(value == item["hex"], f"{variant['id']}: derivation {item['palette_name']}")
            palette[item["palette_name"]] = value
        require(palette == variant["palette"], f"{variant['id']}: canonical palette incomplete or altered; missing={set(palette) - set(variant['palette'])}, extra={set(variant['palette']) - set(palette)}, changed={[k for k in palette.keys() & variant['palette'].keys() if palette[k] != variant['palette'][k]]}")
        consumed = yaml.safe_load((assets / "color-schemes" / (variant["id"] + ".yml")).read_text())
        require(consumed["palette"] == palette, f"{variant['id']}: embedded palette differs from normalized source")
        require(consumed["colors"] == variant["semantic"], f"{variant['id']}: consumed semantic map differs")
        canonical_values = {value for key, value in palette.items() if not key.startswith("muxe_")}
        for comparison in variant["comparisons"]:
            alternative = source_palettes[(comparison["collection"], comparison["source_path"])]
            outside = {key: value for key, value in alternative.items() if value not in canonical_values}
            require(outside == comparison["noncanonical_entries"], f"{variant['id']}: stale source discrepancy classification")
        check_contrast(variant, palette)
        print(f"{variant['id']}: {len(palette)} reproducible colors; source coverage, derivations and role contrasts agree")
    print(f"Reproduced {len(catalog['variants'])} schemes, {len(source_text)} official files, {len(source_palettes)} comparison files. Visual acceptance is separate.")


if __name__ == "__main__":
    main()
