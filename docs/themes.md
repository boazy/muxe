# Themes and color schemes

A **theme** defines menu presentation: shortcut punctuation, labels, breadcrumbs, pagination, and status text. A **color scheme** supplies a named palette and maps it to semantic color roles. Select them independently; every built-in theme can use every built-in scheme.

## Discover and select

Run `muxe config themes` to list embedded and user-defined names. Discovery needs neither an installed configuration file nor a live host. Built-ins are embedded in the executable; `muxe init` does not copy them into the configuration directory.

```yaml
version: 1
theme: brackets
color-scheme: catppuccin-mocha
menus:
  main:
    bindings:
      g:
        label: Git
        action: menu:quit
```

A menu invocation can override either selection with `muxe menu open --theme dots --color-scheme solarized-light main`. These overrides affect that attachment, not other menus. Configuration checks, broker startup and reload, and menu attachments resolve names through the same catalog. Command panes retain their existing behavior and are not recolored as menus.

The default selection is `default` for both catalogs. The default scheme explicitly resets semantic foregrounds and backgrounds to the terminal's defaults; it does not force an RGB palette.

User assets live in `$CONFIG_DIR/themes/<name>.yml` and `$CONFIG_DIR/color-schemes/<name>.yml`. A user file replaces the **entire** embedded asset with the same name, including `default`; it does not merge individual styles or palette entries. A malformed replacement never falls back to the embedded asset. YAML syntax errors prevent loading even for unselected files. Definition errors remain associated with the asset until it is selected.

## Display themes

| Name | Example | Presentation |
| --- | --- | --- |
| `default` | `g      → Git` | Padded-key arrow presentation with a two-column state gutter. |
| `brackets` | `[g] Git` | ASCII keycaps with a shared shortcut width. |
| `dots` | `g ... Git` | Subdued dot leaders that fill the gap before each label. |
| `rail` | `│ g │ Git` | Aligned Unicode rules around the shortcut column. |
| `chevron` | ` g  Git` | Reversed-color key badges joined to Nerd Font Powerline tips. |

All built-in themes align labels within each grid column using the widest shortcut that passes the current menu's visibility conditions; hidden, excluded, and unshown bindings do not widen the shortcut column. Default keeps a six-column minimum and never truncates a longer shortcut. Default, Brackets, Rail, and Chevron pad shorter shortcuts with spaces. Dots fills that space with leaders, with at least three dots and a space at each end to keep punctuation shortcuts identifiable.

Each theme reserves two columns before the shortcut for a state marker, so disabled and blocked rows keep the same alignment as enabled rows. Shortcut widths use terminal display columns, including wide Unicode characters. Grid packing and clipping still follow the layout settings. A viewport narrower than a shortcut and its decoration clips the row.

Custom cell templates can use `key_width` for the shared shortcut width and `key_padding` for the difference between that width and the current key's display width. For example, `{{ key | rpad(key_width) }}` pads a shortcut without cutting it. The renderer never supplies a `key_width` smaller than the current key.

All built-in themes mark disabled bindings with `- ` and blocked bindings with `! `. The blocked marker uses `status.blocked`; disabled labels use `base.muted` without additional terminal dimming. Default, Brackets, Dots, and Rail also use `base.muted` for disabled shortcuts, while their arrows, brackets, dots, and rules retain `menu.separator` in every state. Chevron instead keeps each tip connected to its badge background. The markers preserve binding-state distinctions when colors inherit terminal defaults or color output is disabled.

Default retains its arrow glyph and bold enabled labels. The four non-default themes use regular-weight labels and prefix status messages with `Error:`, `Pending:`, `Blocked:`, `Reload:`, or `Notice:`. Default displays the supplied status message without adding a severity prefix.

Breadcrumb ancestors use `base.muted`, separator glyphs use `menu.separator`, and the current location uses the title style. Muxe has no persistent selected-row highlight; Chevron's badge decorates the shortcut rather than indicating focus.

Non-default themes may use Nerd Font glyphs. Configure your terminal to use a Nerd Font for Chevron, which uses the Powerline right divider (`U+E0B0`). Its tip matches the badge background for both enabled and disabled bindings. Brackets and Dots remain ASCII, and Rail uses ordinary Unicode rules. The default theme has no Nerd Font requirement. Muxe does not detect the terminal's font automatically.

`NO_COLOR` suppresses color output while retaining attributes such as bold. Textual punctuation and state markers remain meaningful even if a terminal also ignores attributes.

## Surface and style backgrounds

The theme's resolved `default` style paints the complete menu viewport before component spans, including padding, column gaps, and unused rows. A resize repaints the resulting viewport. Omitting that style leaves the base surface at the terminal's default style.

Component styles apply on top of the surface. Nested markup selects the innermost named style; it does not merge that style with an outer markup tag. The additional themes therefore specify foreground and background on every named text style. Chevron deliberately reverses the shortcut badge's colors. User themes can use the same behavior without adding a semantic slot.

In literal template markup, escape square brackets as `\[` and `\]`, and a literal backslash as `\\`. Muxe escapes inserted keys, labels, breadcrumbs, and status text automatically, so their punctuation cannot become a style tag or consume the following closing tag.

## Color-scheme inventory

| Family | Selection names |
| --- | --- |
| Catppuccin | `catppuccin-latte`, `catppuccin-frappe`, `catppuccin-macchiato`, `catppuccin-mocha` |
| Nord | `nord` |
| Dracula | `dracula` |
| Solarized | `solarized-dark`, `solarized-light` |
| Gruvbox | `gruvbox-dark`, `gruvbox-dark-hard`, `gruvbox-light`, `gruvbox-light-hard` |
| Tokyo Night | `tokyo-night`, `tokyo-night-storm`, `tokyo-night-moon`, `tokyo-night-day` |
| Rosé Pine | `rose-pine`, `rose-pine-moon`, `rose-pine-dawn` |
| Everforest | `everforest-dark-hard`, `everforest-dark-medium`, `everforest-dark-soft`, `everforest-light-hard`, `everforest-light-medium`, `everforest-light-soft` |
| One | `one-dark`, `one-light` |
| Kanagawa | `kanagawa-wave`, `kanagawa-dragon`, `kanagawa-lotus` |

The catalog contains thirty explicit color schemes plus the terminal-inheriting `default`. Unqualified Gruvbox Dark and Light use the upstream medium-contrast background. Everforest names expose the upstream contrast variants explicitly.

Each scheme retains the complete chosen upstream named palette, including aliases and entries unused by Muxe. Nested names are flattened without dropping their parent namespace. Canonical RGB swatches and iconic backgrounds remain intact. Menu roles may select a stronger existing foreground or a documented Muxe adaptation when the upstream default leaves insufficient separation between labels, secondary text and structural marks. Semantic mappings are explicit in each asset's `colors` section:

| Semantic role | Assignment |
| --- | --- |
| `base.background` | The selected canonical surface. |
| `base.text` | Primary menu text, with a source-backed emphasis choice where needed. |
| `base.muted` | A subdued version of the primary text, not a chromatic accent substitute. |
| `menu.separator` | An independently readable color for arrows, brackets, dots and breadcrumb separators, distinct from both text roles. |
| `menu.hotkey` | An accent that distinguishes shortcuts from labels and separators. |
| `status.error` | Red or rose. |
| `status.pending` | Yellow or gold. |
| `status.blocked` | A warm attention color, distinct from pending and error; orange, peach or rose according to the palette. |
| `status.reload` | Green. |
| `status.notice` | Blue or cyan. |

Palette names and semantic names share the existing lookup namespace; a semantic name takes precedence. Palette values are literal `#rgb` or `#rrggbb` values. Semantic mappings may refer to palette entries or other semantic names. Unknown aliases and cycles are rejected, including unused entries. `inherit` is reserved for the embedded default scheme; in configuration style fields it remains an ordinary alias name.

Assignments preserve each palette's character while keeping simultaneously visible roles distinguishable. Muted text and separators must not share a color; status hues must remain distinguishable from one another. A lower-contrast accent is not silently replaced with neutral text. Muxe additions use separate `muxe_*` names and record source inputs, operation, color space, rounding, output and rejected candidates in the catalog.

The catalog records sRGB contrast against each role's background, including enabled and disabled Chevron badges, and flags ratios below the 4.5:1 normal-text reference. This reference is a diagnostic, not a universal requirement or an accessibility guarantee. Some light-theme accents and thin separators fall below it. Numeric differences alone do not establish readability: review the actual menu at ordinary font sizes, including disabled rows, breadcrumbs and all status levels. Binding-state markers and non-default status prefixes retain cues without color. Font rendering, terminal dimming and inherited terminal colors are not covered by the measurements.

The catalog's visual-validation record scopes the reference evidence: actual `TerminalSurface` output captured through retained PTYs and replayed from its emitted cells, using installed FiraCode Nerd Font Mono regular/bold and Hiragino CJK fallback at 14px in a 9×20-pixel grid. These images are ANSI replays, not native terminal screenshots or certification of a terminal/font combination. Inherited colors use explicit dark/light reference defaults. Other fonts, spacing, antialiasing and terminal dimming can differ; the fixture does not certify broker or host lifecycle behavior.

One Dark and One Light retain all 51 color-bearing variables from their official Less files, including aliases and six alpha roles. Muxe's assets are RGB-only, so alpha roles are composited over that variant's original `syntax-bg`. Metadata preserves the raw upstream expression, reference-compiled RGBA value, compositing background, formula, and resulting RGB. These composites are source-backed adaptations, not unchanged upstream hex literals. Non-color configuration parameters and `NONE` sentinels are retained as metadata rather than treated as RGB swatches.

### Family-specific assignments

| Family | Menu adaptation |
| --- | --- |
| Catppuccin | Use the official `text`, `subtext0` and `overlay1` neutral levels; retain all named and ANSI entries for each flavor. |
| Nord | Derive secondary text from Snow Storm and a readable structural grey from Polar Night. Frost blue remains an accent rather than masquerading as muted text. |
| Dracula | Derive a subdued warm foreground; retain violet-blue `comment` for punctuation. |
| Solarized | Dark derives a stronger cool primary from `base0`; Light uses canonical `base02`. Both retain the official default foreground as secondary text. The source-role distinction is explained below. |
| Gruvbox | Use the warm `fg1` / `fg3` / `gray` sequence, variant-specific surfaces, and bright dark-mode or faded light-mode accents. |
| Tokyo Night | Keep each variant's `fg` / `fg_dark` pair. Structure uses `dark5` in Night/Storm and `comment` in Moon/Day. Day's blocked orange is adapted because its canonical orange and pending ochre have nearly the same hue. |
| Rosé Pine | Use `text` / `subtle` / `muted`; blocked `rose` is separate from pending `gold` and error `love`. Retain the pinned palette's `leaf` color. |
| Everforest | Dark variants use a subdued beige foreground and green-grey structure. Light variants strengthen the cool-grey primary ink and use the unchanged official foreground as secondary text. Preserve each `bg0`, even where a terminal port chooses `bg_dim`. |
| One | Keep official primary and secondary inks; add per-variant structural greys for thin marks. One Light also adds a blocked orange because its two canonical ochres share the same hue. |
| Kanagawa | Preserve all 103 shared canonical names. Wave adds a subdued Fuji neutral; Dragon uses its own grey sequence; Lotus strengthens its cool primary ink instead of using brown-grey secondary text. |

One Dark's `muxe_mono_separator` mixes `mono-3` 45% toward `mono-2`, producing `#6d7482`; One Light mixes its `mono-3` 40% toward `mono-2`, producing `#8a8c94`. These encoded-sRGB channel mixes strengthen thin marks while leaving secondary text distinct. Final channels round to the nearest integer, ties upward. All original monochrome swatches remain available.


## Palette sources and licenses

The asset metadata at [`crates/muxe-core/assets/catalog.json`](../crates/muxe-core/assets/catalog.json) records immutable canonical sources, raw hashes, complete palettes, semantic decisions, derivations and licenses. It also records comparisons against all plausible family candidates in these pinned collections:

| Collection | Immutable revision |
| --- | --- |
| [Gogh, all theme YAML files](https://github.com/Gogh-Co/Gogh/tree/2fbe9bbf3715173fc6f477d4992b66f11afca2b1/themes) | `2fbe9bbf3715173fc6f477d4992b66f11afca2b1` |
| [Tinted Base24, `spec-0.11`](https://github.com/tinted-theming/schemes/tree/3ac91137e879502c442ab89ba0f1a98077d0da9d/base24) | `3ac91137e879502c442ab89ba0f1a98077d0da9d` |
| [iTerm2 Color Schemes, all Kitty files](https://github.com/mbadolato/iTerm2-Color-Schemes/tree/c02052159ff9a1c438ff419b7451cd33f9ab4934/kitty) | `c02052159ff9a1c438ff419b7451cd33f9ab4934` |
| [iTerm2 Color Schemes, all Ghostty files](https://github.com/mbadolato/iTerm2-Color-Schemes/tree/c02052159ff9a1c438ff419b7451cd33f9ab4934/ghostty) | `c02052159ff9a1c438ff419b7451cd33f9ab4934` |

The adjacent [`LICENSES.txt`](../crates/muxe-core/assets/LICENSES.txt) preserves upstream terms, including Gogh's chosen MIT option. Collection licenses do not replace the individual palette authors' licenses.

Comparisons retain every alternative RGB entry and its matching canonical names, then separately compare declared background and foreground roles. Value membership is not role agreement. Source names generate candidates, not identity verdicts: a Catppuccin flavor such as `frappe` and a polarity such as `dark` describe different axes. Multiple paths remain separate, and missing variants remain explicit. Muxe does not average ports or treat agreement as a vote.

For example, the pinned Rosé Pine Dawn editor source uses text `#464261`, while terminal ports use `#575279`. Everforest ports often select a variant's dimmed surface rather than its default surface. Legacy Tokyo Night Light is not current Day. These differences do not justify silently changing canonical swatches or substituting one variant for another.

### Solarized whites and foregrounds

The [official Xresources source](https://github.com/altercation/solarized/blob/62f656a02f93c5190a8753159e34b385588d5ff3/xresources/solarized) distinguishes named colors from terminal roles. Its active dark configuration uses `base0` (`#839496`) as default foreground, `base2` (`#eee8d5`) as ANSI white, and `base3` (`#fdf6e3`) as bright white. Its commented light configuration remaps the neutral macros. Muxe keeps absolute palette names in both modes.

The requested ports make different assignments:

| Source | Default foreground | White-related roles |
| --- | --- | --- |
| Gogh Builtin Solarized, dark and light | `#b2b8ad` | Normal ANSI white is also `#b2b8ad`; bright white is `#fdf6e3` in dark and `#002b36` in light. |
| Tinted Base24 `builtin-solarized`, dark and light | `base05 = #b2b8ad` | `base06 = #eee8d5`; `base07` is `#fdf6e3` in dark and `#002b36` in light. These are Base24 slots, not absolute Solarized names. |
| iTerm2 Solarized, Kitty and Ghostty | Dark `#839496`; light `#657b83` | ANSI white is dark `#eee8d5` or light `#bbb5a2`; bright white is `#fdf6e3` in both. |

Thus Gogh's normal white agrees with Base24's default foreground, not its `base06` slot. `#b2b8ad` is not an official absolute Solarized swatch. Cursor and selection colors are recorded separately; their values are not votes for primary text. The pinned files establish these differences but do not establish a unique historical conversion algorithm.

For menus, Solarized Dark maps text / muted / separator to `muxe_base0_emphasis` (`#b7cfd2`) / `base0` / `base00`. Its primary scales the encoded-sRGB channels of `base0` by 1.4, rounding to the nearest integer with ties upward. This keeps primary and secondary text in the same cool tint instead of pairing warm `base2` labels with cool `base0` secondary text. Light uses `base02` / `base00` / `base0`. Stronger primary labels leave room for readable secondary text and thin marks without forcing both into dim adjacent steps. Canonical colors, including ANSI white `base2`, remain unchanged; these are menu-role adaptations, not changes to the official default foregrounds.

### Reproduce the source evidence

[`tools/collect-palettes.py`](../tools/collect-palettes.py) collects the entire four-source corpus, including schemes unrelated to Muxe's thirty built-ins. It uses the catalog's immutable revisions and complete file-inventory hashes, fetches all canonical source files, and includes both official Solarized modes and the current embedded Muxe assets in one long token table. Output must be explicit and should remain outside tracked source:

```sh
uv run tools/collect-palettes.py --output "$HOME/.cache/muxe/palette-evidence"
uv run tools/check-palettes.py --evidence "$HOME/.cache/muxe/palette-evidence"
```

The collector retains raw inputs under `raw/` and writes `tokens.tsv.gz`, `canonical-sources.json` and `manifest.json`. The compressed TSV preserves source name, path, revision, hashes, variant labels, slot, role, original token, normalized value, raw line and line number. Includes, unset values and commented definitions remain explicit; missing fields are not converted to black. The manifest records complete collection counts and hashes. Compression uses a zero timestamp; identical inputs and tool versions produce deterministic evidence. Do not commit the raw corpus or full uncompressed tables.

To regenerate the table from retained raw inputs without network access:

```sh
uv run tools/collect-palettes.py --offline --output "$HOME/.cache/muxe/palette-evidence"
```

[`tools/check-palettes.py`](../tools/check-palettes.py) independently normalizes the pinned canonical palettes, reproduces derived colors and alpha composites, and recalculates semantic and badge contrast. Its catalog-ID coverage check also detects duplicate or omitted embedded schemes. Both tools reject incomplete or changed pinned inventories. Neither tool certifies actual-font readability; visual review must exercise all display themes, state markers, breadcrumbs and status hues.
