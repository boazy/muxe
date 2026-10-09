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

The default selection is `default` for both catalogs. The default scheme explicitly resets semantic foregrounds and backgrounds to the terminal's defaults; it does not force an RGB palette. The default presentation remains unchanged.

User assets live in `$CONFIG_DIR/themes/<name>.yml` and `$CONFIG_DIR/color-schemes/<name>.yml`. A user file replaces the **entire** embedded asset with the same name, including `default`; it does not merge individual styles or palette entries. A malformed replacement never falls back to the embedded asset. YAML syntax errors prevent loading even for unselected files. Definition errors remain associated with the asset until it is selected.

## Display themes

| Name | Example | Presentation |
| --- | --- | --- |
| `default` | `g      → Git` | Existing padded-key arrow presentation. |
| `brackets` | `[g] Git` | ASCII keycaps with a shared shortcut width. |
| `dots` | `g ... Git` | Subdued dot leaders that fill the gap before each label. |
| `rail` | `│ g │ Git` | Aligned Unicode rules around the shortcut column. |
| `chevron` | ` g  Git` | Reversed-color key badges joined to Nerd Font Powerline tips. |

The four non-default themes align labels within each grid column. They use the widest shortcut that passes the current menu's visibility conditions; hidden, excluded, and unshown bindings do not widen the shortcut column. Brackets, Rail, and Chevron pad shorter shortcuts with spaces. Dots fills that space with leaders, with at least three dots and a space at each end to keep punctuation shortcuts identifiable.

Each theme reserves two columns before the shortcut for a state marker, so disabled and blocked rows keep the same alignment as enabled rows. Shortcut widths use terminal display columns, including wide Unicode characters. Grid packing and clipping still follow the layout settings. A viewport narrower than a shortcut and its decoration clips the row.

Custom cell templates can use `key_width` for the shared shortcut width and `key_padding` for the difference between that width and the current key's display width. For example, `{{ key | rpad(key_width) }}` pads a shortcut without cutting it. The renderer never supplies a `key_width` smaller than the current key.

Non-default themes mark disabled bindings with `- ` and blocked bindings with `! `. Status lines identify their state with `Error:`, `Pending:`, `Blocked:`, `Reload:`, or `Notice:`. Enabled keys remain bold while ordinary labels use regular weight. The final breadcrumb identifies the current location. Muxe has no persistent selected-row highlight; Chevron's badge decorates the shortcut rather than indicating focus.

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

Each scheme retains the complete chosen upstream named palette, including entries unused by Muxe. Nested names are flattened without dropping their parent namespace. Semantic mappings are written explicitly in each asset's `colors` section:

| Semantic role | Assignment |
| --- | --- |
| `base.text` / `base.background` | The upstream primary foreground and selected surface. |
| `base.muted` / `menu.separator` | Subdued secondary colors, distinct from primary text. These roles may share a color. |
| `menu.hotkey` | An accent that distinguishes shortcuts from labels and separators. |
| `status.error` | Red or rose. |
| `status.pending` | Yellow or gold. |
| `status.blocked` | Orange, ochre, or gold. It may share the pending color when the palette has no suitable orange. |
| `status.reload` | Green. |
| `status.notice` | Blue or cyan. |

Palette names and semantic names share the existing lookup namespace; a semantic name takes precedence. Palette values are literal `#rgb` or `#rrggbb` values. Semantic mappings may refer to palette entries or other semantic names. Unknown aliases and cycles are rejected, including unused entries. `inherit` is reserved for the embedded default scheme; in configuration style fields it remains an ordinary alias name.

Assignments preserve the palette's hierarchy and status hues. A lower-contrast accent is not silently replaced with primary text. The catalog records each foreground's sRGB contrast against its background, including Chevron's reversed badge, and flags ratios below the 4.5:1 normal-text reference. This reference is not a universal requirement or an accessibility guarantee: some canonical light-theme accents and secondary colors fall below it. Status prefixes and state markers preserve meaning without color. Terminal font rendering, dimming, and inherited terminal colors are not covered by these measurements.

One Dark and One Light retain all 51 color-bearing variables from their official Less files, including aliases and six alpha roles. Muxe's assets are RGB-only, so alpha roles are composited over that variant's original `syntax-bg`. Metadata preserves the raw upstream expression, reference-compiled RGBA value, compositing background, formula, and resulting RGB. These composites are source-backed adaptations, not unchanged upstream hex literals. Non-color configuration parameters and `NONE` sentinels are retained as metadata rather than treated as RGB swatches.

## Palette sources and licenses

The asset metadata at [`crates/muxe-core/assets/catalog.json`](../crates/muxe-core/assets/catalog.json) records each scheme's immutable canonical source, complete palette, semantic mappings, upstream license, and comparisons with [iTerm2 Color Schemes](https://github.com/mbadolato/iTerm2-Color-Schemes) and [Tinted Theming schemes](https://github.com/tinted-theming/schemes). The adjacent license notices preserve the upstream terms.

Opaque upstream RGB swatches are not changed to make independent ports agree. Comparisons list every alternative RGB entry and its matching canonical palette names, then separately compare declared default-background and default-text roles. Value membership is not a claim of identical roles. Different variants, values, and missing alternatives remain explicit. An ANSI terminal color, a Base16 syntax role, and an editor diagnostic color are not interchangeable merely because they have a similar hue. Refer to the metadata for the exact source and role decision rather than treating cross-port agreement as a vote.
