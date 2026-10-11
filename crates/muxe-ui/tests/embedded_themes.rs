use muxe_core::{ColorSchemeName, ConfigDocument, SourceId, ThemeAssets, ThemeName};
use muxe_ui::{
    BreadcrumbTemplate, CellTemplate, PaginationTemplate, StatusTemplate, TemplateRenderer,
};
use ratatui::style::Modifier;
use unicode_width::UnicodeWidthStr;

#[test]
fn embedded_presentations_preserve_literal_content_and_state_cues_across_schemes() {
    let catalog = ThemeAssets::default().compile_catalog();
    for name in ["default", "brackets", "dots", "rail", "chevron"] {
        for scheme in catalog.color_scheme_names() {
            let theme = catalog
                .resolve(&ThemeName::new(name), scheme)
                .expect("valid pair");
            let renderer = TemplateRenderer::new(&theme).expect("templates compile");
            for (disabled, blocked) in [(false, false), (true, false), (true, true)] {
                let cell = renderer
                    .render_cell(CellTemplate {
                        key: "Ctrl-Shift-[Left]",
                        title: "日本語 [Git]",
                        disabled,
                        blocked,
                        max_title_width: 80,
                        key_width: 20,
                    })
                    .expect("cell renders");
                assert!(
                    cell.plain.contains("Ctrl-Shift-[Left]"),
                    "{name} preserves complete keys"
                );
                assert!(cell.plain.contains("日本語 [Git]"));
                assert!(!cell.plain.contains('\\'), "markup escapes are not visible");
                if blocked {
                    assert!(cell.plain.starts_with("! "));
                } else if disabled {
                    assert!(cell.plain.starts_with("- "));
                } else {
                    assert!(
                        cell.spans
                            .iter()
                            .any(|span| span.style.add_modifier.contains(Modifier::BOLD))
                    );
                }
            }
            let crumbs = renderer
                .render_breadcrumbs(BreadcrumbTemplate {
                    crumbs: &["Root", "日本語"],
                })
                .expect("breadcrumbs render");
            assert!(crumbs.plain.ends_with("日本語"));
            let pagination = PaginationTemplate {
                current: 2,
                count: 3,
                prev_key: "Ctrl-Left",
                next_key: "Ctrl-Right",
                prev_keys: &["Ctrl-Left", "PageUp"],
                next_keys: &["Ctrl-Right", "PageDown"],
            };
            let full = renderer
                .render_pagination_full(pagination)
                .expect("full pager renders");
            assert!(full.plain.contains("Ctrl-Left") && full.plain.contains("Ctrl-Right"));
            for level in ["error", "pending", "blocked", "reload", "notice"] {
                let status = renderer
                    .render_status(StatusTemplate {
                        level,
                        message: "Message [safe]",
                    })
                    .expect("status renders");
                assert!(status.plain.ends_with("Message [safe]"));
            }
        }
    }
}

#[test]
fn presentations_align_labels_and_separators_across_key_widths_and_states() {
    let catalog = ThemeAssets::default().compile_catalog();
    let keys = ["g", "界", "e\u{301}", "👩‍💻", "ctrl+\\", "Ctrl-Shift-[Left]"];
    let key_width = keys
        .iter()
        .map(|key| UnicodeWidthStr::width(*key))
        .max()
        .unwrap();
    for name in ["brackets", "dots", "rail", "chevron"] {
        let theme = catalog
            .resolve(
                &ThemeName::new(name),
                &ColorSchemeName::new("catppuccin-latte"),
            )
            .expect("pair");
        let renderer = TemplateRenderer::new(&theme).expect("renderer");
        let mut label_column = None;
        let mut separator_column = None;
        for key in keys {
            for (disabled, blocked, marker) in [
                (false, false, "  "),
                (true, false, "- "),
                (true, true, "! "),
            ] {
                let cell = renderer
                    .render_cell(CellTemplate {
                        key,
                        title: "Label",
                        disabled,
                        blocked,
                        max_title_width: 40,
                        key_width,
                    })
                    .expect("cell");
                assert!(
                    cell.plain.starts_with(marker),
                    "{name}: reserved marker gutter"
                );
                assert!(
                    cell.plain.contains(key),
                    "{name}: complete key is preserved"
                );
                let label = cell.plain.find("Label").expect("label");
                let column = UnicodeWidthStr::width(&cell.plain[..label]);
                assert_eq!(
                    *label_column.get_or_insert(column),
                    column,
                    "{name}: labels align"
                );
                let separator = match name {
                    "brackets" => cell.plain.rfind(']').unwrap(),
                    "dots" => cell.plain.rfind('.').unwrap(),
                    "rail" => cell.plain.rfind('│').unwrap(),
                    "chevron" => cell.plain.rfind('\u{e0b0}').unwrap(),
                    _ => unreachable!(),
                };
                let column = UnicodeWidthStr::width(&cell.plain[..separator]);
                assert_eq!(
                    *separator_column.get_or_insert(column),
                    column,
                    "{name}: separators align"
                );
                if !disabled {
                    assert!(cell.spans.iter().any(|span| span.text.contains(key)
                        && span.style.add_modifier.contains(Modifier::BOLD)));
                }
                assert!(
                    !cell
                        .spans
                        .iter()
                        .find(|span| span.text.contains("Label"))
                        .unwrap()
                        .style
                        .add_modifier
                        .contains(Modifier::BOLD),
                    "labels have normal weight"
                );
            }
        }
    }
}

#[test]
fn chevron_badge_edge_connects_to_enabled_and_disabled_backgrounds() {
    let catalog = ThemeAssets::default().compile_catalog();
    for scheme in catalog.color_scheme_names() {
        let theme = catalog
            .resolve(&ThemeName::new("chevron"), scheme)
            .expect("pair");
        let renderer = TemplateRenderer::new(&theme).expect("renderer");
        for (disabled, blocked) in [(false, false), (true, false), (true, true)] {
            let cell = renderer
                .render_cell(CellTemplate {
                    key: "ctrl+g",
                    title: "Label",
                    disabled,
                    blocked,
                    max_title_width: 40,
                    key_width: 8,
                })
                .expect("badge");
            let edge = cell
                .spans
                .iter()
                .find(|span| span.text.contains('\u{e0b0}'))
                .expect("Powerline edge");
            let badge = cell
                .spans
                .iter()
                .find(|span| span.text.contains("ctrl+g"))
                .expect("shortcut badge");
            let label = cell
                .spans
                .iter()
                .find(|span| span.text.contains("Label"))
                .unwrap();
            assert_eq!(edge.style.fg, badge.style.bg, "edge continues the badge");
            assert_eq!(edge.style.bg, label.style.bg, "edge joins the surface");
            assert_eq!(
                badge.style.fg, label.style.bg,
                "badge uses inverse foreground"
            );
        }
    }
}

#[test]
fn undersized_alignment_hints_preserve_complete_literal_and_unicode_keys() {
    let catalog = ThemeAssets::default().compile_catalog();
    for name in ["brackets", "dots", "rail", "chevron"] {
        let theme = catalog
            .resolve(&ThemeName::new(name), &ColorSchemeName::new("default"))
            .expect("pair");
        let renderer = TemplateRenderer::new(&theme).expect("renderer");
        for key in [
            "ctrl+[",
            "ctrl+]",
            "ctrl+\\",
            "ctrl+shift+left",
            "👩‍💻",
            "e\u{301}",
        ] {
            let cell = renderer
                .render_cell(CellTemplate {
                    key,
                    title: "Label",
                    disabled: false,
                    blocked: false,
                    max_title_width: 40,
                    key_width: 1,
                })
                .expect("cell");
            assert!(
                cell.plain.contains(key),
                "{name}: width hints cannot truncate {key}"
            );
            assert!(cell.plain.ends_with("Label"));
        }
    }
}

#[test]
fn dotted_leaders_keep_punctuation_keys_identifiable_in_a_wide_viewport() {
    use muxe_ui::{Cell, GridRect, MenuGrid, arrange_cells};
    use ratatui::{buffer::Buffer, layout::Rect, widgets::Widget};

    let theme = ThemeAssets::default()
        .compile_catalog()
        .resolve(&ThemeName::new("dots"), &ColorSchemeName::new("default"))
        .expect("pair");
    let renderer = TemplateRenderer::new(&theme).expect("renderer");
    for key in [".", "[", "]", "\\", "ctrl+\\", "界"] {
        for disabled in [false, true] {
            let cell = renderer
                .render_cell(CellTemplate {
                    key,
                    title: "[hotkey] label\u{1b}",
                    disabled,
                    blocked: false,
                    max_title_width: 40,
                    key_width: 8,
                })
                .expect("punctuation renders as data");
            assert!(cell.plain.chars().all(|character| !character.is_control()));
            let plan = arrange_cells(
                &[Cell { text: &cell.plain }],
                GridRect { width: 60, rows: 1 },
                0,
                0,
            );
            let area = Rect::new(0, 0, 60, 1);
            let mut buffer = Buffer::empty(area);
            MenuGrid {
                plan: &plan,
                cells: &[cell],
                page: 0,
                pager: None,
                status: None,
            }
            .render(area, &mut buffer);
            let visible: String = buffer
                .content
                .iter()
                .flat_map(|cell| cell.symbol().chars())
                .collect();
            let tokens: Vec<_> = visible.split_whitespace().collect();
            let offset = usize::from(disabled);
            assert_eq!(
                tokens[offset], key,
                "shortcut remains an identifiable token"
            );
            assert!(tokens[offset + 1].chars().all(|character| character == '.'));
            assert_eq!(
                UnicodeWidthStr::width(tokens[offset + 1]),
                8 - UnicodeWidthStr::width(key) + 3,
                "leaders fill the remaining key width plus a three-cell minimum"
            );
            assert_eq!(
                tokens[offset + 2],
                "[hotkey]",
                "label markup is rendered literally"
            );
            assert_eq!(tokens[offset + 3], "label", "label controls are removed");
        }
    }
}

#[test]
fn default_padding_preserves_literal_escape_tokens_at_the_visible_key_boundary() {
    let theme = ThemeAssets::default()
        .compile_catalog()
        .resolve(&ThemeName::new("default"), &ColorSchemeName::new("default"))
        .expect("default pair");
    let renderer = TemplateRenderer::new(&theme).expect("renderer");
    for key in ["ctrl+\\", "ctrl+[", "ctrl+]"] {
        let cell = renderer
            .render_cell(CellTemplate {
                key,
                title: "Label",
                disabled: false,
                blocked: false,
                max_title_width: 40,
                key_width: 0,
            })
            .expect("default cell");
        assert!(
            cell.spans
                .iter()
                .any(|span| span.text == key && span.style.add_modifier.contains(Modifier::BOLD)),
            "the complete six-cell shortcut stays in its hotkey style"
        );
    }
}

fn semantic_renderer(presentation: &str) -> TemplateRenderer {
    let mut assets = ThemeAssets::default();
    let scheme = ColorSchemeName::new("role-regression");
    assets.color_schemes.insert(
        scheme.clone(),
        ConfigDocument::parse(
            SourceId::new("role-regression.yml"),
            "title: Role regression\ncolors:\n  base.text: '#eeeeee'\n  base.background: '#101010'\n  base.muted: '#aaaaaa'\n  menu.hotkey: '#8866dd'\n  menu.separator: '#777777'\n  status.error: '#ee3344'\n  status.pending: '#eecc33'\n  status.blocked: '#ee8833'\n  status.reload: '#33cc66'\n  status.notice: '#33aadd'\n",
        )
        .expect("role fixture parses"),
    );
    let theme = assets
        .compile_catalog()
        .resolve(&ThemeName::new(presentation), &scheme)
        .expect("role fixture compiles");
    TemplateRenderer::new(&theme).expect("renderer")
}

#[test]
fn breadcrumbs_separate_ancestor_current_location_and_structural_glyphs() {
    use ratatui::style::Color;

    for presentation in ["default", "brackets", "dots", "rail", "chevron"] {
        let renderer = semantic_renderer(presentation);
        let crumbs = renderer
            .render_breadcrumbs(BreadcrumbTemplate {
                crumbs: &["Root", "Parent", "Current"],
            })
            .expect("breadcrumbs");
        for ancestor in ["Root", "Parent"] {
            let span = crumbs
                .spans
                .iter()
                .find(|span| span.text == ancestor)
                .unwrap();
            assert_eq!(span.style.fg, Some(Color::Rgb(170, 170, 170)));
            assert!(!span.style.add_modifier.contains(Modifier::DIM));
        }
        let current = crumbs
            .spans
            .iter()
            .find(|span| span.text == "Current")
            .unwrap();
        assert_eq!(current.style.fg, Some(Color::Rgb(238, 238, 238)));
        assert!(current.style.add_modifier.contains(Modifier::BOLD));
        let separators: Vec<_> = crumbs
            .spans
            .iter()
            .filter(|span| !["Root", "Parent", "Current"].contains(&span.text.as_str()))
            .collect();
        assert_eq!(separators.len(), 2, "one structural separator per ancestor");
        for separator in separators {
            assert_eq!(separator.style.fg, Some(Color::Rgb(119, 119, 119)));
        }
    }
}

#[test]
fn default_rows_keep_state_markers_aligned_without_dimming_secondary_text() {
    use muxe_ui::{Cell, GridRect, MenuGrid, arrange_cells};
    use ratatui::{buffer::Buffer, layout::Rect, style::Color, widgets::Widget};

    let renderer = semantic_renderer("default");
    let rows: Vec<_> = [(false, false), (true, false), (true, true), (false, true)]
        .into_iter()
        .map(|(disabled, blocked)| {
            renderer
                .render_cell(CellTemplate {
                    key: "g",
                    title: "Label",
                    disabled,
                    blocked,
                    max_title_width: 40,
                    key_width: 6,
                })
                .expect("row")
        })
        .collect();
    let cells: Vec<_> = rows.iter().map(|row| Cell { text: &row.plain }).collect();
    let plan = arrange_cells(&cells, GridRect { width: 40, rows: 4 }, 0, 0);
    let area = Rect::new(0, 0, 40, 4);
    let mut buffer = Buffer::empty(area);
    MenuGrid {
        plan: &plan,
        cells: &rows,
        page: 0,
        pager: None,
        status: None,
    }
    .render(area, &mut buffer);
    for (row, marker) in [(0, " "), (1, "-"), (2, "!"), (3, "!")] {
        assert_eq!(buffer[(0, row)].symbol(), marker);
        assert_eq!(
            buffer[(2, row)].symbol(),
            "g",
            "state does not shift the key"
        );
        assert_eq!(
            buffer[(11, row)].symbol(),
            "L",
            "state does not shift the label"
        );
    }
    assert_eq!(buffer[(0, 2)].fg, Color::Rgb(238, 136, 51));
    assert_eq!(buffer[(0, 3)].fg, Color::Rgb(238, 136, 51));
    for row in 0..4 {
        assert_eq!(buffer[(9, row)].fg, Color::Rgb(119, 119, 119));
    }
    for row in [1, 2] {
        for column in [2, 11] {
            let cell = &buffer[(column, row)];
            assert_eq!(cell.fg, Color::Rgb(170, 170, 170));
            assert!(!cell.modifier.contains(Modifier::DIM));
        }
    }
}

#[test]
fn disabled_and_blocked_rows_keep_structure_distinct_from_secondary_content() {
    use muxe_ui::{Cell, GridRect, MenuGrid, arrange_cells};
    use ratatui::{buffer::Buffer, layout::Rect, style::Color, widgets::Widget};

    for (presentation, key_column, label_column, punctuation) in [
        ("default", 2, 11, vec![9]),
        ("brackets", 3, 11, vec![2, 9]),
        ("dots", 2, 13, (4..12).collect()),
        ("rail", 4, 13, vec![2, 11]),
    ] {
        let renderer = semantic_renderer(presentation);
        let rows: Vec<_> = [false, true]
            .into_iter()
            .map(|blocked| {
                renderer
                    .render_cell(CellTemplate {
                        key: "g",
                        title: "Label",
                        disabled: true,
                        blocked,
                        max_title_width: 40,
                        key_width: 6,
                    })
                    .expect("disabled state")
            })
            .collect();
        let cells: Vec<_> = rows.iter().map(|row| Cell { text: &row.plain }).collect();
        let plan = arrange_cells(&cells, GridRect { width: 40, rows: 2 }, 0, 0);
        let area = Rect::new(0, 0, 40, 2);
        let mut buffer = Buffer::empty(area);
        MenuGrid {
            plan: &plan,
            cells: &rows,
            page: 0,
            pager: None,
            status: None,
        }
        .render(area, &mut buffer);
        for row in 0..2 {
            assert_eq!(buffer[(key_column, row)].symbol(), "g");
            assert_eq!(buffer[(label_column, row)].symbol(), "L");
            for column in [key_column, label_column] {
                assert_eq!(buffer[(column, row)].fg, Color::Rgb(170, 170, 170));
                assert!(!buffer[(column, row)].modifier.contains(Modifier::DIM));
            }
            for &column in &punctuation {
                let mark = &buffer[(column, row)];
                assert_ne!(
                    mark.symbol(),
                    " ",
                    "{presentation}: structural glyph is visible"
                );
                assert_eq!(
                    mark.fg,
                    Color::Rgb(119, 119, 119),
                    "{presentation}: punctuation retains separator"
                );
                assert_ne!(mark.fg, buffer[(label_column, row)].fg);
            }
        }
        assert_eq!(buffer[(0, 0)].symbol(), "-");
        assert_eq!(buffer[(0, 1)].symbol(), "!");
        assert_eq!(buffer[(0, 1)].fg, Color::Rgb(238, 136, 51));
    }
}

#[test]
fn every_presentation_renders_distinct_status_roles() {
    use ratatui::style::Color;

    for presentation in ["default", "brackets", "dots", "rail", "chevron"] {
        let renderer = semantic_renderer(presentation);
        for (level, color) in [
            ("error", Color::Rgb(238, 51, 68)),
            ("pending", Color::Rgb(238, 204, 51)),
            ("blocked", Color::Rgb(238, 136, 51)),
            ("reload", Color::Rgb(51, 204, 102)),
            ("notice", Color::Rgb(51, 170, 221)),
        ] {
            let status = renderer
                .render_status(StatusTemplate {
                    level,
                    message: "Diagnostic",
                })
                .expect("status");
            let message = status
                .spans
                .iter()
                .find(|span| span.text.contains("Diagnostic"))
                .unwrap();
            assert_eq!(message.style.fg, Some(color), "{presentation}: {level}");
        }
    }
}
