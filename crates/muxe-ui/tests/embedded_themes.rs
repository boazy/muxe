use muxe_core::{ColorSchemeName, ThemeAssets, ThemeName};
use muxe_ui::{
    BreadcrumbTemplate, CellTemplate, PaginationTemplate, StatusTemplate, TemplateRenderer,
};
use ratatui::style::Modifier;

#[test]
fn every_embedded_presentation_renders_every_component_with_every_scheme() {
    let catalog = ThemeAssets::default().compile_catalog();
    for name in ["brackets", "dots", "rail", "chevron"] {
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
            assert_eq!(
                renderer
                    .render_pagination_short(pagination)
                    .expect("short pager renders")
                    .plain,
                "2/3"
            );
            for (level, prefix) in [
                ("error", "Error:"),
                ("pending", "Pending:"),
                ("blocked", "Blocked:"),
                ("reload", "Reload:"),
                ("notice", "Notice:"),
            ] {
                let status = renderer
                    .render_status(StatusTemplate {
                        level,
                        message: "Message [safe]",
                    })
                    .expect("status renders");
                assert!(status.plain.starts_with(prefix));
                assert!(status.plain.ends_with("Message [safe]"));
            }
        }
    }
}

#[test]
fn bracket_and_dot_presentations_match_their_documented_shape() {
    let catalog = ThemeAssets::default().compile_catalog();
    for (name, expected) in [
        ("brackets", "[g] Git"),
        ("dots", "g ..... Git"),
        ("rail", "│ g │ Git"),
        ("chevron", " g › Git"),
    ] {
        let theme = catalog
            .resolve(&ThemeName::new(name), &ColorSchemeName::new("default"))
            .expect("terminal inheriting pair");
        let cell = TemplateRenderer::new(&theme)
            .expect("renderer")
            .render_cell(CellTemplate {
                key: "g",
                title: "Git",
                disabled: false,
                blocked: false,
                max_title_width: 40,
            })
            .expect("cell");
        assert_eq!(cell.plain, expected);
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
    for key in [".", "[", "]", "\\"] {
        for disabled in [false, true] {
            let cell = renderer
                .render_cell(CellTemplate {
                    key,
                    title: "[hotkey] label\u{1b}",
                    disabled,
                    blocked: false,
                    max_title_width: 40,
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
            assert_eq!(
                tokens[offset + 1],
                ".....",
                "leaders remain separate from the shortcut"
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
            })
            .expect("default cell");
        assert_eq!(cell.plain, format!("{key} → Label"));
        assert!(
            cell.spans
                .iter()
                .any(|span| span.text == key && span.style.add_modifier.contains(Modifier::BOLD)),
            "the complete six-cell shortcut stays in its hotkey style"
        );
    }
}
