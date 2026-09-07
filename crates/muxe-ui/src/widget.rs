use ratatui::{buffer::Buffer, layout::Rect, widgets::Widget};
use unicode_width::UnicodeWidthStr;

use crate::{RenderedText, layout::GridPlan};

/// The single status-line value selected by the UI's status precedence rules.
#[derive(Clone, Copy, Debug)]
pub struct MenuStatus<'a> {
    pub rendered: &'a RenderedText,
}

/// Renders one page of a precomputed menu grid, pager, and optional status line.
pub struct MenuGrid<'a> {
    pub plan: &'a GridPlan,
    pub cells: &'a [RenderedText],
    pub page: usize,
    pub pager: Option<&'a RenderedText>,
    pub status: Option<MenuStatus<'a>>,
}

impl Widget for MenuGrid<'_> {
    fn render(self, area: Rect, buffer: &mut Buffer) {
        let Some(page) = self.plan.page(self.page) else {
            return;
        };
        let pager_row = self
            .plan
            .has_pager
            .then_some(area.y.saturating_add(self.plan.pager_row_offset()));
        let status_row = self.status.map(|_| area.bottom().saturating_sub(1));
        for slot in self.plan.slots.iter().filter(|slot| slot.page == self.page) {
            let Some(column) = page.columns.get(slot.column as usize) else {
                continue;
            };
            let y = area.y.saturating_add(self.plan.cell_row_offset(slot.row));
            if y >= pager_row.unwrap_or(status_row.unwrap_or(area.bottom()))
                || y >= status_row.unwrap_or(area.bottom())
            {
                continue;
            }
            let x = area.x.saturating_add(column.offset);
            if x >= area.right() {
                continue;
            }
            let width = column.width.min(area.right().saturating_sub(x));
            if let Some(cell) = self.cells.get(slot.source_index) {
                write_rendered(buffer, x, y, width, cell);
            }
        }
        if let (Some(pager), Some(y)) = (self.pager, pager_row) {
            write_rendered(buffer, area.x, y, area.width, pager);
        }
        if let (Some(status), Some(y)) = (self.status, status_row) {
            write_rendered(buffer, area.x, y, area.width, status.rendered);
        }
    }
}

pub(crate) fn write_rendered(
    buffer: &mut Buffer,
    mut x: u16,
    y: u16,
    width: u16,
    rendered: &RenderedText,
) {
    let right = x.saturating_add(width);
    for span in &rendered.spans {
        if x >= right {
            break;
        }
        let remaining = right.saturating_sub(x);
        buffer.set_stringn(x, y, &span.text, remaining as usize, span.style);
        x = x.saturating_add(
            u16::try_from(UnicodeWidthStr::width(span.text.as_str())).unwrap_or(u16::MAX),
        );
    }
}

#[cfg(test)]
mod tests {
    use ratatui::{buffer::Buffer, layout::Rect, style::Modifier, widgets::Widget};

    use crate::{
        RenderedSpan,
        layout::{GridColumn, GridPage, GridSlot},
    };

    use super::*;

    #[test]
    fn grid_uses_packed_offsets_resolved_styles_and_status() {
        let plan = GridPlan {
            columns: 2,
            row_gap: 0,
            rows_per_page: 2,
            page_count: 1,
            has_pager: false,
            pages: vec![GridPage {
                columns: vec![
                    GridColumn {
                        offset: 0,
                        width: 7,
                    },
                    GridColumn {
                        offset: 10,
                        width: 9,
                    },
                ],
            }],
            slots: vec![
                GridSlot {
                    source_index: 0,
                    page: 0,
                    column: 0,
                    row: 0,
                },
                GridSlot {
                    source_index: 1,
                    page: 0,
                    column: 0,
                    row: 1,
                },
                GridSlot {
                    source_index: 2,
                    page: 0,
                    column: 1,
                    row: 0,
                },
            ],
        };
        let bold = ratatui::style::Style::default().add_modifier(Modifier::BOLD);
        let rendered = [
            RenderedText {
                plain: "a one".into(),
                spans: vec![RenderedSpan {
                    text: "a one".into(),
                    style: bold,
                }],
            },
            RenderedText {
                plain: "b two".into(),
                spans: vec![RenderedSpan {
                    text: "b two".into(),
                    style: ratatui::style::Style::default(),
                }],
            },
            RenderedText {
                plain: "c three".into(),
                spans: vec![RenderedSpan {
                    text: "c three".into(),
                    style: ratatui::style::Style::default(),
                }],
            },
        ];
        let cells = rendered;
        let status = RenderedText {
            plain: "ready".into(),
            spans: vec![RenderedSpan {
                text: "ready".into(),
                style: ratatui::style::Style::default(),
            }],
        };
        let mut buffer = Buffer::empty(Rect::new(0, 0, 20, 3));
        MenuGrid {
            plan: &plan,
            cells: &cells,
            page: 0,
            pager: None,
            status: Some(MenuStatus { rendered: &status }),
        }
        .render(Rect::new(0, 0, 20, 3), &mut buffer);

        assert_eq!(buffer[(0, 0)].symbol(), "a");
        assert!(buffer[(0, 0)].modifier.contains(Modifier::BOLD));
        assert_eq!(buffer[(0, 1)].symbol(), "b");
        assert_eq!(buffer[(10, 0)].symbol(), "c");
        assert_eq!(buffer[(0, 2)].symbol(), "r");
    }

    #[test]
    fn grid_uses_row_gaps_and_the_reserved_pager_row() {
        let plan = GridPlan {
            columns: 1,
            row_gap: 1,
            rows_per_page: 2,
            page_count: 2,
            has_pager: true,
            pages: vec![
                GridPage {
                    columns: vec![GridColumn {
                        offset: 0,
                        width: 8,
                    }],
                },
                GridPage {
                    columns: vec![GridColumn {
                        offset: 0,
                        width: 8,
                    }],
                },
            ],
            slots: vec![
                GridSlot {
                    source_index: 0,
                    page: 0,
                    column: 0,
                    row: 0,
                },
                GridSlot {
                    source_index: 1,
                    page: 0,
                    column: 0,
                    row: 1,
                },
            ],
        };
        let cells = [
            RenderedText {
                plain: "a".into(),
                spans: vec![RenderedSpan {
                    text: "a".into(),
                    style: ratatui::style::Style::default(),
                }],
            },
            RenderedText {
                plain: "b".into(),
                spans: vec![RenderedSpan {
                    text: "b".into(),
                    style: ratatui::style::Style::default(),
                }],
            },
        ];
        let pager = RenderedText {
            plain: "page".into(),
            spans: vec![RenderedSpan {
                text: "page".into(),
                style: ratatui::style::Style::default(),
            }],
        };
        let mut buffer = Buffer::empty(Rect::new(0, 0, 8, 5));
        MenuGrid {
            plan: &plan,
            cells: &cells,
            page: 0,
            pager: Some(&pager),
            status: None,
        }
        .render(Rect::new(0, 0, 8, 5), &mut buffer);

        assert_eq!(buffer[(0, 0)].symbol(), "a");
        assert_eq!(buffer[(0, 1)].symbol(), " ");
        assert_eq!(buffer[(0, 2)].symbol(), "b");
        assert_eq!(buffer[(0, 3)].symbol(), "p");
    }
}
