use ratatui::{buffer::Buffer, layout::Rect, widgets::Widget};

use crate::layout::GridPlan;

/// One rendered menu cell borrowed from template output.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct MenuGridCell<'a> {
    pub text: &'a str,
}

/// The single status-line value selected by the UI's status precedence rules.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct MenuStatus<'a> {
    pub text: &'a str,
}

/// Renders one page of a precomputed menu grid and an optional status line.
pub struct MenuGrid<'a> {
    pub plan: &'a GridPlan,
    pub cells: &'a [MenuGridCell<'a>],
    pub page: usize,
    pub status: Option<MenuStatus<'a>>,
}

impl Widget for MenuGrid<'_> {
    fn render(self, area: Rect, buffer: &mut Buffer) {
        let columns = self.plan.columns.max(1);
        let column_width = area.width / columns;
        if column_width == 0 {
            return;
        }

        let status_row = self.status.map(|_| area.bottom().saturating_sub(1));
        for slot in self.plan.slots.iter().filter(|slot| slot.page == self.page) {
            let y = area.y.saturating_add(slot.row);
            if y >= status_row.unwrap_or(area.bottom()) {
                continue;
            }
            let x = area
                .x
                .saturating_add(slot.column.saturating_mul(column_width));
            let width = if slot.column + 1 == columns {
                area.right().saturating_sub(x)
            } else {
                column_width
            };
            if let Some(cell) = self.cells.get(slot.source_index) {
                buffer.set_stringn(
                    x,
                    y,
                    cell.text,
                    width as usize,
                    ratatui::style::Style::default(),
                );
            }
        }
        if let (Some(status), Some(y)) = (self.status, status_row) {
            buffer.set_stringn(
                area.x,
                y,
                status.text,
                area.width as usize,
                ratatui::style::Style::default(),
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use ratatui::{buffer::Buffer, layout::Rect, widgets::Widget};

    use crate::layout::{GridPlan, GridSlot};

    use super::*;

    #[test]
    fn grid_renders_column_major_cells_without_overwriting_status() {
        let plan = GridPlan {
            columns: 2,
            rows_per_page: 2,
            page_count: 1,
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
        let cells = [
            MenuGridCell { text: "a one" },
            MenuGridCell { text: "b two" },
            MenuGridCell { text: "c three" },
        ];
        let mut buffer = Buffer::empty(Rect::new(0, 0, 20, 3));
        MenuGrid {
            plan: &plan,
            cells: &cells,
            page: 0,
            status: Some(MenuStatus { text: "ready" }),
        }
        .render(Rect::new(0, 0, 20, 3), &mut buffer);

        assert_eq!(buffer[(0, 0)].symbol(), "a");
        assert_eq!(buffer[(0, 1)].symbol(), "b");
        assert_eq!(buffer[(10, 0)].symbol(), "c");
        assert_eq!(buffer[(0, 2)].symbol(), "r");
    }
}
