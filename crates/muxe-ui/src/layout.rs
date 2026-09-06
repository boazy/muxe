use unicode_width::{UnicodeWidthChar, UnicodeWidthStr};

/// A visible menu cell borrowed from a broker-supplied view.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Cell<'a> {
    pub key: &'a str,
    pub title: &'a str,
    pub disabled: bool,
    pub blocked: bool,
}

/// The grid area available after title, breadcrumb, status, and pager rows are reserved.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct GridRect {
    pub width: u16,
    pub rows: u16,
}

/// One cell's deterministic page, column, and row location.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct GridSlot {
    pub source_index: usize,
    pub page: usize,
    pub column: u16,
    pub row: u16,
}

/// A deterministic, column-major plan for visible menu cells.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GridPlan {
    pub columns: u16,
    pub rows_per_page: u16,
    pub page_count: usize,
    pub slots: Vec<GridSlot>,
}

/// Removes Unicode controls and cuts a label at its first line boundary.
pub fn sanitize_single_line(value: &str) -> String {
    value
        .chars()
        .take_while(|character| !matches!(character, '\n' | '\r'))
        .filter(|character| !character.is_control())
        .collect()
}

/// Arranges cells in the largest fitting number of columns.
///
/// `max_title_width` applies after labels are sanitized. Keys remain intact even when the grid
/// falls back to one column that is wider than `area.width`.
pub fn arrange_cells(
    cells: &[Cell<'_>],
    area: GridRect,
    between_columns: u16,
    max_title_width: usize,
) -> GridPlan {
    if cells.is_empty() || area.rows == 0 {
        return GridPlan {
            columns: 0,
            rows_per_page: area.rows,
            page_count: 0,
            slots: Vec::new(),
        };
    }

    let cell_width = cells
        .iter()
        .map(|cell| display_width(cell.key) + title_width(cell.title, max_title_width))
        .max()
        .expect("non-empty cells");
    let columns = fitting_columns(
        cells.len(),
        cell_width,
        area.width as usize,
        between_columns,
    );
    let page_capacity = columns as usize * area.rows as usize;
    let page_count = cells.len().div_ceil(page_capacity);
    let mut slots = Vec::with_capacity(cells.len());

    for source_index in 0..cells.len() {
        let within_page = source_index % page_capacity;
        slots.push(GridSlot {
            source_index,
            page: source_index / page_capacity,
            column: (within_page / area.rows as usize) as u16,
            row: (within_page % area.rows as usize) as u16,
        });
    }

    GridPlan {
        columns,
        rows_per_page: area.rows,
        page_count,
        slots,
    }
}

fn fitting_columns(item_count: usize, cell_width: usize, available_width: usize, gap: u16) -> u16 {
    for columns in (1..=item_count.min(u16::MAX as usize)).rev() {
        let required_width = columns
            .saturating_mul(cell_width)
            .saturating_add((columns - 1).saturating_mul(gap as usize));
        if required_width <= available_width {
            return columns as u16;
        }
    }
    1
}

fn title_width(title: &str, max_width: usize) -> usize {
    let title = sanitize_single_line(title);
    if title.is_empty() {
        0
    } else {
        1 + truncated_width(&title, max_width)
    }
}

fn display_width(value: &str) -> usize {
    UnicodeWidthStr::width(value)
}

fn truncated_width(value: &str, max_width: usize) -> usize {
    if display_width(value) <= max_width {
        return display_width(value);
    }
    if max_width == 0 {
        return 0;
    }
    if max_width == 1 {
        return 1;
    }

    let mut width = 0;
    for character in value.chars() {
        let character_width = UnicodeWidthChar::width(character).unwrap_or(0);
        if width + character_width > max_width - 1 {
            break;
        }
        width += character_width;
    }
    width + 1
}

#[cfg(test)]
mod tests {
    use proptest::prelude::*;

    use super::*;

    #[test]
    fn labels_are_single_line_and_control_free_before_measurement() {
        assert_eq!(
            sanitize_single_line("open\u{0007} now\nignored"),
            "open now"
        );
    }

    #[test]
    fn layout_fills_pages_in_column_major_order() {
        let cells = [
            Cell {
                key: "a",
                title: "one",
                disabled: false,
                blocked: false,
            },
            Cell {
                key: "b",
                title: "two",
                disabled: false,
                blocked: false,
            },
            Cell {
                key: "c",
                title: "three",
                disabled: false,
                blocked: false,
            },
            Cell {
                key: "d",
                title: "four",
                disabled: false,
                blocked: false,
            },
            Cell {
                key: "e",
                title: "five",
                disabled: false,
                blocked: false,
            },
        ];
        let plan = arrange_cells(&cells, GridRect { width: 40, rows: 2 }, 2, 24);

        assert_eq!(plan.columns, 4);
        assert_eq!(plan.page_count, 1);
        assert_eq!(
            plan.slots,
            vec![
                GridSlot {
                    source_index: 0,
                    page: 0,
                    column: 0,
                    row: 0
                },
                GridSlot {
                    source_index: 1,
                    page: 0,
                    column: 0,
                    row: 1
                },
                GridSlot {
                    source_index: 2,
                    page: 0,
                    column: 1,
                    row: 0
                },
                GridSlot {
                    source_index: 3,
                    page: 0,
                    column: 1,
                    row: 1
                },
                GridSlot {
                    source_index: 4,
                    page: 0,
                    column: 2,
                    row: 0
                },
            ]
        );
    }

    #[test]
    fn narrow_grids_keep_keys_and_paginate() {
        let cells = [
            Cell {
                key: "ctrl+shift+very-long-key",
                title: "a very long title",
                disabled: false,
                blocked: false,
            },
            Cell {
                key: "b",
                title: "two",
                disabled: false,
                blocked: false,
            },
            Cell {
                key: "c",
                title: "three",
                disabled: false,
                blocked: false,
            },
        ];
        let plan = arrange_cells(&cells, GridRect { width: 4, rows: 1 }, 3, 4);

        assert_eq!(plan.columns, 1);
        assert_eq!(plan.page_count, 3);
        assert_eq!(
            plan.slots[2],
            GridSlot {
                source_index: 2,
                page: 2,
                column: 0,
                row: 0
            }
        );
    }

    proptest! {
        #[test]
        fn packing_is_deterministic_and_places_each_cell_once(
            count in 1usize..128,
            width in 0u16..160,
            rows in 1u16..16,
            gap in 0u16..8,
        ) {
            let cells = vec![
                Cell { key: "ctrl+shift+f12", title: "a title", disabled: false, blocked: false };
                count
            ];
            let area = GridRect { width, rows };
            let first = arrange_cells(&cells, area, gap, 24);
            let second = arrange_cells(&cells, area, gap, 24);

            prop_assert_eq!(&first, &second);
            prop_assert_eq!(first.slots.len(), count);
            for (source_index, slot) in first.slots.iter().enumerate() {
                prop_assert_eq!(slot.source_index, source_index);
                prop_assert!(slot.column < first.columns);
                prop_assert!(slot.row < rows);
                prop_assert!(slot.page < first.page_count);
            }
        }
    }
}
