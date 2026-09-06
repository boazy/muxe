use unicode_width::{UnicodeWidthChar, UnicodeWidthStr};

/// A rendered visible cell that can be measured without inspecting configuration fields.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Cell<'a> {
    /// Text after template expansion and style-markup removal.
    pub text: &'a str,
}

/// Physical grid lines available after title, status, and vertical padding are reserved.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct GridRect {
    pub width: u16,
    pub rows: u16,
}

/// Exact geometry for one rendered column.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct GridColumn {
    pub offset: u16,
    pub width: u16,
}

/// One cell's deterministic page, column, and logical row location.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct GridSlot {
    pub source_index: usize,
    pub page: usize,
    pub column: u16,
    pub row: u16,
}

/// Exact geometry for one page of a column-major grid.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GridPage {
    pub columns: Vec<GridColumn>,
}

/// A deterministic, column-major plan for rendered menu cells.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GridPlan {
    /// The selected maximum column count. The final page can use fewer columns.
    pub columns: u16,
    /// Physical blank lines between adjacent cell rows.
    pub row_gap: u16,
    /// Number of cell rows that fit on each page.
    pub rows_per_page: u16,
    /// Empty menus still have one logical page so `pages.current` and `pages.count` stay 1-based.
    pub page_count: usize,
    /// Whether a pager row is reserved below the grid.
    pub has_pager: bool,
    pub pages: Vec<GridPage>,
    pub slots: Vec<GridSlot>,
}

impl GridPlan {
    pub fn page(&self, page: usize) -> Option<&GridPage> {
        self.pages.get(page)
    }

    pub const fn cell_row_offset(&self, row: u16) -> u16 {
        row.saturating_mul(self.row_gap.saturating_add(1))
    }

    pub const fn pager_row_offset(&self) -> u16 {
        if self.rows_per_page == 0 {
            0
        } else {
            self.rows_per_page
                .saturating_sub(1)
                .saturating_mul(self.row_gap.saturating_add(1))
                .saturating_add(1)
        }
    }
}

/// Removes Unicode controls and cuts a label at its first line boundary.
pub fn sanitize_single_line(value: &str) -> String {
    value
        .chars()
        .take_while(|character| !matches!(character, '\n' | '\r'))
        .filter(|character| !character.is_control())
        .collect()
}

/// Truncates a single-line value to a display width, using an ellipsis when necessary.
pub fn ellipsize(value: &str, max_width: usize) -> String {
    let value = sanitize_single_line(value);
    if display_width(&value) <= max_width {
        return value;
    }
    if max_width == 0 {
        return String::new();
    }
    if max_width == 1 {
        return "…".into();
    }

    let mut output = String::new();
    let mut width = 0;
    for character in value.chars() {
        let character_width = UnicodeWidthChar::width(character).unwrap_or(0);
        if width + character_width > max_width - 1 {
            break;
        }
        output.push(character);
        width += character_width;
    }
    output.push('…');
    output
}

/// Arranges rendered cells in the largest fitting number of columns.
///
/// For each candidate, packing uses the maximum rendered width of each actual column on every
/// page. A single overwide cell falls back to one clipped column. The function first tests the
/// full physical grid height without a pager row, then reserves a pager row only when multiple
/// pages are necessary.
pub fn arrange_cells(
    cells: &[Cell<'_>],
    area: GridRect,
    between_rows: u16,
    between_columns: u16,
) -> GridPlan {
    let rows_without_pager = cell_rows_for_height(area.rows, between_rows);
    if cells.is_empty() {
        return empty_plan(rows_without_pager, between_rows);
    }
    if rows_without_pager == 0 {
        return no_grid_plan(between_rows);
    }

    let without_pager = select_plan(
        cells,
        area.width,
        rows_without_pager,
        between_rows,
        between_columns,
    );
    if without_pager.page_count == 1 {
        return without_pager;
    }
    if area.rows <= 1 {
        return no_grid_plan(between_rows);
    }

    let rows_with_pager = cell_rows_for_height(area.rows - 1, between_rows);
    if rows_with_pager == 0 {
        return no_grid_plan(between_rows);
    }
    let mut with_pager = select_plan(
        cells,
        area.width,
        rows_with_pager,
        between_rows,
        between_columns,
    );
    with_pager.has_pager = true;
    with_pager
}

fn cell_rows_for_height(height: u16, between_rows: u16) -> u16 {
    if height == 0 {
        0
    } else {
        1 + (height - 1) / between_rows.saturating_add(1)
    }
}

fn empty_plan(rows_per_page: u16, row_gap: u16) -> GridPlan {
    GridPlan {
        columns: 0,
        row_gap,
        rows_per_page,
        page_count: 1,
        has_pager: false,
        pages: vec![GridPage {
            columns: Vec::new(),
        }],
        slots: Vec::new(),
    }
}

fn no_grid_plan(row_gap: u16) -> GridPlan {
    GridPlan {
        columns: 0,
        row_gap,
        rows_per_page: 0,
        page_count: 1,
        has_pager: false,
        pages: vec![GridPage {
            columns: Vec::new(),
        }],
        slots: Vec::new(),
    }
}

fn select_plan(
    cells: &[Cell<'_>],
    width: u16,
    rows: u16,
    row_gap: u16,
    column_gap: u16,
) -> GridPlan {
    let max_columns = cells.len().div_ceil(rows as usize).min(u16::MAX as usize);
    for columns in (1..=max_columns).rev() {
        if let Some(plan) =
            plan_for_columns(cells, width, rows, row_gap, column_gap, columns as u16)
        {
            return plan;
        }
    }
    unreachable!("non-empty input always has a one-column fallback")
}

fn plan_for_columns(
    cells: &[Cell<'_>],
    available_width: u16,
    rows: u16,
    row_gap: u16,
    column_gap: u16,
    columns: u16,
) -> Option<GridPlan> {
    let capacity = columns as usize * rows as usize;
    let page_count = cells.len().div_ceil(capacity);
    let mut pages = Vec::with_capacity(page_count);
    let mut slots = Vec::with_capacity(cells.len());

    for page in 0..page_count {
        let start = page * capacity;
        let end = (start + capacity).min(cells.len());
        let used_columns = (end - start).div_ceil(rows as usize);
        let mut widths = Vec::with_capacity(used_columns);
        for column in 0..used_columns {
            let column_start = start + column * rows as usize;
            let column_end = (column_start + rows as usize).min(end);
            let width = cells[column_start..column_end]
                .iter()
                .map(|cell| display_width(cell.text))
                .max()
                .expect("used columns contain a cell");
            widths.push(width);
        }
        let required_width = widths
            .iter()
            .copied()
            .sum::<usize>()
            .saturating_add(column_gap as usize * widths.len().saturating_sub(1));
        if required_width > available_width as usize && columns > 1 {
            return None;
        }

        let mut offset = 0u16;
        let mut page_columns = Vec::with_capacity(widths.len());
        for width in widths {
            let width = width.min(u16::MAX as usize) as u16;
            page_columns.push(GridColumn { offset, width });
            offset = offset.saturating_add(width).saturating_add(column_gap);
        }
        for source_index in start..end {
            let within_page = source_index - start;
            slots.push(GridSlot {
                source_index,
                page,
                column: (within_page / rows as usize) as u16,
                row: (within_page % rows as usize) as u16,
            });
        }
        pages.push(GridPage {
            columns: page_columns,
        });
    }

    Some(GridPlan {
        columns,
        row_gap,
        rows_per_page: rows,
        page_count,
        has_pager: false,
        pages,
        slots,
    })
}

fn display_width(value: &str) -> usize {
    UnicodeWidthStr::width(value)
}

#[cfg(test)]
mod tests {
    use proptest::prelude::*;

    use super::*;

    #[test]
    fn labels_are_single_line_and_control_free_before_template_rendering() {
        assert_eq!(
            sanitize_single_line("open\u{0007} now\nignored"),
            "open now"
        );
    }

    #[test]
    fn uneven_column_widths_allow_a_wider_arrangement() {
        let cells = [
            Cell {
                text: "very-long-visible-cell",
            },
            Cell { text: "x" },
            Cell { text: "y" },
        ];
        let plan = arrange_cells(&cells, GridRect { width: 26, rows: 2 }, 0, 2);

        assert_eq!(plan.columns, 2);
        assert_eq!(
            plan.page(0).expect("first page").columns,
            vec![
                GridColumn {
                    offset: 0,
                    width: 22
                },
                GridColumn {
                    offset: 24,
                    width: 1
                },
            ]
        );
    }

    #[test]
    fn template_expansion_controls_column_measurement() {
        let cells = [
            Cell {
                text: "! a      → open",
            },
            Cell {
                text: "b      → build",
            },
        ];
        let plan = arrange_cells(&cells, GridRect { width: 32, rows: 1 }, 0, 3);

        assert_eq!(plan.columns, 2);
        assert_eq!(
            plan.page(0).expect("first page").columns,
            vec![
                GridColumn {
                    offset: 0,
                    width: 15,
                },
                GridColumn {
                    offset: 18,
                    width: 14,
                },
            ]
        );
    }

    #[test]
    fn row_gaps_change_capacity_and_exact_offsets() {
        let cells = [
            Cell { text: "a" },
            Cell { text: "b" },
            Cell { text: "c" },
            Cell { text: "d" },
        ];
        let plan = arrange_cells(&cells, GridRect { width: 1, rows: 5 }, 1, 0);

        assert_eq!(plan.rows_per_page, 2);
        assert_eq!(plan.row_gap, 1);
        assert_eq!(plan.cell_row_offset(1), 2);
        assert!(plan.has_pager);
        assert_eq!(plan.pager_row_offset(), 3);
    }

    #[test]
    fn two_pass_pager_reservation_keeps_single_page_grid_taller() {
        let cells = [Cell { text: "a" }, Cell { text: "b" }, Cell { text: "c" }];
        let one_page = arrange_cells(&cells, GridRect { width: 4, rows: 2 }, 0, 1);
        let paged = arrange_cells(&cells, GridRect { width: 1, rows: 2 }, 0, 1);

        assert!(!one_page.has_pager);
        assert_eq!(one_page.rows_per_page, 2);
        assert!(paged.has_pager);
        assert_eq!(paged.rows_per_page, 1);
    }

    #[test]
    fn empty_and_scarce_grids_are_single_page_title_status_only_plans() {
        let empty = arrange_cells(&[], GridRect { width: 20, rows: 4 }, 0, 3);
        let cells = [Cell { text: "a" }, Cell { text: "b" }];
        let scarce = arrange_cells(&cells, GridRect { width: 1, rows: 1 }, 0, 0);

        assert_eq!(empty.page_count, 1);
        assert_eq!(empty.rows_per_page, 4);
        assert_eq!(scarce.page_count, 1);
        assert_eq!(scarce.rows_per_page, 0);
        assert!(!scarce.has_pager);
        assert!(scarce.slots.is_empty());
    }

    proptest! {
        #[test]
        fn packing_is_deterministic_and_places_each_cell_once(
            count in 1usize..128,
            width in 0u16..160,
            rows in 1u16..16,
            row_gap in 0u16..4,
            column_gap in 0u16..8,
        ) {
            let cells = vec![Cell { text: "ctrl+shift+f12 → a title" }; count];
            let area = GridRect { width, rows };
            let first = arrange_cells(&cells, area, row_gap, column_gap);
            let second = arrange_cells(&cells, area, row_gap, column_gap);

            prop_assert_eq!(&first, &second);
            if first.rows_per_page == 0 {
                prop_assert!(first.slots.is_empty());
            } else {
                prop_assert_eq!(first.slots.len(), count);
                for (source_index, slot) in first.slots.iter().enumerate() {
                    prop_assert_eq!(slot.source_index, source_index);
                    prop_assert!(slot.column < first.columns);
                    prop_assert!(slot.row < first.rows_per_page);
                    prop_assert!(slot.page < first.page_count);
                }
            }
        }
    }
}
