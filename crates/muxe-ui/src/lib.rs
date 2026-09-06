#![forbid(unsafe_code)]

//! Terminal presentation and input conversion for an attached Muxe UI session.

mod input;
mod layout;
mod snapshot;
mod template;
mod widget;

pub use input::{ConvertedInput, ConvertedKeyEvent, convert_input};
pub use layout::{Cell, GridPlan, GridRect, GridSlot, arrange_cells, sanitize_single_line};
pub use snapshot::{ArchivedUiSnapshot, SnapshotError};
pub use template::{CellTemplate, TemplateError, escape_markup_text, render_cell_template};
pub use widget::{MenuGrid, MenuGridCell, MenuStatus};
