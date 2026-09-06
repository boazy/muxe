#![forbid(unsafe_code)]

//! Terminal presentation and input conversion for an attached Muxe UI session.

mod input;
mod layout;
mod runtime;
mod snapshot;
mod template;
mod terminal;
mod widget;

pub use input::{convert_input, ConvertedInput, ConvertedKeyEvent};
pub use layout::{
    arrange_cells, ellipsize, sanitize_single_line, Cell, GridColumn, GridPage, GridPlan, GridRect,
    GridSlot,
};
pub use runtime::{PreparedMenu, UiCommand, UiError, UiRuntime};
pub use snapshot::{ArchivedUiSnapshot, SnapshotError};
pub use template::{
    escape_markup_text, BreadcrumbTemplate, CellTemplate, PaginationTemplate, RenderedSpan,
    RenderedText, StatusTemplate, TemplateError, TemplateRenderer,
};
pub use terminal::{
    kitty_flags, InputDriver, KittyNegotiation, KittyNegotiationError, NegotiationUpdate,
    SurfaceFrame, SurfacePadding, TerminalSurface,
};
pub use widget::{MenuGrid, MenuStatus};
