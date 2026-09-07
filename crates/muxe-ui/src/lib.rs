#![forbid(unsafe_code)]

//! Terminal presentation and input conversion for an attached Muxe UI session.

mod input;
mod layout;
mod runner;
mod runtime;
mod snapshot;
mod template;
mod terminal;
mod widget;

pub use input::{ConvertedInput, ConvertedKeyEvent, convert_input};
pub use layout::{
    Cell, GridColumn, GridPage, GridPlan, GridRect, GridSlot, arrange_cells, ellipsize,
    sanitize_single_line,
};
pub use runner::{
    DEFAULT_KITTY_NEGOTIATION_TIMEOUT, DETACH_CLEANUP_TIMEOUT, UiControl, UiExit, UiRunError,
    UiSession, run_attached,
};
pub use runtime::{InvocationDisposition, PreparedMenu, UiCommand, UiError, UiRuntime};
pub use snapshot::{ArchivedUiSnapshot, SnapshotError};
pub use template::{
    BreadcrumbTemplate, CellTemplate, PaginationTemplate, RenderedSpan, RenderedText,
    StatusTemplate, TemplateError, TemplateRenderer, escape_markup_text,
};
pub use terminal::{
    InputDriver, KittyNegotiation, KittyNegotiationError, NegotiationUpdate, SurfaceFrame,
    SurfacePadding, TerminalSurface, kitty_flags,
};
pub use widget::{MenuGrid, MenuStatus};
