use std::time::Duration;

use muxe_core::{
    AfterAction, CanonicalKey, EventKind, ExecutionId as CoreExecutionId, KeyCapabilities,
    KeyboardProfile, MenuControl as CoreMenuControl, MenuId as CoreMenuId, MenuSession,
    MenuSessionEvent, MenuSessionInput, MenuSessionOutput, MenuSessionState, SessionInstant,
};
use muxe_protocol::{
    ArchivedKeyboardProfileWire, ArchivedLocalMenuActionWire, ArchivedMenuControl,
    ArchivedMenuViewMenuWire, BindingAvailability, BindingId, BrokerEvent,
    ConditionEvaluationErrorWire, ExecutionId as WireExecutionId, ExecutionOutcome, MenuControl,
    PagesContextWire, ProtocolDiagnostic, evaluate_archived_binding_state,
};
use ratatui::layout::Rect;
use thiserror::Error;
use unicode_width::UnicodeWidthStr;

use crate::{
    ArchivedUiSnapshot, BreadcrumbTemplate, Cell, CellTemplate, ConvertedInput, GridPlan, GridRect,
    MenuStatus, PaginationTemplate, RenderedText, SnapshotError, StatusTemplate, SurfaceFrame,
    SurfacePadding, TemplateError, TemplateRenderer, arrange_cells, sanitize_single_line,
};

/// A broker request or local state transition selected by a checked menu binding.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum UiCommand {
    /// The broker remains authoritative for this ordinary binding.
    Invoke { generation: u64, binding: BindingId },
    /// The broker must apply this control to a pending execution.
    MenuControl(MenuControl),
    /// The attached session must be released without a further menu-control request.
    Detach,
    /// The UI consumed input by changing local menu or pager state.
    Redraw,
    /// The input does not select a visible, enabled binding.
    Ignored,
}

/// Broker-authoritative disposition for one accepted binding invocation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum InvocationDisposition {
    /// The broker will emit one terminal completion for this full wire execution ID.
    Await,
    /// The configured post-action transition applies immediately.
    Detached,
    /// A focus-sensitive action waits for host-confirmed UI disappearance.
    /// It always quits the UI and ignores the binding's `after_action`.
    Dismissed,
}
/// A fully rendered page derived from one checked archived attachment.
pub struct PreparedMenu {
    pub title: String,
    pub breadcrumbs: RenderedText,
    pub padding: SurfacePadding,
    pub plan: GridPlan,
    pub cells: Vec<RenderedText>,
    pub page: usize,
    pub pager: Option<RenderedText>,
    pub status: Option<RenderedText>,
}

impl PreparedMenu {
    /// Borrows this prepared page as content for a terminal surface redraw.
    #[must_use]
    pub fn surface_frame(&self) -> SurfaceFrame<'_> {
        SurfaceFrame {
            title: &self.title,
            breadcrumb: &self.breadcrumbs,
            padding: self.padding,
            plan: &self.plan,
            cells: &self.cells,
            page: self.page,
            pager: self.pager.as_ref(),
            status: self.status.as_ref().map(|rendered| MenuStatus { rendered }),
        }
    }
}

/// Runtime failure while traversing a checked broker attachment.
#[derive(Debug, Error)]
pub enum UiError {
    #[error(transparent)]
    Snapshot(#[from] SnapshotError),
    #[error(transparent)]
    Template(#[from] TemplateError),
    #[error(transparent)]
    Condition(#[from] ConditionEvaluationErrorWire),
    #[error("attachment has no menu at index {0}")]
    MissingMenu(usize),
    #[error("attachment does not contain its root menu")]
    MissingRoot,
    #[error("attachment binding has an invalid canonical key `{0}`")]
    InvalidBindingKey(String),
    #[error("page-dependent binding conditions did not reach a stable page count")]
    UnstablePageConditions,
    #[error("menu session is already dismissed")]
    SessionDismissed,
    #[error("attachment does not contain binding {generation}:{ordinal}")]
    MissingBinding { generation: u64, ordinal: u64 },
    #[error("UI-local execution sequence overflowed")]
    ExecutionSequenceExhausted,
}

struct StatusMessage {
    level: String,
    message: String,
}

struct BindingAvailabilityOverlay {
    binding: BindingId,
    availability: BindingAvailability,
    diagnostic: Option<String>,
}

struct PendingExecution {
    wire: WireExecutionId,
    core: CoreExecutionId,
    after_action: AfterAction,
}

struct BindingPolicy {
    after_action: AfterAction,
}

/// UI-local state around a checked archived attachment.
///
/// The frame remains the source of every menu, binding, layout, condition, and local action.
/// This type compiles only the attachment's template environment and produces transient rendered
/// cells for the current terminal size; it never deserializes or mirrors the menu graph.
pub struct UiRuntime {
    snapshot: ArchivedUiSnapshot,
    renderer: TemplateRenderer,
    keyboard_profile: KeyboardProfile,
    menu_session: MenuSession,
    current_page: usize,
    last_page_count: usize,
    last_area: Option<Rect>,
    status: Option<StatusMessage>,
    availability: Vec<BindingAvailabilityOverlay>,
    pending: Option<PendingExecution>,
    next_execution: u64,
}

impl UiRuntime {
    /// Attaches a checked broker response and compiles its supplied templates once.
    ///
    /// # Errors
    ///
    /// Returns [`UiError`] when the frame cannot be decoded, is not a UI attachment response,
    /// carries an invalid binding key, or supplies templates that fail to compile.
    pub fn attach(frame: muxe_protocol::ArchivedFrame) -> Result<Self, UiError> {
        Self::attach_at(frame, SessionInstant(Duration::ZERO))
    }

    /// Attaches a checked broker response at the caller-supplied monotonic session time.
    ///
    /// # Errors
    ///
    /// Returns [`UiError`] when the frame cannot be decoded, is not a UI attachment response,
    /// carries an invalid binding key, or supplies templates that fail to compile.
    pub fn attach_at(
        frame: muxe_protocol::ArchivedFrame,
        now: SessionInstant,
    ) -> Result<Self, UiError> {
        let snapshot = ArchivedUiSnapshot::new(frame)?;
        let renderer = snapshot
            .with_attachment(|attachment| TemplateRenderer::from_archived(&attachment.theme))??;
        let keyboard_profile = snapshot.with_attachment(keyboard_profile_from_attachment)?;
        let (root, timeout) = snapshot.with_attachment(|attachment| {
            (
                CoreMenuId::new(attachment.menu.root.0.as_str()),
                attachment
                    .inactivity_timeout_millis
                    .as_ref()
                    .map(|timeout| Duration::from_millis(timeout.to_native())),
            )
        })?;
        snapshot.with_attachment(validate_binding_keys)??;
        Ok(Self {
            snapshot,
            renderer,
            keyboard_profile,
            menu_session: MenuSession::new(root, timeout, now),
            current_page: 0,
            last_page_count: 1,
            last_area: None,
            status: None,
            availability: Vec::new(),
            pending: None,
            next_execution: 0,
        })
    }

    /// Returns the checked broker session ID associated with this runtime.
    ///
    /// # Errors
    ///
    /// Returns [`UiError`] when the retained frame fails to decode or is not a UI attachment
    /// response.
    pub fn session_id(&self) -> Result<&str, UiError> {
        Ok(self.snapshot.session_id()?)
    }

    /// Returns the parser profile exactly supplied by the attached broker snapshot.
    ///
    /// # Errors
    ///
    /// Returns [`UiError`] when the attached keyboard profile cannot be read. The profile is
    /// cloned from the checked attachment, so a valid attachment always succeeds.
    pub fn keyboard_profile(&self) -> Result<KeyboardProfile, UiError> {
        Ok(self.keyboard_profile.clone())
    }

    /// Returns the next caller-supplied inactivity deadline, if it is not paused.
    pub fn inactivity_deadline(&self) -> Option<SessionInstant> {
        self.menu_session.deadline()
    }

    /// Applies one converted terminal input at a caller-supplied monotonic session time.
    ///
    /// # Errors
    ///
    /// Returns [`UiError`] when the current menu cannot be resolved, a binding key is
    /// invalid, or a binding condition fails to evaluate.
    pub fn handle_input_at(
        &mut self,
        input: &ConvertedInput,
        at: SessionInstant,
    ) -> Result<UiCommand, UiError> {
        let ConvertedInput::Key(input) = input else {
            return Ok(UiCommand::Ignored);
        };
        if input.event.kind == EventKind::Release {
            let output = self.menu_session.handle(MenuSessionEvent::Key {
                at,
                input: MenuSessionInput::Unknown,
            });
            return Ok(self.menu_output(output.as_ref()));
        }

        let selection = self.select_current_binding(input)?;
        let active = matches!(self.menu_session.state(), MenuSessionState::Active);
        match selection {
            Selection::Unavailable(message) => {
                let output = self.menu_session.handle(MenuSessionEvent::Key {
                    at,
                    input: MenuSessionInput::Unknown,
                });
                if active {
                    self.status = Some(StatusMessage {
                        level: "blocked".into(),
                        message,
                    });
                    return Ok(UiCommand::Redraw);
                }
                Ok(self.menu_output(output.as_ref()))
            }
            Selection::Open(target) if active => {
                let output = self.menu_session.handle(MenuSessionEvent::OpenSubmenu {
                    at,
                    menu: CoreMenuId::new(target),
                });
                Ok(self.menu_output(output.as_ref()))
            }
            Selection::Control(control) => {
                let output = self.menu_session.handle(MenuSessionEvent::Key {
                    at,
                    input: MenuSessionInput::Control(wire_to_core_control(control)),
                });
                Ok(self.menu_output(output.as_ref()))
            }
            Selection::PagePrevious | Selection::PageNext if active => {
                let output = self.menu_session.handle(MenuSessionEvent::Key {
                    at,
                    input: MenuSessionInput::Unknown,
                });
                if !matches!(output, Some(MenuSessionOutput::UnknownKeySwallowed)) {
                    return Ok(self.menu_output(output.as_ref()));
                }
                let previous = matches!(selection, Selection::PagePrevious);
                if self.turn_page(previous) {
                    self.status = None;
                    Ok(UiCommand::Redraw)
                } else {
                    Ok(UiCommand::Ignored)
                }
            }
            Selection::Binding(binding) => {
                self.status = None;
                let output = self.menu_session.handle(MenuSessionEvent::Key {
                    at,
                    input: MenuSessionInput::Binding(core_binding_id(binding)),
                });
                Ok(self.menu_output(output.as_ref()))
            }
            Selection::Ignored
            | Selection::Open(_)
            | Selection::PagePrevious
            | Selection::PageNext => {
                let output = self.menu_session.handle(MenuSessionEvent::Key {
                    at,
                    input: MenuSessionInput::Unknown,
                });
                Ok(self.menu_output(output.as_ref()))
            }
        }
    }

    /// Resolves the visible binding selected by one key input on the current page.
    fn select_current_binding(
        &self,
        input: &crate::ConvertedKeyEvent,
    ) -> Result<Selection, UiError> {
        let current_menu = self.current_menu_id()?;
        self.snapshot.with_attachment(|attachment| {
            let menu = attachment
                .menu
                .menus
                .iter()
                .find(|menu| menu.id.0.as_str() == current_menu.as_str())
                .ok_or(UiError::MissingRoot)?;
            select_binding(
                menu,
                &self.keyboard_profile,
                input,
                self.current_page,
                self.last_page_count,
                &self.availability,
            )
        })?
    }

    /// Moves the pager one page, reporting whether the visible page changed.
    fn turn_page(&mut self, previous: bool) -> bool {
        if previous {
            if self.current_page > 0 {
                self.current_page -= 1;
                true
            } else {
                false
            }
        } else if self.current_page.saturating_add(1) < self.last_page_count {
            self.current_page += 1;
            true
        } else {
            false
        }
    }

    /// Advances the pure session clock to its next caller-owned deadline.
    pub fn tick(&mut self, at: SessionInstant) -> UiCommand {
        let output = self.menu_session.handle(MenuSessionEvent::Tick { at });
        self.menu_output(output.as_ref())
    }

    /// Records the broker's authoritative acceptance disposition for a selected binding.
    ///
    /// # Errors
    ///
    /// Returns [`UiError::MissingBinding`] when the binding is not part of the pinned attachment,
    /// or [`UiError::ExecutionSequenceExhausted`] when no local execution ID remains.
    pub fn invocation_accepted(
        &mut self,
        binding: BindingId,
        execution: WireExecutionId,
        disposition: InvocationDisposition,
        at: SessionInstant,
    ) -> Result<UiCommand, UiError> {
        let policy = self.binding_policy(&binding)?;
        match disposition {
            InvocationDisposition::Await => {
                if !matches!(self.menu_session.state(), MenuSessionState::Active)
                    || self.pending.is_some()
                {
                    return Ok(UiCommand::Ignored);
                }
                self.next_execution = self
                    .next_execution
                    .checked_add(1)
                    .ok_or(UiError::ExecutionSequenceExhausted)?;
                let core = CoreExecutionId(self.next_execution);
                self.pending = Some(PendingExecution {
                    wire: execution,
                    core,
                    after_action: policy.after_action,
                });
                let output = self.menu_session.handle(MenuSessionEvent::ActionPending {
                    at,
                    execution: core,
                });
                Ok(self.menu_output(output.as_ref()))
            }
            InvocationDisposition::Detached | InvocationDisposition::Dismissed => {
                let after_action = if disposition == InvocationDisposition::Dismissed {
                    AfterAction::Quit
                } else {
                    policy.after_action
                };
                let output = self
                    .menu_session
                    .handle(MenuSessionEvent::DetachedAccepted {
                        at,
                        execution: CoreExecutionId(0),
                        after_action,
                    });
                Ok(self.menu_output(output.as_ref()))
            }
        }
    }

    /// Applies the broker's correlated acknowledgement of one pending menu control.
    pub fn pending_control_completed(
        &mut self,
        execution: &WireExecutionId,
        control: MenuControl,
        at: SessionInstant,
    ) -> UiCommand {
        let Some(pending) = self.pending.as_ref() else {
            return UiCommand::Ignored;
        };
        if &pending.wire != execution {
            return UiCommand::Ignored;
        }
        let core = pending.core;
        let output = self
            .menu_session
            .handle(MenuSessionEvent::PendingControlCompleted {
                at,
                execution: core,
                control: wire_to_core_control(control),
            });
        if !matches!(
            self.menu_session.state(),
            MenuSessionState::Pending {
                execution,
                ..
            } if *execution == core
        ) {
            self.pending = None;
        }
        self.menu_output(output.as_ref())
    }

    /// Applies one asynchronous broker event without replacing the pinned attachment.
    ///
    /// # Errors
    ///
    /// Returns [`UiError`] when the attached session ID or attachment cannot be read, or when a
    /// binding condition fails to evaluate.
    pub fn handle_broker_event(
        &mut self,
        event: &BrokerEvent,
        at: SessionInstant,
    ) -> Result<UiCommand, UiError> {
        match event {
            BrokerEvent::ExecutionCompleted {
                session,
                execution,
                outcome,
                diagnostic,
            } if session.as_str() == self.session_id()? => {
                Ok(self.on_execution_completed(execution, *outcome, diagnostic.as_ref(), at))
            }
            BrokerEvent::BindingAvailabilityChanged {
                session,
                generation,
                binding,
                availability,
                diagnostic,
            } if session.as_str() == self.session_id()? => {
                let known = self.snapshot.with_attachment(|attachment| {
                    attachment.menu.generation.to_native() == *generation
                        && attachment
                            .menu
                            .menus
                            .iter()
                            .flat_map(|menu| menu.bindings.iter())
                            .any(|candidate| {
                                candidate.id.generation.to_native() == binding.generation
                                    && candidate.id.ordinal.to_native() == binding.ordinal
                            })
                })?;
                if !known {
                    return Ok(UiCommand::Ignored);
                }
                if let Some(current) = self
                    .availability
                    .iter_mut()
                    .find(|current| current.binding == *binding)
                {
                    current.availability = *availability;
                    current.diagnostic = diagnostic
                        .as_ref()
                        .map(|diagnostic| diagnostic.message.clone());
                } else {
                    self.availability.push(BindingAvailabilityOverlay {
                        binding: *binding,
                        availability: *availability,
                        diagnostic: diagnostic
                            .as_ref()
                            .map(|diagnostic| diagnostic.message.clone()),
                    });
                }
                Ok(UiCommand::Redraw)
            }
            BrokerEvent::AdapterHealthChanged {
                healthy,
                diagnostic,
            } => {
                self.status = Some(StatusMessage {
                    level: if *healthy { "ready" } else { "blocked" }.into(),
                    message: diagnostic.as_ref().map_or_else(
                        || {
                            if *healthy {
                                "Host adapter is available".into()
                            } else {
                                "Host adapter is unavailable".into()
                            }
                        },
                        |diagnostic| diagnostic.message.clone(),
                    ),
                });
                Ok(UiCommand::Redraw)
            }
            BrokerEvent::BrokerRetiring | BrokerEvent::Fatal(_) => Ok(UiCommand::Detach),
            _ => Ok(UiCommand::Ignored),
        }
    }

    /// Applies a broker execution completion to the pending invocation.
    fn on_execution_completed(
        &mut self,
        execution: &WireExecutionId,
        outcome: ExecutionOutcome,
        diagnostic: Option<&ProtocolDiagnostic>,
        at: SessionInstant,
    ) -> UiCommand {
        let Some(pending) = self.pending.as_ref() else {
            return UiCommand::Ignored;
        };
        if pending.wire != *execution {
            return UiCommand::Ignored;
        }
        let core = pending.core;
        let after_action = pending.after_action;
        let output = self.menu_session.handle(MenuSessionEvent::ActionCompleted {
            at,
            execution: core,
            success: matches!(outcome, ExecutionOutcome::Succeeded),
            after_action,
        });
        let still_pending = matches!(
            self.menu_session.state(),
            MenuSessionState::Pending {
                execution,
                ..
            } if *execution == core
        );
        if !still_pending {
            self.pending = None;
        }
        if output.is_some() {
            return self.menu_output(output.as_ref());
        }
        if !still_pending {
            let (level, fallback) = execution_status(outcome);
            self.status = Some(StatusMessage {
                level: level.into(),
                message: diagnostic
                    .map_or_else(|| fallback.into(), |diagnostic| diagnostic.message.clone()),
            });
            return UiCommand::Redraw;
        }
        UiCommand::Ignored
    }

    /// Shows a recoverable broker diagnostic without replacing the pinned attachment.
    pub fn report_broker_error(&mut self, message: String) {
        self.status = Some(StatusMessage {
            level: "error".into(),
            message,
        });
    }

    /// Renders the current menu against the exact terminal rectangle before a surface redraw.
    ///
    /// # Errors
    ///
    /// Returns [`UiError`] when the current menu is missing, a template fails to render, or the
    /// page conditions never stabilize on a page count.
    pub fn prepare(&mut self, area: Rect) -> Result<PreparedMenu, UiError> {
        if self
            .last_area
            .replace(area)
            .is_some_and(|previous| previous != area)
        {
            self.current_page = 0;
            self.last_page_count = 1;
        }
        let current_menu = self.current_menu_id()?;
        let stack = self.menu_session.stack();
        let current_page = self.current_page;
        let last_page_count = self.last_page_count;
        let status = self.status.as_ref();
        let (prepared, page) = self.snapshot.with_attachment(|attachment| {
            let menu = attachment
                .menu
                .menus
                .iter()
                .find(|menu| menu.id.0.as_str() == current_menu.as_str())
                .ok_or(UiError::MissingRoot)?;
            let padding = SurfacePadding {
                left: menu.layout.padding.left.to_native(),
                right: menu.layout.padding.right.to_native(),
                top: menu.layout.padding.top.to_native(),
                bottom: menu.layout.padding.bottom.to_native(),
            };
            let grid = grid_rect(area, padding, status.is_some());
            let max_title_width = menu.layout.max_item_title_length.to_native() as usize;
            let mut count = last_page_count.max(1);
            let mut page = current_page.min(count.saturating_sub(1));
            let attempts = menu.bindings.len().saturating_mul(2).saturating_add(2);

            for _ in 0..attempts {
                let pages = PagesContextWire {
                    current: page.saturating_add(1) as u64,
                    count: count as u64,
                };
                let cells = render_visible_cells(
                    &self.renderer,
                    menu,
                    pages,
                    max_title_width,
                    &self.availability,
                )?;
                let layout_cells = cells
                    .iter()
                    .map(|rendered| Cell {
                        text: &rendered.plain,
                    })
                    .collect::<Vec<_>>();
                let plan = arrange_cells(
                    &layout_cells,
                    grid,
                    menu.layout.padding.between_rows.to_native(),
                    menu.layout.padding.between_columns.to_native(),
                );
                let next_count = plan.page_count.max(1);
                let next_page = page.min(next_count.saturating_sub(1));
                if next_count == count && next_page == page {
                    let breadcrumbs = render_breadcrumbs(&self.renderer, attachment, stack)?;
                    let pager_text =
                        render_pager(&self.renderer, menu, &plan, page, grid.width as usize)?;
                    let status = status
                        .map(|status| {
                            self.renderer.render_status(StatusTemplate {
                                level: &status.level,
                                message: &status.message,
                            })
                        })
                        .transpose()?;
                    return Ok((
                        PreparedMenu {
                            title: menu
                                .title
                                .as_ref()
                                .map(|title| sanitize_single_line(title.as_str()))
                                .unwrap_or_default(),
                            breadcrumbs,
                            padding,
                            plan,
                            cells,
                            page,
                            pager: pager_text,
                            status,
                        },
                        page,
                    ));
                }
                count = next_count;
                page = next_page;
            }
            Err(UiError::UnstablePageConditions)
        })??;
        self.current_page = page;
        self.last_page_count = prepared.plan.page_count.max(1);
        Ok(prepared)
    }

    fn current_menu_id(&self) -> Result<CoreMenuId, UiError> {
        self.menu_session
            .current_menu()
            .cloned()
            .ok_or(UiError::SessionDismissed)
    }

    fn binding_policy(&self, binding: &BindingId) -> Result<BindingPolicy, UiError> {
        self.snapshot.with_attachment(|attachment| {
            attachment
                .menu
                .menus
                .iter()
                .flat_map(|menu| menu.bindings.iter())
                .find(|candidate| {
                    candidate.id.generation.to_native() == binding.generation
                        && candidate.id.ordinal.to_native() == binding.ordinal
                })
                .map(|binding| BindingPolicy {
                    after_action: archived_after_action(&binding.settings.after_action),
                })
                .ok_or(UiError::MissingBinding {
                    generation: binding.generation,
                    ordinal: binding.ordinal,
                })
        })?
    }

    fn menu_output(&mut self, output: Option<&MenuSessionOutput>) -> UiCommand {
        match output {
            Some(MenuSessionOutput::InvokeBinding(binding)) => UiCommand::Invoke {
                generation: binding.generation().0,
                binding: BindingId {
                    generation: binding.generation().0,
                    ordinal: binding.ordinal(),
                },
            },
            Some(MenuSessionOutput::RequestPendingControl { control, .. }) => {
                UiCommand::MenuControl(core_to_wire_control(*control))
            }
            Some(MenuSessionOutput::NavigatedTo(_) | MenuSessionOutput::ReturnedTo(_)) => {
                self.current_page = 0;
                self.last_page_count = 1;
                self.status = None;
                UiCommand::Redraw
            }
            Some(MenuSessionOutput::Dismissed) => UiCommand::Detach,
            Some(
                MenuSessionOutput::UnknownKeySwallowed | MenuSessionOutput::PendingInputSwallowed,
            )
            | None => UiCommand::Ignored,
        }
    }

    #[cfg(test)]
    fn handle_input(&mut self, input: &ConvertedInput) -> Result<UiCommand, UiError> {
        self.handle_input_at(input, SessionInstant(Duration::ZERO))
    }
}

fn execution_status(outcome: ExecutionOutcome) -> (&'static str, &'static str) {
    match outcome {
        ExecutionOutcome::Succeeded => ("ready", "Action completed"),
        ExecutionOutcome::Failed => ("error", "Action failed"),
        ExecutionOutcome::Cancelled => ("blocked", "Action cancelled"),
        ExecutionOutcome::TimedOut => ("error", "Action timed out"),
        ExecutionOutcome::Detached => ("ready", "Action continues detached"),
        ExecutionOutcome::OutcomeUnknown => ("error", "Action outcome is unknown"),
    }
}

fn core_binding_id(binding: BindingId) -> muxe_core::BindingId {
    muxe_core::BindingId::new(
        muxe_core::CompiledGeneration(binding.generation),
        binding.ordinal,
    )
}

fn wire_to_core_control(control: MenuControl) -> CoreMenuControl {
    match control {
        MenuControl::Quit => CoreMenuControl::Quit,
        MenuControl::Return => CoreMenuControl::Return,
    }
}

fn core_to_wire_control(control: CoreMenuControl) -> MenuControl {
    match control {
        CoreMenuControl::Quit => MenuControl::Quit,
        CoreMenuControl::Return => MenuControl::Return,
    }
}

fn archived_after_action(after_action: &muxe_protocol::ArchivedAfterAction) -> AfterAction {
    match after_action {
        muxe_protocol::ArchivedAfterAction::Quit => AfterAction::Quit,
        muxe_protocol::ArchivedAfterAction::Return => AfterAction::Return,
        muxe_protocol::ArchivedAfterAction::Stay => AfterAction::Stay,
    }
}

fn keyboard_profile_from_attachment(
    attachment: &muxe_protocol::ArchivedUiAttachmentWire,
) -> KeyboardProfile {
    match &attachment.keyboard {
        ArchivedKeyboardProfileWire::Vt100 {
            escape_timeout_millis,
        } => KeyboardProfile::Vt100 {
            escape_timeout: Duration::from_millis(escape_timeout_millis.to_native()),
        },
        ArchivedKeyboardProfileWire::Kitty(capabilities) => {
            KeyboardProfile::Kitty(KeyCapabilities {
                event_types: capabilities.event_types,
                alternate_keys: capabilities.alternate_keys,
                all_keys_as_escape_codes: capabilities.all_keys_as_escape_codes,
            })
        }
    }
}

fn grid_rect(area: Rect, padding: SurfacePadding, has_status: bool) -> GridRect {
    let body_height = area.height.saturating_sub(1);
    GridRect {
        width: area
            .width
            .saturating_sub(padding.left.saturating_add(padding.right)),
        rows: body_height
            .saturating_sub(padding.top.saturating_add(padding.bottom))
            .saturating_sub(u16::from(has_status)),
    }
}

fn render_visible_cells(
    renderer: &TemplateRenderer,
    menu: &ArchivedMenuViewMenuWire,
    pages: PagesContextWire,
    max_title_width: usize,
    availability: &[BindingAvailabilityOverlay],
) -> Result<Vec<RenderedText>, UiError> {
    let mut cells = Vec::new();
    for binding in menu.bindings.iter() {
        let state = evaluate_archived_binding_state(binding, pages)?;
        if !state.included || !state.shown || binding.hidden {
            continue;
        }
        let binding_id = BindingId {
            generation: binding.id.generation.to_native(),
            ordinal: binding.id.ordinal.to_native(),
        };
        let blocked = availability
            .iter()
            .find(|current| current.binding == binding_id)
            .map_or(state.blocked, |current| {
                current.availability == BindingAvailability::Blocked
            });
        cells.push(
            renderer.render_cell(CellTemplate {
                key: binding.key.as_str(),
                title: binding
                    .label
                    .as_ref()
                    .map(rkyv::string::ArchivedString::as_str)
                    .unwrap_or_default(),
                disabled: !state.enabled || blocked,
                blocked,
                max_title_width,
            })?,
        );
    }
    Ok(cells)
}

fn render_breadcrumbs(
    renderer: &TemplateRenderer,
    attachment: &muxe_protocol::ArchivedUiAttachmentWire,
    stack: &[CoreMenuId],
) -> Result<RenderedText, UiError> {
    let crumbs = stack
        .iter()
        .filter_map(|id| {
            attachment
                .menu
                .menus
                .iter()
                .find(|menu| menu.id.0.as_str() == id.as_str())
        })
        .filter_map(|menu| {
            menu.title
                .as_ref()
                .map(rkyv::string::ArchivedString::as_str)
        })
        .collect::<Vec<_>>();
    renderer
        .render_breadcrumbs(BreadcrumbTemplate { crumbs: &crumbs })
        .map_err(UiError::from)
}

fn render_pager(
    renderer: &TemplateRenderer,
    menu: &ArchivedMenuViewMenuWire,
    plan: &GridPlan,
    page: usize,
    available_width: usize,
) -> Result<Option<RenderedText>, UiError> {
    if !plan.has_pager {
        return Ok(None);
    }
    let prev_keys = pager_keys(menu, true);
    let next_keys = pager_keys(menu, false);
    let pagination = PaginationTemplate {
        current: page.saturating_add(1) as u64,
        count: plan.page_count as u64,
        prev_key: prev_keys.first().copied().unwrap_or_default(),
        next_key: next_keys.first().copied().unwrap_or_default(),
        prev_keys: &prev_keys,
        next_keys: &next_keys,
    };
    let full = renderer.render_pagination_full(pagination)?;
    if UnicodeWidthStr::width(full.plain.as_str()) <= available_width {
        Ok(Some(full))
    } else {
        Ok(Some(renderer.render_pagination_short(pagination)?))
    }
}

fn pager_keys(menu: &ArchivedMenuViewMenuWire, previous: bool) -> Vec<&str> {
    menu.bindings
        .iter()
        .filter(|binding| match binding.local_menu_action.as_ref() {
            Some(ArchivedLocalMenuActionWire::PagePrevious) => previous,
            Some(ArchivedLocalMenuActionWire::PageNext) => !previous,
            _ => false,
        })
        .map(|binding| binding.key.as_str())
        .collect()
}

enum Selection {
    Ignored,
    Unavailable(String),
    Open(String),
    Control(MenuControl),
    PagePrevious,
    PageNext,
    Binding(BindingId),
}

fn validate_binding_keys(
    attachment: &muxe_protocol::ArchivedUiAttachmentWire,
) -> Result<(), UiError> {
    for binding in attachment
        .menu
        .menus
        .iter()
        .flat_map(|menu| menu.bindings.iter())
    {
        CanonicalKey::parse(binding.key.as_str())
            .map_err(|_| UiError::InvalidBindingKey(binding.key.as_str().to_owned()))?;
    }
    Ok(())
}

fn select_binding(
    menu: &ArchivedMenuViewMenuWire,
    profile: &KeyboardProfile,
    input: &crate::ConvertedKeyEvent,
    page: usize,
    page_count: usize,
    availability: &[BindingAvailabilityOverlay],
) -> Result<Selection, UiError> {
    let pages = PagesContextWire {
        current: page.saturating_add(1) as u64,
        count: page_count.max(1) as u64,
    };
    for binding in menu.bindings.iter() {
        if input.event.kind == EventKind::Repeat
            && !binding
                .settings
                .repeat
                .as_ref()
                .is_some_and(|repeat| *repeat)
        {
            continue;
        }
        let key = CanonicalKey::parse(binding.key.as_str())
            .map_err(|_| UiError::InvalidBindingKey(binding.key.as_str().to_owned()))?;
        if !profile.matches_binding(&key, &input.event) {
            continue;
        }
        let state = evaluate_archived_binding_state(binding, pages)?;
        if !state.included || !state.shown || binding.hidden {
            continue;
        }
        let binding_id = BindingId {
            generation: binding.id.generation.to_native(),
            ordinal: binding.id.ordinal.to_native(),
        };
        let availability = availability
            .iter()
            .find(|current| current.binding == binding_id);
        let blocked = availability.map_or(state.blocked, |current| {
            current.availability == BindingAvailability::Blocked
        });
        if !state.enabled || blocked {
            return Ok(Selection::Unavailable(
                availability
                    .and_then(|current| current.diagnostic.as_deref())
                    .or_else(|| {
                        binding
                            .diagnostic
                            .as_ref()
                            .map(|diagnostic| diagnostic.message.as_str())
                    })
                    .unwrap_or("Binding is unavailable")
                    .to_owned(),
            ));
        }
        return Ok(match binding.local_menu_action.as_ref() {
            Some(ArchivedLocalMenuActionWire::Open { target }) => {
                Selection::Open(target.0.as_str().to_owned())
            }
            Some(ArchivedLocalMenuActionWire::Control(ArchivedMenuControl::Quit)) => {
                Selection::Control(MenuControl::Quit)
            }
            Some(ArchivedLocalMenuActionWire::Control(ArchivedMenuControl::Return)) => {
                Selection::Control(MenuControl::Return)
            }
            Some(ArchivedLocalMenuActionWire::PagePrevious) => Selection::PagePrevious,
            Some(ArchivedLocalMenuActionWire::PageNext) => Selection::PageNext,
            None => Selection::Binding(binding_id),
        });
    }
    Ok(Selection::Ignored)
}

#[cfg(test)]
pub(crate) mod tests {
    use std::collections::BTreeMap;

    use muxe_core::{ThemeSection, compiled_default_theme};
    use muxe_protocol::{
        AfterAction, BindingConditionsWire, BindingSettingsWire, BindingStateWire, BrokerResponse,
        ColorSchemeWire, CompiledThemeWire, ConnectionDecoder, ConnectionPolicy, ExecutionMode,
        ExecutionPolicyWire, HostKind, KeyCapabilitiesWire, KeyboardProfileWire, LayoutPaddingWire,
        LayoutSettingsWire, LiveServerIdentity, MenuControlAction, MenuId, MenuViewMenuWire,
        MenuViewWire, NamedStringWire, NamedStyleWire, PeerRole, Prelude, RequestId,
        SchemaFingerprint, ServerId, StyleWire, ThemeSectionWire, TimeoutAction, UiAttachmentWire,
        UiSessionId, Welcome, WireMessage, encode_frame,
    };
    use muxe_terminal_input::{
        EventKind as RawEventKind, InputEvent, KeyIdentity as RawKeyIdentity, LockState,
        Modifiers as RawModifiers, Parser, RawKeyEvent,
    };

    use super::*;

    fn strings(values: &BTreeMap<String, String>) -> Vec<NamedStringWire> {
        values
            .iter()
            .map(|(name, value)| NamedStringWire {
                name: name.clone(),
                value: value.clone(),
            })
            .collect()
    }

    fn theme_section(section: &ThemeSection) -> ThemeSectionWire {
        ThemeSectionWire {
            styles: section
                .styles
                .iter()
                .map(|(name, style)| NamedStyleWire {
                    name: name.clone(),
                    style: StyleWire {
                        foreground: style.foreground.clone(),
                        background: style.background.clone(),
                        bold: style.bold,
                        dim: style.dim,
                        italic: style.italic,
                        underline: style.underline,
                        strikethrough: style.strikethrough,
                    },
                })
                .collect(),
            templates: strings(&section.templates),
        }
    }

    fn default_theme_wire() -> CompiledThemeWire {
        let theme = compiled_default_theme();
        CompiledThemeWire {
            common: theme_section(&theme.theme.common),
            menu: theme_section(&theme.theme.menu),
            settings: strings(&theme.theme.settings),
            scheme: ColorSchemeWire {
                title: theme.scheme.title,
                palette: strings(&theme.scheme.palette),
                colors: strings(&theme.scheme.colors),
            },
        }
    }

    fn settings() -> BindingSettingsWire {
        BindingSettingsWire {
            after_action: AfterAction::Stay,
            execution: ExecutionPolicyWire {
                mode: ExecutionMode::Await,
                timeout_millis: None,
                on_timeout: TimeoutAction::Detach,
                on_menu_control: MenuControlAction::Detach,
            },
            repeat: None,
        }
    }

    fn binding(
        ordinal: u64,
        key: &str,
        label: &str,
        conditions: BindingConditionsWire,
        local_menu_action: Option<muxe_protocol::LocalMenuActionWire>,
    ) -> muxe_protocol::BindingViewWire {
        muxe_protocol::BindingViewWire {
            id: BindingId {
                generation: 7,
                ordinal,
            },
            key: key.into(),
            label: Some(label.into()),
            hidden: false,
            state: BindingStateWire {
                included: true,
                enabled: true,
                shown: true,
                blocked: false,
            },
            settings: settings(),
            conditions,
            local_menu_action,
            diagnostic: None,
        }
    }

    pub(crate) fn binding_with_policy(
        ordinal: u64,
        key: &str,
        after_action: AfterAction,
        execution_mode: ExecutionMode,
        local_menu_action: Option<muxe_protocol::LocalMenuActionWire>,
    ) -> muxe_protocol::BindingViewWire {
        let mut binding = binding(
            ordinal,
            key,
            key,
            BindingConditionsWire::default(),
            local_menu_action,
        );
        binding.settings.after_action = after_action;
        binding.settings.execution.mode = execution_mode;
        binding
    }

    fn layout() -> LayoutSettingsWire {
        LayoutSettingsWire {
            padding: LayoutPaddingWire {
                left: 1,
                right: 1,
                top: 0,
                bottom: 0,
                between_rows: 0,
                between_columns: 3,
            },
            max_item_title_length: 24,
        }
    }

    fn frame_bytes(message: &WireMessage) -> Vec<u8> {
        let frame = encode_frame(message).expect("fixture message encodes");
        let mut bytes = frame.prefix().to_vec();
        bytes.extend_from_slice(frame.payload());
        bytes
    }

    fn archived_attachment() -> muxe_protocol::ArchivedFrame {
        let attachment = UiAttachmentWire {
            menu: MenuViewWire {
                generation: 7,
                root: MenuId::new("root"),
                menus: vec![
                    MenuViewMenuWire {
                        id: MenuId::new("root"),
                        title: Some("Root".into()),
                        layout: layout(),
                        bindings: vec![
                            binding(1, "a", "Open", BindingConditionsWire::default(), None),
                            binding(
                                2,
                                "n",
                                "Next",
                                BindingConditionsWire::default(),
                                Some(muxe_protocol::LocalMenuActionWire::Open {
                                    target: MenuId::new("child"),
                                }),
                            ),
                            binding(
                                3,
                                "h",
                                "Hidden by archived condition",
                                BindingConditionsWire {
                                    include: Some(muxe_protocol::ConditionIrWire::Bool(false)),
                                    ..BindingConditionsWire::default()
                                },
                                None,
                            ),
                        ],
                    },
                    MenuViewMenuWire {
                        id: MenuId::new("child"),
                        title: Some("Child".into()),
                        layout: layout(),
                        bindings: vec![binding(
                            4,
                            "r",
                            "Return",
                            BindingConditionsWire::default(),
                            Some(muxe_protocol::LocalMenuActionWire::Control(
                                MenuControl::Return,
                            )),
                        )],
                    },
                ],
            },
            keyboard: KeyboardProfileWire::Kitty(KeyCapabilitiesWire {
                event_types: true,
                alternate_keys: true,
                all_keys_as_escape_codes: false,
            }),
            inactivity_timeout_millis: None,
            theme: default_theme_wire(),
        };
        archive_attachment(attachment)
    }

    fn archive_attachment(attachment: UiAttachmentWire) -> muxe_protocol::ArchivedFrame {
        let mut decoder = ConnectionDecoder::new(ConnectionPolicy::client(
            PeerRole::Ui,
            SchemaFingerprint::application(),
        ));
        decoder
            .push(
                &Prelude::rkyv(PeerRole::Ui, SchemaFingerprint::application()).encode(),
                |_| {},
            )
            .expect("fixture prelude decodes");
        let welcome = WireMessage::Welcome {
            request_id: RequestId([1; 16]),
            welcome: Welcome {
                broker_version: "test".into(),
                live_server: LiveServerIdentity {
                    host: HostKind::Herdr,
                    discovery_key: "test".into(),
                    server_id: ServerId::new("server"),
                },
                accepted_frame_len: muxe_protocol::MAX_FRAME_LEN,
            },
        };
        decoder
            .push(&frame_bytes(&welcome), |_| {})
            .expect("fixture welcome decodes");
        let response = WireMessage::Response {
            request_id: RequestId([2; 16]),
            response: BrokerResponse::UiAttached {
                session: UiSessionId::new("ui"),
                snapshot: attachment,
            },
        };
        let mut output = None;
        decoder
            .push(&frame_bytes(&response), |frame| output = Some(frame))
            .expect("fixture attachment decodes");
        output.expect("response yields checked archive")
    }

    fn profiled_attachment(
        keyboard: KeyboardProfileWire,
        bindings: Vec<muxe_protocol::BindingViewWire>,
    ) -> muxe_protocol::ArchivedFrame {
        profiled_attachment_with_timeout(keyboard, bindings, None)
    }

    pub(crate) fn profiled_attachment_with_timeout(
        keyboard: KeyboardProfileWire,
        bindings: Vec<muxe_protocol::BindingViewWire>,
        inactivity_timeout_millis: Option<u64>,
    ) -> muxe_protocol::ArchivedFrame {
        archive_attachment(UiAttachmentWire {
            menu: MenuViewWire {
                generation: 7,
                root: MenuId::new("root"),
                menus: vec![MenuViewMenuWire {
                    id: MenuId::new("root"),
                    title: Some("Root".into()),
                    layout: layout(),
                    bindings,
                }],
            },
            keyboard,
            inactivity_timeout_millis,
            theme: default_theme_wire(),
        })
    }

    fn parsed_input(bytes: &[u8]) -> ConvertedInput {
        let mut parser = Parser::new();
        let mut event = None;
        parser.push(bytes, |parsed| {
            assert!(
                event.replace(parsed).is_none(),
                "one input sequence must emit one event"
            );
        });
        parser.finish(|parsed| {
            assert!(
                event.replace(parsed).is_none(),
                "one input sequence must emit one event"
            );
        });
        crate::convert_input(event.expect("input sequence emits one event"))
    }

    fn press(character: char) -> ConvertedInput {
        crate::convert_input(InputEvent::Key(RawKeyEvent {
            primary: RawKeyIdentity::Unicode(character),
            shifted: None,
            base: None,
            modifiers: RawModifiers::NONE,
            kind: RawEventKind::Press,
            locks: LockState::NONE,
            keypad: None,
        }))
    }
    #[test]
    fn broker_availability_events_overlay_the_pinned_attachment_without_replacing_it() {
        let mut runtime = UiRuntime::attach(archived_attachment()).expect("archive attaches");
        let binding = BindingId {
            generation: 7,
            ordinal: 1,
        };
        let blocked = BrokerEvent::BindingAvailabilityChanged {
            session: UiSessionId::new("ui"),
            generation: 7,
            binding,
            availability: BindingAvailability::Blocked,
            diagnostic: Some(muxe_protocol::ProtocolDiagnostic {
                code: muxe_protocol::DiagnosticCode::ActionBlocked,
                message: "Herdr is reconnecting".into(),
            }),
        };
        assert_eq!(
            runtime
                .handle_broker_event(&blocked, SessionInstant(Duration::ZERO))
                .expect("event applies to this session"),
            UiCommand::Redraw
        );
        assert_eq!(
            runtime
                .handle_input(&press('a'))
                .expect("blocked binding is handled locally"),
            UiCommand::Redraw
        );
        assert!(
            runtime
                .prepare(Rect::new(0, 0, 40, 8))
                .expect("status renders")
                .status
                .expect("availability diagnostic is visible")
                .plain
                .contains("Herdr is reconnecting")
        );

        let enabled = BrokerEvent::BindingAvailabilityChanged {
            session: UiSessionId::new("ui"),
            generation: 7,
            binding,
            availability: BindingAvailability::Enabled,
            diagnostic: None,
        };
        assert_eq!(
            runtime
                .handle_broker_event(&enabled, SessionInstant(Duration::ZERO))
                .expect("event restores this binding"),
            UiCommand::Redraw
        );
        assert_eq!(
            runtime
                .handle_input(&press('a'))
                .expect("restored binding invokes its pinned generation"),
            UiCommand::Invoke {
                generation: 7,
                binding: BindingId {
                    generation: 7,
                    ordinal: 1,
                },
            }
        );
        assert_eq!(
            runtime
                .handle_broker_event(
                    &BrokerEvent::BindingAvailabilityChanged {
                        session: UiSessionId::new("ui"),
                        generation: 8,
                        binding: BindingId {
                            generation: 8,
                            ordinal: 1,
                        },
                        availability: BindingAvailability::Blocked,
                        diagnostic: None,
                    },
                    SessionInstant(Duration::ZERO),
                )
                .expect("newer generations must not replace this attachment"),
            UiCommand::Ignored
        );
    }

    fn at(milliseconds: u64) -> SessionInstant {
        SessionInstant(Duration::from_millis(milliseconds))
    }

    #[test]
    fn unknown_input_resets_inactivity_before_the_session_dismisses() {
        let mut runtime = UiRuntime::attach_at(
            profiled_attachment_with_timeout(
                KeyboardProfileWire::Vt100 {
                    escape_timeout_millis: 25,
                },
                Vec::new(),
                Some(10),
            ),
            at(0),
        )
        .expect("attachment attaches");

        assert_eq!(
            runtime
                .handle_input_at(&press('x'), at(9))
                .expect("unknown input is swallowed"),
            UiCommand::Ignored
        );
        assert_eq!(runtime.tick(at(10)), UiCommand::Ignored);
        assert_eq!(runtime.tick(at(18)), UiCommand::Ignored);
        assert_eq!(runtime.tick(at(19)), UiCommand::Detach);
    }

    #[test]
    fn awaited_control_uses_the_core_session_and_ignores_stale_completion_nonces() {
        let mut runtime = UiRuntime::attach_at(
            profiled_attachment_with_timeout(
                KeyboardProfileWire::Vt100 {
                    escape_timeout_millis: 25,
                },
                vec![
                    binding_with_policy(1, "a", AfterAction::Stay, ExecutionMode::Await, None),
                    binding_with_policy(
                        2,
                        "r",
                        AfterAction::Stay,
                        ExecutionMode::Await,
                        Some(muxe_protocol::LocalMenuActionWire::Control(
                            MenuControl::Return,
                        )),
                    ),
                ],
                Some(10),
            ),
            at(0),
        )
        .expect("attachment attaches");
        let execution = muxe_protocol::ExecutionId([1; 16]);

        assert_eq!(
            runtime
                .handle_input_at(&press('a'), at(1))
                .expect("binding invokes"),
            UiCommand::Invoke {
                generation: 7,
                binding: BindingId {
                    generation: 7,
                    ordinal: 1,
                },
            }
        );
        assert_eq!(
            runtime
                .invocation_accepted(
                    BindingId {
                        generation: 7,
                        ordinal: 1,
                    },
                    execution,
                    InvocationDisposition::Await,
                    at(2),
                )
                .expect("awaited action is staged"),
            UiCommand::Ignored
        );
        assert_eq!(runtime.inactivity_deadline(), None);
        assert_eq!(
            runtime
                .handle_input_at(&press('x'), at(3))
                .expect("pending input is swallowed"),
            UiCommand::Ignored
        );
        assert_eq!(
            runtime
                .handle_input_at(&press('r'), at(4))
                .expect("pending return is broker-controlled"),
            UiCommand::MenuControl(MenuControl::Return)
        );
        assert_eq!(
            runtime
                .handle_broker_event(
                    &BrokerEvent::ExecutionCompleted {
                        session: UiSessionId::new("ui"),
                        execution: muxe_protocol::ExecutionId([2; 16]),
                        outcome: muxe_protocol::ExecutionOutcome::Succeeded,
                        diagnostic: None,
                    },
                    at(5),
                )
                .expect("mismatched nonce is stale"),
            UiCommand::Ignored
        );
        assert_eq!(
            runtime.pending_control_completed(&execution, MenuControl::Return, at(6)),
            UiCommand::Detach
        );
    }

    #[test]
    fn accepted_actions_apply_their_archived_after_action_only_on_the_matching_lifecycle_event() {
        let binding = BindingId {
            generation: 7,
            ordinal: 1,
        };
        let mut awaited = UiRuntime::attach_at(
            profiled_attachment(
                KeyboardProfileWire::Vt100 {
                    escape_timeout_millis: 25,
                },
                vec![binding_with_policy(
                    1,
                    "a",
                    AfterAction::Quit,
                    ExecutionMode::Await,
                    None,
                )],
            ),
            at(0),
        )
        .expect("awaited attachment attaches");
        let awaited_execution = muxe_protocol::ExecutionId([3; 16]);
        assert!(matches!(
            awaited.handle_input_at(&press('a'), at(1)),
            Ok(UiCommand::Invoke { .. })
        ));
        assert_eq!(
            awaited
                .invocation_accepted(
                    binding,
                    awaited_execution,
                    InvocationDisposition::Await,
                    at(2),
                )
                .expect("awaited action is staged"),
            UiCommand::Ignored
        );
        assert_eq!(
            awaited
                .handle_broker_event(
                    &BrokerEvent::ExecutionCompleted {
                        session: UiSessionId::new("ui"),
                        execution: awaited_execution,
                        outcome: muxe_protocol::ExecutionOutcome::Succeeded,
                        diagnostic: None,
                    },
                    at(3),
                )
                .expect("matching completion applies after-action"),
            UiCommand::Detach
        );

        let mut detached = UiRuntime::attach_at(
            profiled_attachment(
                KeyboardProfileWire::Vt100 {
                    escape_timeout_millis: 25,
                },
                vec![binding_with_policy(
                    1,
                    "a",
                    AfterAction::Quit,
                    ExecutionMode::Detach,
                    None,
                )],
            ),
            at(0),
        )
        .expect("detached attachment attaches");
        assert!(matches!(
            detached.handle_input_at(&press('a'), at(1)),
            Ok(UiCommand::Invoke { .. })
        ));
        assert_eq!(
            detached
                .invocation_accepted(
                    binding,
                    muxe_protocol::ExecutionId([4; 16]),
                    InvocationDisposition::Detached,
                    at(2),
                )
                .expect("detached disposition applies immediately"),
            UiCommand::Detach
        );
    }

    #[test]
    fn dismissed_creation_ignores_stay_and_return_after_actions() {
        for after_action in [AfterAction::Stay, AfterAction::Return] {
            let mut runtime = UiRuntime::attach_at(
                profiled_attachment(
                    KeyboardProfileWire::Vt100 {
                        escape_timeout_millis: 25,
                    },
                    vec![binding_with_policy(
                        1,
                        "a",
                        after_action,
                        ExecutionMode::Await,
                        None,
                    )],
                ),
                at(0),
            )
            .expect("attachment attaches");
            let binding = BindingId {
                generation: 7,
                ordinal: 1,
            };
            assert!(matches!(
                runtime.handle_input_at(&press('a'), at(1)),
                Ok(UiCommand::Invoke { .. })
            ));
            assert_eq!(
                runtime
                    .invocation_accepted(
                        binding,
                        muxe_protocol::ExecutionId([5; 16]),
                        InvocationDisposition::Dismissed,
                        at(2),
                    )
                    .expect("dismissed creation is accepted"),
                UiCommand::Detach
            );
        }
    }

    #[test]
    fn terminal_resize_resets_a_paginated_menu_to_its_first_page() {
        let bindings = (1..=8)
            .map(|ordinal| {
                binding(
                    ordinal,
                    &format!("f{ordinal}"),
                    &format!("Binding {ordinal}"),
                    BindingConditionsWire::default(),
                    None,
                )
            })
            .collect();
        let mut runtime = UiRuntime::attach(profiled_attachment(
            KeyboardProfileWire::Vt100 {
                escape_timeout_millis: 25,
            },
            bindings,
        ))
        .expect("attachment attaches");

        let first = runtime
            .prepare(Rect::new(0, 0, 12, 3))
            .expect("small terminal renders");
        assert!(first.plan.page_count > 1);
        runtime.current_page = 1;
        assert_eq!(
            runtime
                .prepare(Rect::new(0, 0, 12, 3))
                .expect("same terminal preserves the selected page")
                .page,
            1
        );
        assert_eq!(
            runtime
                .prepare(Rect::new(0, 0, 12, 4))
                .expect("resized terminal renders")
                .page,
            0
        );
    }

    #[test]
    fn vt100_aliases_use_one_unmodified_parser_event_for_each_binding_form() {
        let bindings = [
            ("tab", b"\t".as_slice()),
            ("ctrl+i", b"\t".as_slice()),
            ("enter", b"\r".as_slice()),
            ("ctrl+m", b"\r".as_slice()),
            ("backspace", b"\x08".as_slice()),
            ("ctrl+h", b"\x08".as_slice()),
            ("esc", b"\x1b".as_slice()),
            ("ctrl+[", b"\x1b".as_slice()),
            ("ctrl+a", b"\x01".as_slice()),
            ("ctrl+shift+a", b"\x01".as_slice()),
        ]
        .into_iter()
        .enumerate()
        .map(|(ordinal, (key, bytes))| {
            (
                binding(
                    ordinal as u64,
                    key,
                    key,
                    BindingConditionsWire::default(),
                    None,
                ),
                bytes,
            )
        })
        .collect::<Vec<_>>();

        for (ordinal, (_, bytes)) in bindings.iter().enumerate() {
            let mut runtime = UiRuntime::attach(profiled_attachment(
                KeyboardProfileWire::Vt100 {
                    escape_timeout_millis: 25,
                },
                vec![bindings[ordinal].0.clone()],
            ))
            .expect("VT100 attachment attaches");
            runtime
                .prepare(Rect::new(0, 0, 40, 8))
                .expect("VT100 attachment renders");

            assert_eq!(
                runtime
                    .handle_input(&parsed_input(bytes))
                    .expect("single parser event selects the configured alias"),
                UiCommand::Invoke {
                    generation: 7,
                    binding: BindingId {
                        generation: 7,
                        ordinal: ordinal as u64,
                    },
                }
            );
        }
    }

    #[test]
    fn kitty_preserves_tab_and_control_i_as_distinct_parser_events() {
        let mut runtime = UiRuntime::attach(profiled_attachment(
            KeyboardProfileWire::Kitty(KeyCapabilitiesWire {
                event_types: true,
                alternate_keys: true,
                all_keys_as_escape_codes: false,
            }),
            vec![
                binding(1, "tab", "Tab", BindingConditionsWire::default(), None),
                binding(
                    2,
                    "ctrl+i",
                    "Control I",
                    BindingConditionsWire::default(),
                    None,
                ),
            ],
        ))
        .expect("Kitty attachment attaches");
        runtime
            .prepare(Rect::new(0, 0, 40, 8))
            .expect("Kitty attachment renders");

        assert_eq!(
            runtime
                .handle_input(&parsed_input(b"\x1b[9;1u"))
                .expect("Kitty Tab matches Tab only"),
            UiCommand::Invoke {
                generation: 7,
                binding: BindingId {
                    generation: 7,
                    ordinal: 1,
                },
            }
        );
        assert_eq!(
            runtime
                .handle_input(&parsed_input(b"\x1b[105;5u"))
                .expect("Kitty control-I matches control-I only"),
            UiCommand::Invoke {
                generation: 7,
                binding: BindingId {
                    generation: 7,
                    ordinal: 2,
                },
            }
        );
    }

    #[test]
    fn checked_broker_archive_drives_templates_conditions_navigation_and_invocation() {
        let mut runtime = UiRuntime::attach(archived_attachment()).expect("archive attaches");

        assert_eq!(runtime.session_id().expect("session is borrowed"), "ui");
        assert_eq!(
            runtime.keyboard_profile().expect("profile is archived"),
            KeyboardProfile::Kitty(KeyCapabilities {
                event_types: true,
                alternate_keys: true,
                all_keys_as_escape_codes: false,
            })
        );
        let root = runtime
            .prepare(Rect::new(0, 0, 40, 8))
            .expect("root renders");
        assert_eq!(root.title, "Root");
        assert_eq!(root.cells.len(), 2);
        assert!(root.cells[0].plain.contains("Open"));
        assert_eq!(
            runtime.handle_input(&press('a')).expect("binding matches"),
            UiCommand::Invoke {
                generation: 7,
                binding: BindingId {
                    generation: 7,
                    ordinal: 1,
                },
            }
        );
        assert_eq!(
            runtime.handle_input(&press('n')).expect("open is local"),
            UiCommand::Redraw
        );
        let child = runtime
            .prepare(Rect::new(0, 0, 40, 8))
            .expect("child renders");
        assert_eq!(
            root.cells[0].spans[0].style.fg,
            Some(ratatui::style::Color::Reset)
        );
        assert_eq!(child.title, "Child");
        assert_eq!(
            runtime
                .handle_input(&press('r'))
                .expect("return is local with a caller"),
            UiCommand::Redraw
        );
        assert_eq!(
            runtime
                .prepare(Rect::new(0, 0, 40, 8))
                .expect("root rerenders")
                .title,
            "Root"
        );
    }
}
