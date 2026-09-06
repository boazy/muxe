use std::time::Duration;

use muxe_core::{CanonicalKey, EventKind, KeyCapabilities, KeyboardProfile};
use muxe_protocol::{
    evaluate_archived_binding_state, ArchivedKeyboardProfileWire, ArchivedLocalMenuActionWire,
    ArchivedMenuControl, ArchivedMenuViewMenuWire, BindingId, ConditionEvaluationErrorWire,
    MenuControl, PagesContextWire,
};
use ratatui::layout::Rect;
use thiserror::Error;
use unicode_width::UnicodeWidthStr;

use crate::{
    arrange_cells, sanitize_single_line, ArchivedUiSnapshot, BreadcrumbTemplate, Cell,
    CellTemplate, ConvertedInput, GridPlan, GridRect, MenuStatus, PaginationTemplate, RenderedText,
    SnapshotError, StatusTemplate, SurfaceFrame, SurfacePadding, TemplateError, TemplateRenderer,
};

/// A broker request or local redraw selected by a checked menu binding.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum UiCommand {
    /// The broker remains authoritative for this ordinary binding.
    Invoke { generation: u64, binding: BindingId },
    /// The host must send this control request to the broker.
    MenuControl(MenuControl),
    /// The UI consumed input by changing local menu or pager state.
    Redraw,
    /// The input does not select a visible, enabled binding.
    Ignored,
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
}

struct StatusMessage {
    level: String,
    message: String,
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
    current_menu: usize,
    menu_stack: Vec<usize>,
    current_page: usize,
    last_page_count: usize,
    status: Option<StatusMessage>,
}

impl UiRuntime {
    /// Attaches a checked broker response and compiles its supplied templates once.
    pub fn attach(frame: muxe_protocol::ArchivedFrame) -> Result<Self, UiError> {
        let snapshot = ArchivedUiSnapshot::new(frame)?;
        let renderer = snapshot
            .with_attachment(|attachment| TemplateRenderer::from_archived(&attachment.theme))??;
        let keyboard_profile = snapshot.with_attachment(keyboard_profile_from_attachment)?;
        let current_menu = snapshot.with_attachment(|attachment| {
            attachment
                .menu
                .menus
                .iter()
                .position(|menu| menu.id.0.as_str() == attachment.menu.root.0.as_str())
                .ok_or(UiError::MissingRoot)
        })??;
        snapshot.with_attachment(validate_binding_keys)??;
        Ok(Self {
            snapshot,
            renderer,
            keyboard_profile,
            current_menu,
            menu_stack: Vec::new(),
            current_page: 0,
            last_page_count: 1,
            status: None,
        })
    }

    /// Returns the checked broker session ID associated with this runtime.
    pub fn session_id(&self) -> Result<&str, UiError> {
        Ok(self.snapshot.session_id()?)
    }

    /// Returns the parser profile exactly supplied by the attached broker snapshot.
    pub fn keyboard_profile(&self) -> Result<KeyboardProfile, UiError> {
        Ok(self.keyboard_profile.clone())
    }

    /// Renders the current menu against the exact terminal rectangle before a surface redraw.
    pub fn prepare(&mut self, area: Rect) -> Result<PreparedMenu, UiError> {
        let current_menu = self.current_menu;
        let current_page = self.current_page;
        let last_page_count = self.last_page_count;
        let status = self.status.as_ref();
        let (prepared, page) = self.snapshot.with_attachment(|attachment| {
            let menu = attachment
                .menu
                .menus
                .get(current_menu)
                .ok_or(UiError::MissingMenu(current_menu))?;
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
                let cells = render_visible_cells(&self.renderer, menu, pages, max_title_width)?;
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
                    let breadcrumbs = render_breadcrumbs(
                        &self.renderer,
                        attachment,
                        &self.menu_stack,
                        current_menu,
                    )?;
                    let pager =
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
                            pager,
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

    /// Applies one converted key against the checked current menu.
    pub fn handle_input(&mut self, input: &ConvertedInput) -> Result<UiCommand, UiError> {
        let ConvertedInput::Key(input) = input else {
            return Ok(UiCommand::Ignored);
        };
        if input.event.kind == EventKind::Release {
            return Ok(UiCommand::Ignored);
        }
        let selection = self.snapshot.with_attachment(|attachment| {
            select_binding(
                attachment
                    .menu
                    .menus
                    .get(self.current_menu)
                    .ok_or(UiError::MissingMenu(self.current_menu))?,
                &self.keyboard_profile,
                input,
                self.current_page,
                self.last_page_count,
            )
        })??;
        match selection {
            Selection::Ignored => Ok(UiCommand::Ignored),
            Selection::Unavailable(message) => {
                self.status = Some(StatusMessage {
                    level: "blocked".into(),
                    message,
                });
                Ok(UiCommand::Redraw)
            }
            Selection::Open(target) => {
                let target_index = self.snapshot.with_attachment(|attachment| {
                    attachment
                        .menu
                        .menus
                        .iter()
                        .position(|menu| menu.id.0.as_str() == target)
                        .ok_or(UiError::MissingRoot)
                })??;
                self.menu_stack.push(self.current_menu);
                self.current_menu = target_index;
                self.current_page = 0;
                self.last_page_count = 1;
                self.status = None;
                Ok(UiCommand::Redraw)
            }
            Selection::Control(MenuControl::Return) => {
                if let Some(menu) = self.menu_stack.pop() {
                    self.current_menu = menu;
                    self.current_page = 0;
                    self.last_page_count = 1;
                    self.status = None;
                    Ok(UiCommand::Redraw)
                } else {
                    Ok(UiCommand::MenuControl(MenuControl::Return))
                }
            }
            Selection::Control(control) => Ok(UiCommand::MenuControl(control)),
            Selection::PagePrevious => {
                if self.current_page > 0 {
                    self.current_page -= 1;
                    self.status = None;
                    Ok(UiCommand::Redraw)
                } else {
                    Ok(UiCommand::Ignored)
                }
            }
            Selection::PageNext => {
                if self.current_page.saturating_add(1) < self.last_page_count {
                    self.current_page += 1;
                    self.status = None;
                    Ok(UiCommand::Redraw)
                } else {
                    Ok(UiCommand::Ignored)
                }
            }
            Selection::Invoke {
                generation,
                binding,
            } => {
                self.status = None;
                Ok(UiCommand::Invoke {
                    generation,
                    binding,
                })
            }
        }
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
) -> Result<Vec<RenderedText>, UiError> {
    let mut cells = Vec::new();
    for binding in menu.bindings.iter() {
        let state = evaluate_archived_binding_state(binding, pages)?;
        if !state.included || !state.shown || binding.hidden {
            continue;
        }
        cells.push(
            renderer.render_cell(CellTemplate {
                key: binding.key.as_str(),
                title: binding
                    .label
                    .as_ref()
                    .map(|label| label.as_str())
                    .unwrap_or_default(),
                disabled: !state.enabled,
                blocked: state.blocked,
                max_title_width,
            })?,
        );
    }
    Ok(cells)
}

fn render_breadcrumbs(
    renderer: &TemplateRenderer,
    attachment: &muxe_protocol::ArchivedUiAttachmentWire,
    stack: &[usize],
    current_menu: usize,
) -> Result<RenderedText, UiError> {
    let crumbs = stack
        .iter()
        .copied()
        .chain(core::iter::once(current_menu))
        .filter_map(|index| attachment.menu.menus.get(index))
        .filter_map(|menu| menu.title.as_ref().map(|title| title.as_str()))
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
    Invoke { generation: u64, binding: BindingId },
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
        if !state.enabled || state.blocked {
            return Ok(Selection::Unavailable(
                binding
                    .diagnostic
                    .as_ref()
                    .map(|diagnostic| diagnostic.message.as_str().to_owned())
                    .unwrap_or_else(|| "Binding is unavailable".into()),
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
            None => Selection::Invoke {
                generation: binding.id.generation.to_native(),
                binding: BindingId {
                    generation: binding.id.generation.to_native(),
                    ordinal: binding.id.ordinal.to_native(),
                },
            },
        });
    }
    Ok(Selection::Ignored)
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use muxe_core::{compiled_default_theme, ThemeSection};
    use muxe_protocol::{
        encode_frame, AfterAction, BindingConditionsWire, BindingSettingsWire, BindingStateWire,
        BrokerResponse, ColorSchemeWire, CompiledThemeWire, ConnectionDecoder, ConnectionPolicy,
        ExecutionMode, ExecutionPolicyWire, HostKind, KeyCapabilitiesWire, KeyboardProfileWire,
        LayoutPaddingWire, LayoutSettingsWire, LiveServerIdentity, MenuControlAction, MenuId,
        MenuViewMenuWire, MenuViewWire, NamedStringWire, NamedStyleWire, PeerRole, Prelude,
        RequestId, SchemaFingerprint, ServerId, StyleWire, ThemeSectionWire, TimeoutAction,
        UiAttachmentWire, UiSessionId, Welcome, WireMessage,
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
            inactivity_timeout_millis: None,
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
