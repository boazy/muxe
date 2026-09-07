use std::time::Duration;

use muxe_core::{
    AfterAction as CoreAfterAction, BindingConditions, ConditionIr, KeyboardProfile,
    LocalMenuAction, MenuControl as CoreMenuControl, MenuView, MenuViewMenu, ThemeSection,
    UiAttachmentView, ViewBindingSettings,
};
use muxe_protocol::{
    AfterAction, BindingConditionsWire, BindingId, BindingSettingsWire, BindingStateWire,
    BindingViewWire, ColorSchemeWire, CompiledThemeWire, ConditionIrWire, ExecutionMode,
    ExecutionPolicyWire, KeyCapabilitiesWire, KeyboardProfileWire, LayoutPaddingWire,
    LayoutSettingsWire, LocalMenuActionWire, MenuControl, MenuControlAction, MenuId,
    MenuViewMenuWire, MenuViewWire, NamedStringWire, NamedStyleWire, StyleWire, ThemeSectionWire,
    TimeoutAction, UiAttachmentWire,
};

pub fn attachment(view: &UiAttachmentView) -> UiAttachmentWire {
    UiAttachmentWire {
        menu: menu_view(&view.menu),
        keyboard: keyboard(&view.keyboard),
        inactivity_timeout_millis: view.inactivity_timeout.map(duration_millis),
        theme: theme(&view.theme),
    }
}

fn menu_view(view: &MenuView) -> MenuViewWire {
    MenuViewWire {
        generation: view.generation.0,
        root: MenuId::new(view.root.as_str()),
        menus: view.menus.iter().map(menu).collect(),
    }
}

fn menu(menu: &MenuViewMenu) -> MenuViewMenuWire {
    MenuViewMenuWire {
        id: MenuId::new(menu.id.as_str()),
        title: menu.title.clone(),
        layout: LayoutSettingsWire {
            padding: LayoutPaddingWire {
                left: menu.layout.padding.left,
                right: menu.layout.padding.right,
                top: menu.layout.padding.top,
                bottom: menu.layout.padding.bottom,
                between_rows: menu.layout.padding.between_rows,
                between_columns: menu.layout.padding.between_columns,
            },
            max_item_title_length: menu.layout.max_item_title_length,
        },
        bindings: menu
            .bindings
            .iter()
            .map(|binding| BindingViewWire {
                id: BindingId {
                    generation: binding.id.generation().0,
                    ordinal: binding.id.ordinal(),
                },
                key: binding.key.to_string(),
                label: binding.label.clone(),
                hidden: binding.hidden,
                state: BindingStateWire {
                    included: binding.state.included,
                    enabled: binding.state.enabled,
                    shown: binding.state.shown,
                    blocked: binding.state.blocked,
                },
                settings: settings(&binding.settings),
                conditions: conditions(&binding.conditions),
                local_menu_action: binding.local_menu_action.as_ref().map(local_action),
                diagnostic: None,
            })
            .collect(),
    }
}

fn settings(settings: &ViewBindingSettings) -> BindingSettingsWire {
    BindingSettingsWire {
        after_action: match settings.after_action {
            CoreAfterAction::Quit => AfterAction::Quit,
            CoreAfterAction::Return => AfterAction::Return,
            CoreAfterAction::Stay => AfterAction::Stay,
        },
        execution: ExecutionPolicyWire {
            mode: match settings.execution.mode {
                muxe_core::ExecutionMode::Await => ExecutionMode::Await,
                muxe_core::ExecutionMode::Detach => ExecutionMode::Detach,
            },
            timeout_millis: settings.execution.timeout.map(duration_millis),
            on_timeout: match settings.execution.on_timeout {
                muxe_core::TimeoutAction::Detach => TimeoutAction::Detach,
                muxe_core::TimeoutAction::Cancel => TimeoutAction::Cancel,
            },
            on_menu_control: match settings.execution.on_menu_control {
                muxe_core::MenuControlAction::Detach => MenuControlAction::Detach,
                muxe_core::MenuControlAction::Cancel => MenuControlAction::Cancel,
            },
        },
        repeat: settings.repeat,
    }
}

fn conditions(conditions: &BindingConditions) -> BindingConditionsWire {
    BindingConditionsWire {
        include: conditions
            .include
            .as_ref()
            .map(|value| condition_to_wire(value.ir())),
        enable: conditions
            .enable
            .as_ref()
            .map(|value| condition_to_wire(value.ir())),
        show: conditions
            .show
            .as_ref()
            .map(|value| condition_to_wire(value.ir())),
    }
}

fn condition_to_wire(condition: &ConditionIr) -> ConditionIrWire {
    match condition {
        ConditionIr::Bool(value) => ConditionIrWire::Bool(*value),
        ConditionIr::Integer(value) => ConditionIrWire::Integer(*value),
        ConditionIr::PagesCount => ConditionIrWire::PagesCount,
        ConditionIr::PagesCurrent => ConditionIrWire::PagesCurrent,
        ConditionIr::Not(value) => ConditionIrWire::Not(Box::new(condition_to_wire(value))),
        ConditionIr::And(left, right) => ConditionIrWire::And(
            Box::new(condition_to_wire(left)),
            Box::new(condition_to_wire(right)),
        ),
        ConditionIr::Or(left, right) => ConditionIrWire::Or(
            Box::new(condition_to_wire(left)),
            Box::new(condition_to_wire(right)),
        ),
        ConditionIr::Equal(left, right) => ConditionIrWire::Equal(
            Box::new(condition_to_wire(left)),
            Box::new(condition_to_wire(right)),
        ),
        ConditionIr::NotEqual(left, right) => ConditionIrWire::NotEqual(
            Box::new(condition_to_wire(left)),
            Box::new(condition_to_wire(right)),
        ),
        ConditionIr::Less(left, right) => ConditionIrWire::Less(
            Box::new(condition_to_wire(left)),
            Box::new(condition_to_wire(right)),
        ),
        ConditionIr::LessEqual(left, right) => ConditionIrWire::LessEqual(
            Box::new(condition_to_wire(left)),
            Box::new(condition_to_wire(right)),
        ),
        ConditionIr::Greater(left, right) => ConditionIrWire::Greater(
            Box::new(condition_to_wire(left)),
            Box::new(condition_to_wire(right)),
        ),
        ConditionIr::GreaterEqual(left, right) => ConditionIrWire::GreaterEqual(
            Box::new(condition_to_wire(left)),
            Box::new(condition_to_wire(right)),
        ),
    }
}

fn local_action(action: &LocalMenuAction) -> LocalMenuActionWire {
    match action {
        LocalMenuAction::Open { target } => LocalMenuActionWire::Open {
            target: MenuId::new(target.as_str()),
        },
        LocalMenuAction::Control(CoreMenuControl::Quit) => {
            LocalMenuActionWire::Control(MenuControl::Quit)
        }
        LocalMenuAction::Control(CoreMenuControl::Return) => {
            LocalMenuActionWire::Control(MenuControl::Return)
        }
        LocalMenuAction::PagePrevious => LocalMenuActionWire::PagePrevious,
        LocalMenuAction::PageNext => LocalMenuActionWire::PageNext,
    }
}

fn keyboard(profile: &KeyboardProfile) -> KeyboardProfileWire {
    match profile {
        KeyboardProfile::Vt100 { escape_timeout } => KeyboardProfileWire::Vt100 {
            escape_timeout_millis: duration_millis(*escape_timeout),
        },
        KeyboardProfile::Kitty(capabilities) => KeyboardProfileWire::Kitty(KeyCapabilitiesWire {
            event_types: capabilities.event_types,
            alternate_keys: capabilities.alternate_keys,
            all_keys_as_escape_codes: capabilities.all_keys_as_escape_codes,
        }),
    }
}

fn theme(value: &muxe_core::CompiledTheme) -> CompiledThemeWire {
    CompiledThemeWire {
        common: theme_section(&value.theme.common),
        menu: theme_section(&value.theme.menu),
        settings: strings(&value.theme.settings),
        scheme: ColorSchemeWire {
            title: value.scheme.title.clone(),
            palette: strings(&value.scheme.palette),
            colors: strings(&value.scheme.colors),
        },
    }
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

fn strings(values: &std::collections::BTreeMap<String, String>) -> Vec<NamedStringWire> {
    values
        .iter()
        .map(|(name, value)| NamedStringWire {
            name: name.clone(),
            value: value.clone(),
        })
        .collect()
}

fn duration_millis(value: Duration) -> u64 {
    u64::try_from(value.as_millis()).unwrap_or(u64::MAX)
}
