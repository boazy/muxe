use std::{collections::BTreeMap, hint::black_box, time::Duration};

use muxe_core::{SessionInstant, ThemeSection, compiled_default_theme};
use muxe_protocol::{
    AfterAction, BindingConditionsWire, BindingId, BindingSettingsWire, BindingStateWire,
    BrokerResponse, ColorSchemeWire, CompiledThemeWire, ConnectionDecoder, ConnectionPolicy,
    ExecutionMode, ExecutionPolicyWire, HostKind, KeyboardProfileWire, LayoutPaddingWire,
    LayoutSettingsWire, LiveServerIdentity, MenuControlAction, MenuId, MenuViewMenuWire,
    MenuViewWire, NamedStringWire, NamedStyleWire, PeerRole, Prelude, RequestId, SchemaFingerprint,
    ServerId, StyleWire, ThemeSectionWire, TimeoutAction, UiAttachmentWire, UiSessionId, Welcome,
    WireMessage, encode_frame,
};
use muxe_terminal_input::{EventKind, InputEvent, KeyIdentity, LockState, Modifiers, RawKeyEvent};
use muxe_ui::{UiRuntime, convert_input};
use ratatui::layout::Rect;

const MENU_BINDINGS: [(&str, &str); 12] = [
    ("a", "Open"),
    ("b", "Build"),
    ("c", "Check"),
    ("d", "Deploy"),
    ("e", "Export"),
    ("f", "Format"),
    ("g", "Generate"),
    ("h", "Help"),
    ("i", "Inspect"),
    ("j", "Jump"),
    ("k", "Keep"),
    ("l", "List"),
];

fn main() {
    divan::main();
}

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

fn binding(ordinal: u64, key: &str, label: &str) -> muxe_protocol::BindingViewWire {
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
        settings: BindingSettingsWire {
            after_action: AfterAction::Stay,
            execution: ExecutionPolicyWire {
                mode: ExecutionMode::Await,
                timeout_millis: None,
                on_timeout: TimeoutAction::Detach,
                on_menu_control: MenuControlAction::Detach,
            },
            repeat: None,
        },
        conditions: BindingConditionsWire::default(),
        local_menu_action: None,
        diagnostic: None,
    }
}

fn frame_bytes(message: &WireMessage) -> Vec<u8> {
    let frame = encode_frame(message).expect("finite benchmark message encodes");
    let mut bytes = frame.prefix().to_vec();
    bytes.extend_from_slice(frame.payload());
    bytes
}

fn checked_benchmark_frame() -> muxe_protocol::ArchivedFrame {
    let bindings = MENU_BINDINGS
        .iter()
        .enumerate()
        .map(|(index, (key, label))| {
            binding(
                u64::try_from(index + 1).expect("finite benchmark ordinal fits"),
                key,
                label,
            )
        })
        .collect();
    let attachment = UiAttachmentWire {
        menu: MenuViewWire {
            generation: 7,
            root: MenuId::new("root"),
            menus: vec![MenuViewMenuWire {
                id: MenuId::new("root"),
                title: Some("Benchmark menu".into()),
                layout: LayoutSettingsWire {
                    padding: LayoutPaddingWire {
                        left: 1,
                        right: 1,
                        top: 0,
                        bottom: 0,
                        between_rows: 0,
                        between_columns: 3,
                    },
                    max_item_title_length: 24,
                },
                bindings,
            }],
        },
        keyboard: KeyboardProfileWire::Vt100 {
            escape_timeout_millis: 25,
        },
        inactivity_timeout_millis: None,
        theme: default_theme_wire(),
    };
    let mut decoder = ConnectionDecoder::new(ConnectionPolicy::client(
        PeerRole::Ui,
        SchemaFingerprint::application(),
    ));
    decoder
        .push(
            &Prelude::rkyv(PeerRole::Ui, SchemaFingerprint::application()).encode(),
            |_| {},
        )
        .expect("finite benchmark prelude decodes");
    let welcome = WireMessage::Welcome {
        request_id: RequestId([1; 16]),
        welcome: Welcome {
            broker_version: "benchmark".into(),
            live_server: LiveServerIdentity {
                host: HostKind::Herdr,
                discovery_key: "benchmark".into(),
                server_id: ServerId::new("benchmark"),
            },
            accepted_frame_len: muxe_protocol::MAX_FRAME_LEN,
        },
    };
    decoder
        .push(&frame_bytes(&welcome), |_| {})
        .expect("finite benchmark welcome decodes");
    let response = WireMessage::Response {
        request_id: RequestId([2; 16]),
        response: BrokerResponse::UiAttached {
            session: UiSessionId::new("benchmark"),
            snapshot: attachment,
        },
    };
    let mut output = None;
    decoder
        .push(&frame_bytes(&response), |frame| output = Some(frame))
        .expect("finite benchmark attachment decodes");
    output.expect("finite benchmark frame is archived")
}

fn key_a() -> muxe_ui::ConvertedInput {
    convert_input(InputEvent::Key(RawKeyEvent {
        primary: KeyIdentity::Unicode('a'),
        shifted: None,
        base: None,
        modifiers: Modifiers::NONE,
        kind: EventKind::Press,
        locks: LockState::NONE,
        keypad: None,
    }))
}

#[divan::bench]
fn attach_and_prepare_menu(bencher: divan::Bencher<'_, '_>) {
    bencher
        .with_inputs(checked_benchmark_frame)
        .bench_values(|frame| {
            let mut runtime = UiRuntime::attach(frame).expect("checked attachment attaches");
            black_box(
                runtime
                    .prepare(Rect::new(0, 0, 80, 24))
                    .expect("finite menu prepares"),
            );
        });
}

fn prepared_binding_input() -> (UiRuntime, muxe_ui::ConvertedInput) {
    let mut runtime =
        UiRuntime::attach(checked_benchmark_frame()).expect("checked attachment attaches");
    runtime
        .prepare(Rect::new(0, 0, 80, 24))
        .expect("finite menu prepares");
    (runtime, key_a())
}

#[divan::bench]
fn match_menu_binding(bencher: divan::Bencher<'_, '_>) {
    bencher
        .with_inputs(prepared_binding_input)
        .bench_refs(|(runtime, key)| {
            black_box(
                runtime
                    .handle_input_at(key, SessionInstant(Duration::from_millis(1)))
                    .expect("finite binding matches"),
            );
        });
}
