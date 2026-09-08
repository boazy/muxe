//! Client-scoped Zellij WASM bridge.
//!
//! One bridge instance runs per attached Zellij client. It owns client
//! identity, Locked-mode capture, origin snapshots, the tracked pane/tab
//! inventory, and typed dispatch of broker requests through the generated
//! conversion code. It links the same `muxe-zellij-protocol` payloads as the
//! native adapter so neither side can disagree on shapes.
//!
//! Source-only build: requesting permissions in code grants nothing by itself;
//! no live host is touched by building or testing this crate.

#![forbid(unsafe_code)]

mod bridge;
mod dispatcher;
mod focus;
mod outcome;

use bridge::{Bridge, ShimEffects};
use zellij_tile::prelude::*;

#[derive(Default)]
struct MuxeBridge {
    inner: Bridge,
    effects: ShimEffects,
}

register_plugin!(MuxeBridge);

impl ZellijPlugin for MuxeBridge {
    fn load(&mut self, _configuration: std::collections::BTreeMap<String, String>) {
        self.inner.load(&mut self.effects);
    }

    fn update(&mut self, event: Event) -> bool {
        self.inner.update(event, &mut self.effects);
        // Headless bridge: never requests a render.
        false
    }

    fn pipe(&mut self, message: PipeMessage) -> bool {
        // Borrow the two halves separately: the state machine drives effects.
        let (inner, effects) = (&mut self.inner, &mut self.effects);
        inner.pipe(message, effects);
        true
    }
}
