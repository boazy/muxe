//! Tracked pane/tab inventory with bridge-resolved focus.
//!
//! The bridge subscribes to `PaneUpdate` (a manifest of ordered panes per tab
//! position with full geometry) and `TabUpdate` (positions with an active
//! flag). Two operations that have no pinned primitive resolve here:
//!
//! - Indexed focus selects the active tab's manifest-order pane. Manifest
//!   order is host-reported order; that semantic is documented on the request.
//! - Neighbor focus computes the nearest pane from a base pane's tracked
//!   geometry: strictly separated in the requested direction, ranked by edge
//!   overlap then center distance.

use std::collections::BTreeMap;

use muxe_zellij_protocol::NeighborDirection;

/// Geometry snapshot for one tracked pane.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PaneGeometry {
    /// Numeric pane ID within its kind.
    pub id: u32,
    /// True for plugin panes, false for terminals.
    pub is_plugin: bool,
    /// Left edge, in cells.
    pub x: usize,
    /// Top edge, in cells.
    pub y: usize,
    /// Width, in cells.
    pub columns: usize,
    /// Height, in cells.
    pub rows: usize,
}

impl PaneGeometry {
    /// Right edge, saturating.
    pub const fn right(self) -> usize {
        self.x.saturating_add(self.columns)
    }

    /// Bottom edge, saturating.
    pub const fn bottom(self) -> usize {
        self.y.saturating_add(self.rows)
    }

    /// Vertical overlap with another pane, in cells.
    fn overlap_y(self, other: Self) -> usize {
        let top = self.y.max(other.y);
        let bottom = self.bottom().min(other.bottom());
        bottom.saturating_sub(top)
    }

    /// Horizontal overlap with another pane, in cells.
    fn overlap_x(self, other: Self) -> usize {
        let left = self.x.max(other.x);
        let right = self.right().min(other.right());
        right.saturating_sub(left)
    }

    /// Squared center distance, for tie-breaking.
    fn distance2(self, other: Self) -> usize {
        let dx = self
            .x
            .saturating_add(self.columns / 2)
            .abs_diff(other.x + other.columns / 2);
        let dy = self
            .y
            .saturating_add(self.rows / 2)
            .abs_diff(other.y + other.rows / 2);
        dx.saturating_mul(dx).saturating_add(dy.saturating_mul(dy))
    }
}

/// Per-tab ordered pane inventory with the active tab position.
#[derive(Clone, Debug, Default)]
pub struct PaneInventory {
    panes: BTreeMap<usize, Vec<PaneGeometry>>,
    active_tab: Option<usize>,
}

impl PaneInventory {
    /// Empty inventory.
    pub fn new() -> Self {
        Self::default()
    }

    /// Replaces the tracked manifest wholesale from a `PaneUpdate` event.
    pub fn set_manifest(&mut self, panes: BTreeMap<usize, Vec<PaneGeometry>>) {
        self.panes = panes;
    }

    /// Records the active tab position from a `TabUpdate` event.
    pub fn set_active_tab(&mut self, position: Option<usize>) {
        self.active_tab = position;
    }

    /// Panes of the active tab in manifest order, if known.
    pub fn active_panes(&self) -> Option<&[PaneGeometry]> {
        self.active_tab
            .and_then(|tab| self.panes.get(&tab))
            .map(Vec::as_slice)
    }

    /// Resolves a manifest-order index of the active tab.
    pub fn pane_at(&self, index: u32) -> Option<PaneGeometry> {
        let position: usize = index.try_into().ok()?;
        self.active_panes()?.get(position).copied()
    }

    /// Finds a tracked pane by numeric ID and kind.
    pub fn find(&self, id: u32, is_plugin: bool) -> Option<PaneGeometry> {
        self.panes
            .values()
            .flatten()
            .find(|pane| pane.id == id && pane.is_plugin == is_plugin)
            .copied()
    }

    /// Computes the nearest neighbor of a base pane in one direction.
    ///
    /// Candidates must lie strictly beyond the base edge in that direction;
    /// ranking prefers edge overlap, then center distance. Returns `None` when
    /// no tracked pane qualifies.
    pub fn neighbor(
        &self,
        base: PaneGeometry,
        direction: NeighborDirection,
    ) -> Option<PaneGeometry> {
        let mut best: Option<(usize, usize, PaneGeometry)> = None;
        for candidate in self.panes.values().flatten() {
            if candidate.id == base.id && candidate.is_plugin == base.is_plugin {
                continue;
            }
            let separated = match direction {
                NeighborDirection::Left => candidate.right() <= base.x,
                NeighborDirection::Right => candidate.x >= base.right(),
                NeighborDirection::Up => candidate.bottom() <= base.y,
                NeighborDirection::Down => candidate.y >= base.bottom(),
            };
            if !separated {
                continue;
            }
            let overlap = match direction {
                NeighborDirection::Left | NeighborDirection::Right => base.overlap_y(*candidate),
                NeighborDirection::Up | NeighborDirection::Down => base.overlap_x(*candidate),
            };
            let distance = base.distance2(*candidate);
            let rank = (overlap, usize::MAX - distance);
            if best
                .is_none_or(|(best_overlap, best_inverse, _)| rank > (best_overlap, best_inverse))
            {
                best = Some((overlap, usize::MAX - distance, *candidate));
            }
        }
        best.map(|(_, _, pane)| pane)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pane(id: u32, x: usize, y: usize, columns: usize, rows: usize) -> PaneGeometry {
        PaneGeometry {
            id,
            is_plugin: false,
            x,
            y,
            columns,
            rows,
        }
    }

    fn inventory() -> PaneInventory {
        let mut inventory = PaneInventory::new();
        inventory.set_manifest(BTreeMap::from([(
            0,
            vec![
                pane(1, 0, 0, 50, 20),
                pane(2, 50, 0, 50, 20),
                pane(3, 0, 20, 100, 10),
            ],
        )]));
        inventory.set_active_tab(Some(0));
        inventory
    }

    #[test]
    fn index_selects_manifest_order() {
        let inventory = inventory();
        assert_eq!(inventory.pane_at(0).expect("first").id, 1);
        assert_eq!(inventory.pane_at(2).expect("third").id, 3);
        assert!(inventory.pane_at(3).is_none());
    }

    #[test]
    fn neighbor_prefers_overlap_then_distance() {
        let inventory = inventory();
        let base = pane(1, 0, 0, 50, 20);
        assert_eq!(
            inventory
                .neighbor(base, NeighborDirection::Right)
                .expect("right")
                .id,
            2
        );
        assert_eq!(
            inventory
                .neighbor(base, NeighborDirection::Down)
                .expect("down")
                .id,
            3
        );
        assert!(inventory.neighbor(base, NeighborDirection::Left).is_none());
        assert!(inventory.neighbor(base, NeighborDirection::Up).is_none());
    }

    #[test]
    fn touching_edges_do_not_count_as_separated() {
        // Pane 2 starts exactly where pane 1 ends: separated for Right.
        // A zero-width gap is still a clean split; an overlapping edge is not.
        let mut with_overlap = inventory();
        let overlapping = PaneGeometry {
            id: 9,
            is_plugin: false,
            x: 49,
            y: 0,
            columns: 10,
            rows: 20,
        };
        with_overlap.set_manifest(BTreeMap::from([(
            0,
            vec![pane(1, 0, 0, 50, 20), overlapping],
        )]));
        assert!(
            with_overlap
                .neighbor(pane(1, 0, 0, 50, 20), NeighborDirection::Right)
                .is_none()
        );
    }
}
