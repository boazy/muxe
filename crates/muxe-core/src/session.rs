use std::time::Duration;

use crate::execution::{AfterAction, MenuControl};
use crate::menu::{BindingId, MenuId};

/// Caller-provided monotonic time for the pure menu state machine.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct SessionInstant(pub Duration);

impl SessionInstant {
    pub fn checked_add(self, duration: Duration) -> Option<Self> {
        self.0.checked_add(duration).map(Self)
    }

    #[must_use]
    pub fn saturating_duration_since(self, earlier: Self) -> Duration {
        self.0.saturating_sub(earlier.0)
    }
}

/// Broker-unique execution identity. Completion transitions must match it exactly.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ExecutionId(pub u64);

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum MenuSessionState {
    Active,
    Pending {
        execution: ExecutionId,
        requested_control: Option<MenuControl>,
    },
    Dismissed,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MenuSessionInput {
    Binding(BindingId),
    Unknown,
    Control(MenuControl),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum MenuSessionEvent {
    /// Every received key is represented explicitly so unknown keys reset inactivity.
    Key {
        at: SessionInstant,
        input: MenuSessionInput,
    },
    /// Broker accepted an awaitable action. The current inactivity deadline is paused.
    ActionPending {
        at: SessionInstant,
        execution: ExecutionId,
    },
    DetachedAccepted {
        at: SessionInstant,
        execution: ExecutionId,
        after_action: AfterAction,
    },
    /// Broker completed an awaited action. A completion for another execution is stale and ignored.
    ActionCompleted {
        at: SessionInstant,
        execution: ExecutionId,
        success: bool,
        after_action: AfterAction,
    },
    /// Broker accepted detach/cancel for a particular interrupted awaited action.
    PendingControlCompleted {
        at: SessionInstant,
        execution: ExecutionId,
        control: MenuControl,
    },
    /// The destination menu supplies the policy as soon as it becomes visible.
    OpenSubmenu {
        at: SessionInstant,
        menu: MenuId,
        timeout: Option<Duration>,
    },
    /// Explicit clock advance; the caller supplies this rather than the state machine reading time.
    Tick { at: SessionInstant },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum MenuSessionOutput {
    InvokeBinding(BindingId),
    UnknownKeySwallowed,
    PendingInputSwallowed,
    RequestPendingControl {
        execution: ExecutionId,
        control: MenuControl,
    },
    NavigatedTo(MenuId),
    ReturnedTo(MenuId),
    Dismissed,
}
/// One caller-stack level and its pinned inactivity policy.
#[derive(Clone, Debug)]
pub struct MenuFrame {
    menu: MenuId,
    timeout: Option<Duration>,
}

impl MenuFrame {
    #[must_use]
    pub fn menu(&self) -> &MenuId {
        &self.menu
    }
}

/// Pure caller-stack, inactivity, and pending-action state. It deliberately has no terminal,
/// host, IPC, clock, or action-payload dependency.
#[derive(Clone, Debug)]
pub struct MenuSession {
    stack: Vec<MenuFrame>,
    deadline: Option<SessionInstant>,
    paused_remaining: Option<Duration>,
    state: MenuSessionState,
}

impl MenuSession {
    #[must_use]
    pub fn new(root: MenuId, timeout: Option<Duration>, now: SessionInstant) -> Self {
        let deadline = timeout.and_then(|duration| now.checked_add(duration));
        Self {
            stack: vec![MenuFrame {
                menu: root,
                timeout,
            }],
            deadline,
            paused_remaining: None,
            state: MenuSessionState::Active,
        }
    }

    #[must_use]
    pub fn state(&self) -> &MenuSessionState {
        &self.state
    }

    #[must_use]
    pub fn current_menu(&self) -> Option<&MenuId> {
        self.stack.last().map(MenuFrame::menu)
    }

    #[must_use]
    pub fn stack(&self) -> &[MenuFrame] {
        &self.stack
    }

    #[must_use]
    pub fn deadline(&self) -> Option<SessionInstant> {
        self.deadline
    }

    pub fn handle(&mut self, event: MenuSessionEvent) -> Option<MenuSessionOutput> {
        match event {
            MenuSessionEvent::Key { at, input } => self.key(at, input),
            MenuSessionEvent::ActionPending { at, execution } => self.action_pending(at, execution),
            MenuSessionEvent::DetachedAccepted {
                at,
                execution: _,
                after_action,
            } => matches!(self.state, MenuSessionState::Active)
                .then(|| self.apply_after_action(after_action, at))
                .flatten(),
            MenuSessionEvent::ActionCompleted {
                at,
                execution,
                success,
                after_action,
            } => self.action_completed(at, execution, success, after_action),
            MenuSessionEvent::PendingControlCompleted {
                at,
                execution,
                control,
            } => self.pending_control_completed(at, execution, control),
            MenuSessionEvent::OpenSubmenu { at, menu, timeout } => {
                self.open_submenu(at, menu, timeout)
            }
            MenuSessionEvent::Tick { at } => self.tick(at),
        }
    }

    fn key(&mut self, at: SessionInstant, input: MenuSessionInput) -> Option<MenuSessionOutput> {
        match self.state {
            MenuSessionState::Dismissed => None,
            MenuSessionState::Pending {
                execution,
                requested_control,
            } => {
                // A received key always resets inactivity, even while its deadline is paused.
                self.reset_paused_timeout();
                match input {
                    MenuSessionInput::Control(control) if requested_control.is_none() => {
                        self.state = MenuSessionState::Pending {
                            execution,
                            requested_control: Some(control),
                        };
                        Some(MenuSessionOutput::RequestPendingControl { execution, control })
                    }
                    _ => Some(MenuSessionOutput::PendingInputSwallowed),
                }
            }
            MenuSessionState::Active => {
                self.reset_deadline(at);
                match input {
                    MenuSessionInput::Binding(binding) => {
                        Some(MenuSessionOutput::InvokeBinding(binding))
                    }
                    MenuSessionInput::Unknown => Some(MenuSessionOutput::UnknownKeySwallowed),
                    MenuSessionInput::Control(control) => Some(self.apply_control(control, at)),
                }
            }
        }
    }

    fn action_pending(
        &mut self,
        at: SessionInstant,
        execution: ExecutionId,
    ) -> Option<MenuSessionOutput> {
        if !matches!(self.state, MenuSessionState::Active) {
            return None;
        }
        self.paused_remaining = self
            .deadline
            .map(|deadline| deadline.saturating_duration_since(at));
        self.deadline = None;
        self.state = MenuSessionState::Pending {
            execution,
            requested_control: None,
        };
        None
    }

    fn action_completed(
        &mut self,
        at: SessionInstant,
        execution: ExecutionId,
        success: bool,
        after_action: AfterAction,
    ) -> Option<MenuSessionOutput> {
        let MenuSessionState::Pending {
            execution: current,
            requested_control,
        } = self.state
        else {
            return None;
        };
        if current != execution {
            return None;
        }
        // A user control request wins the race over ordinary post-action handling. The broker will
        // acknowledge that request through PendingControlCompleted.
        if requested_control.is_some() {
            return None;
        }
        self.state = MenuSessionState::Active;
        if success {
            if after_action == AfterAction::Stay {
                self.resume_deadline(at);
            } else {
                self.paused_remaining = None;
            }
            self.apply_after_action(after_action, at)
        } else {
            // Dispatch failure keeps this level visible and resets inactivity like a received key.
            self.paused_remaining = None;
            self.reset_deadline(at);
            None
        }
    }

    fn pending_control_completed(
        &mut self,
        at: SessionInstant,
        execution: ExecutionId,
        control: MenuControl,
    ) -> Option<MenuSessionOutput> {
        let MenuSessionState::Pending {
            execution: current,
            requested_control: Some(requested),
        } = self.state
        else {
            return None;
        };
        if current != execution || requested != control {
            return None;
        }
        self.paused_remaining = None;
        self.state = MenuSessionState::Active;
        Some(self.apply_control(control, at))
    }

    fn open_submenu(
        &mut self,
        at: SessionInstant,
        menu: MenuId,
        timeout: Option<Duration>,
    ) -> Option<MenuSessionOutput> {
        if !matches!(self.state, MenuSessionState::Active) {
            return None;
        }
        self.stack.push(MenuFrame {
            menu: menu.clone(),
            timeout,
        });
        self.reset_deadline(at);
        Some(MenuSessionOutput::NavigatedTo(menu))
    }

    fn tick(&mut self, at: SessionInstant) -> Option<MenuSessionOutput> {
        if matches!(self.state, MenuSessionState::Active)
            && self.deadline.is_some_and(|deadline| at >= deadline)
        {
            Some(self.dismiss())
        } else {
            None
        }
    }

    fn apply_after_action(
        &mut self,
        after_action: AfterAction,
        at: SessionInstant,
    ) -> Option<MenuSessionOutput> {
        match after_action {
            AfterAction::Quit => Some(self.dismiss()),
            AfterAction::Return => Some(self.apply_control(MenuControl::Return, at)),
            AfterAction::Stay => None,
        }
    }

    fn apply_control(&mut self, control: MenuControl, at: SessionInstant) -> MenuSessionOutput {
        match control {
            MenuControl::Quit => self.dismiss(),
            MenuControl::Return if self.stack.len() == 1 => self.dismiss(),
            MenuControl::Return => {
                self.stack.pop();
                self.reset_deadline(at);
                MenuSessionOutput::ReturnedTo(
                    self.stack
                        .last()
                        .expect("non-root return retains caller")
                        .menu
                        .clone(),
                )
            }
        }
    }

    fn dismiss(&mut self) -> MenuSessionOutput {
        self.deadline = None;
        self.paused_remaining = None;
        self.state = MenuSessionState::Dismissed;
        MenuSessionOutput::Dismissed
    }

    fn reset_deadline(&mut self, at: SessionInstant) {
        self.deadline = self
            .stack
            .last()
            .and_then(|frame| frame.timeout)
            .and_then(|duration| at.checked_add(duration));
    }

    fn reset_paused_timeout(&mut self) {
        self.paused_remaining = self.stack.last().and_then(|frame| frame.timeout);
    }

    fn resume_deadline(&mut self, at: SessionInstant) {
        self.deadline = self
            .paused_remaining
            .take()
            .and_then(|remaining| at.checked_add(remaining));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::menu::MenuName;

    fn expect_name(value: &str) -> MenuName {
        MenuName::parse(value).expect("valid test menu name")
    }

    fn at(milliseconds: u64) -> SessionInstant {
        SessionInstant(Duration::from_millis(milliseconds))
    }

    #[test]
    fn unknown_key_resets_inactivity_without_navigation() {
        let root = MenuId::named(expect_name("main"));
        let mut session = MenuSession::new(root.clone(), Some(Duration::from_secs(10)), at(0));
        assert_eq!(
            session.handle(MenuSessionEvent::Key {
                at: at(9_999),
                input: MenuSessionInput::Unknown
            }),
            Some(MenuSessionOutput::UnknownKeySwallowed)
        );
        assert_eq!(
            session.handle(MenuSessionEvent::Tick { at: at(10_000) }),
            None
        );
        assert_eq!(session.current_menu(), Some(&root));
    }

    #[test]
    fn return_uses_actual_caller_stack() {
        let root = MenuId::named(expect_name("root"));
        let mut session = MenuSession::new(root.clone(), None, at(0));
        assert!(matches!(
            session.handle(MenuSessionEvent::OpenSubmenu {
                at: at(1),
                menu: MenuId::named(expect_name("child")),
                timeout: None,
            }),
            Some(MenuSessionOutput::NavigatedTo(_))
        ));
        assert_eq!(
            session.handle(MenuSessionEvent::Key {
                at: at(2),
                input: MenuSessionInput::Control(MenuControl::Return),
            }),
            Some(MenuSessionOutput::ReturnedTo(root))
        );
    }

    #[test]
    fn stale_completion_cannot_finish_later_execution() {
        let mut session = MenuSession::new(
            MenuId::named(expect_name("root")),
            Some(Duration::from_secs(10)),
            at(0),
        );
        session.handle(MenuSessionEvent::ActionPending {
            at: at(1),
            execution: ExecutionId(2),
        });
        assert_eq!(
            session.handle(MenuSessionEvent::ActionCompleted {
                at: at(2),
                execution: ExecutionId(1),
                success: true,
                after_action: AfterAction::Quit,
            }),
            None
        );
        assert!(matches!(
            session.state(),
            MenuSessionState::Pending {
                execution: ExecutionId(2),
                ..
            }
        ));
    }

    #[test]
    fn detached_acceptance_applies_post_action_while_active() {
        let mut session = MenuSession::new(MenuId::named(expect_name("root")), None, at(0));
        assert_eq!(
            session.handle(MenuSessionEvent::DetachedAccepted {
                at: at(1),
                execution: ExecutionId(3),
                after_action: AfterAction::Quit,
            }),
            Some(MenuSessionOutput::Dismissed)
        );
    }

    #[test]
    fn pending_control_wins_completion_race_and_resets_paused_timer() {
        let mut session = MenuSession::new(
            MenuId::named(expect_name("root")),
            Some(Duration::from_secs(10)),
            at(0),
        );
        session.handle(MenuSessionEvent::ActionPending {
            at: at(1),
            execution: ExecutionId(4),
        });
        session.handle(MenuSessionEvent::Key {
            at: at(2),
            input: MenuSessionInput::Unknown,
        });
        assert_eq!(
            session.handle(MenuSessionEvent::Key {
                at: at(3),
                input: MenuSessionInput::Control(MenuControl::Quit),
            }),
            Some(MenuSessionOutput::RequestPendingControl {
                execution: ExecutionId(4),
                control: MenuControl::Quit,
            })
        );
        assert_eq!(
            session.handle(MenuSessionEvent::ActionCompleted {
                at: at(4),
                execution: ExecutionId(4),
                success: true,
                after_action: AfterAction::Stay,
            }),
            None
        );
        assert_eq!(
            session.handle(MenuSessionEvent::PendingControlCompleted {
                at: at(5),
                execution: ExecutionId(4),
                control: MenuControl::Quit,
            }),
            Some(MenuSessionOutput::Dismissed)
        );
    }
    #[test]
    fn navigation_installs_destination_policy_and_fresh_parent_interval() {
        let root = MenuId::named(expect_name("root"));
        let child = MenuId::named(expect_name("child"));
        let mut session = MenuSession::new(root.clone(), Some(Duration::from_millis(10)), at(0));
        assert_eq!(
            session.handle(MenuSessionEvent::OpenSubmenu {
                at: at(9),
                menu: child.clone(),
                timeout: None,
            }),
            Some(MenuSessionOutput::NavigatedTo(child))
        );
        assert_eq!(session.deadline(), None);
        assert_eq!(session.handle(MenuSessionEvent::Tick { at: at(100) }), None);
        assert_eq!(
            session.handle(MenuSessionEvent::Key {
                at: at(101),
                input: MenuSessionInput::Control(MenuControl::Return),
            }),
            Some(MenuSessionOutput::ReturnedTo(root))
        );
        assert_eq!(session.deadline(), Some(at(111)));
        assert_eq!(session.handle(MenuSessionEvent::Tick { at: at(110) }), None);
        assert_eq!(
            session.handle(MenuSessionEvent::Tick { at: at(111) }),
            Some(MenuSessionOutput::Dismissed)
        );
    }

    #[test]
    fn off_root_finite_child_and_awaited_return_restore_visible_policy() {
        let root = MenuId::named(expect_name("root"));
        let child = MenuId::named(expect_name("child"));
        let mut session = MenuSession::new(root.clone(), None, at(0));
        session.handle(MenuSessionEvent::OpenSubmenu {
            at: at(3),
            menu: child,
            timeout: Some(Duration::from_millis(5)),
        });
        assert_eq!(session.deadline(), Some(at(8)));
        session.handle(MenuSessionEvent::ActionPending {
            at: at(4),
            execution: ExecutionId(5),
        });
        assert_eq!(session.deadline(), None);
        assert_eq!(session.handle(MenuSessionEvent::Tick { at: at(100) }), None);
        session.handle(MenuSessionEvent::Key {
            at: at(101),
            input: MenuSessionInput::Unknown,
        });
        session.handle(MenuSessionEvent::ActionCompleted {
            at: at(102),
            execution: ExecutionId(5),
            success: true,
            after_action: AfterAction::Stay,
        });
        assert_eq!(session.deadline(), Some(at(107)));
        session.handle(MenuSessionEvent::ActionPending {
            at: at(103),
            execution: ExecutionId(6),
        });
        assert_eq!(
            session.handle(MenuSessionEvent::ActionCompleted {
                at: at(110),
                execution: ExecutionId(6),
                success: true,
                after_action: AfterAction::Return,
            }),
            Some(MenuSessionOutput::ReturnedTo(root))
        );
        assert_eq!(session.deadline(), None);
    }

    #[test]
    fn failed_await_rearms_full_visible_interval() {
        let mut session = MenuSession::new(
            MenuId::named(expect_name("root")),
            Some(Duration::from_millis(10)),
            at(0),
        );
        session.handle(MenuSessionEvent::ActionPending {
            at: at(4),
            execution: ExecutionId(7),
        });
        session.handle(MenuSessionEvent::ActionCompleted {
            at: at(100),
            execution: ExecutionId(7),
            success: false,
            after_action: AfterAction::Return,
        });
        assert_eq!(session.deadline(), Some(at(110)));
    }
    #[test]
    fn pending_return_ack_installs_parent_interval_after_completion_race() {
        let root = MenuId::named(expect_name("root"));
        let mut session = MenuSession::new(root.clone(), Some(Duration::from_millis(10)), at(0));
        session.handle(MenuSessionEvent::OpenSubmenu {
            at: at(1),
            menu: MenuId::named(expect_name("child")),
            timeout: None,
        });
        session.handle(MenuSessionEvent::ActionPending {
            at: at(2),
            execution: ExecutionId(8),
        });
        assert_eq!(
            session.handle(MenuSessionEvent::Key {
                at: at(3),
                input: MenuSessionInput::Control(MenuControl::Return),
            }),
            Some(MenuSessionOutput::RequestPendingControl {
                execution: ExecutionId(8),
                control: MenuControl::Return,
            })
        );
        assert_eq!(
            session.handle(MenuSessionEvent::ActionCompleted {
                at: at(4),
                execution: ExecutionId(8),
                success: true,
                after_action: AfterAction::Stay,
            }),
            None
        );
        assert_eq!(session.deadline(), None);
        assert_eq!(
            session.handle(MenuSessionEvent::PendingControlCompleted {
                at: at(25),
                execution: ExecutionId(8),
                control: MenuControl::Return,
            }),
            Some(MenuSessionOutput::ReturnedTo(root))
        );
        assert_eq!(session.deadline(), Some(at(35)));
    }
    #[test]
    fn zero_duration_is_immediately_due_and_distinct_from_off() {
        let root = MenuId::named(expect_name("root"));
        let mut immediate = MenuSession::new(root.clone(), Some(Duration::ZERO), at(7));
        assert_eq!(immediate.deadline(), Some(at(7)));
        assert_eq!(
            immediate.handle(MenuSessionEvent::Tick { at: at(7) }),
            Some(MenuSessionOutput::Dismissed)
        );
        let mut off = MenuSession::new(root, None, at(7));
        assert_eq!(off.deadline(), None);
        assert_eq!(off.handle(MenuSessionEvent::Tick { at: at(7) }), None);
    }
}
