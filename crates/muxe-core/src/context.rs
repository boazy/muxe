use std::fmt;
use std::path::PathBuf;

use crate::diagnostic::{ConfigDiagnostic, DiagnosticCode, SourceSpan};

macro_rules! opaque_id {
    ($name:ident, $description:literal) => {
        #[doc = $description]
        #[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
        pub struct $name(String);

        impl $name {
            pub fn new(value: impl Into<String>) -> Self {
                Self(value.into())
            }

            pub fn as_str(&self) -> &str {
                &self.0
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str(&self.0)
            }
        }
    };
}

opaque_id!(ServerId, "Opaque live-host server identity.");
opaque_id!(ClientId, "Opaque host client identity.");
opaque_id!(SessionId, "Opaque host session identity.");
opaque_id!(WorkspaceId, "Opaque host workspace identity.");
opaque_id!(TabId, "Opaque host tab identity.");
opaque_id!(PaneId, "Opaque host pane identity.");
opaque_id!(WorktreeId, "Opaque host worktree identity.");
opaque_id!(AgentId, "Opaque host agent identity.");
opaque_id!(LinkHandlerId, "Opaque host link-handler identity.");

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum OriginHostKind {
    Zellij,
    Herdr,
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum OriginPaneType {
    Tiled,
    Floating,
    Plugin,
    Terminal,
    Other,
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum OriginInvocationSource {
    RootBinding,
    CommandLine,
    Link,
    Automation,
}

/// Immutable host state captured before a UI session can change focus.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OriginContext {
    pub host_kind: OriginHostKind,
    pub server_id: ServerId,
    pub client_id: Option<ClientId>,
    pub session_id: Option<SessionId>,
    pub workspace_id: Option<WorkspaceId>,
    pub tab_id: Option<TabId>,
    pub tab_index: Option<u64>,
    pub pane_id: Option<PaneId>,
    pub pane_type: Option<OriginPaneType>,
    pub pane_cwd: Option<PathBuf>,
    pub selection_text: Option<String>,
    pub invocation_source: OriginInvocationSource,
    pub worktree_id: Option<WorktreeId>,
    pub worktree_path: Option<PathBuf>,
    pub agent_id: Option<AgentId>,
    pub link_url: Option<String>,
    pub link_handler_id: Option<LinkHandlerId>,
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum ContextType {
    HostKind,
    ServerId,
    ClientId,
    SessionId,
    WorkspaceId,
    TabId,
    UnsignedInteger,
    PaneId,
    PaneType,
    AbsolutePath,
    String,
    InvocationSource,
    WorktreeId,
    AgentId,
    Url,
    LinkHandlerId,
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub enum ContextValue {
    HostKind(OriginHostKind),
    ServerId(ServerId),
    ClientId(ClientId),
    SessionId(SessionId),
    WorkspaceId(WorkspaceId),
    TabId(TabId),
    UnsignedInteger(u64),
    PaneId(PaneId),
    PaneType(OriginPaneType),
    AbsolutePath(PathBuf),
    String(String),
    InvocationSource(OriginInvocationSource),
    WorktreeId(WorktreeId),
    AgentId(AgentId),
    Url(String),
    LinkHandlerId(LinkHandlerId),
}

impl ContextValue {
    pub const fn context_type(&self) -> ContextType {
        match self {
            Self::HostKind(_) => ContextType::HostKind,
            Self::ServerId(_) => ContextType::ServerId,
            Self::ClientId(_) => ContextType::ClientId,
            Self::SessionId(_) => ContextType::SessionId,
            Self::WorkspaceId(_) => ContextType::WorkspaceId,
            Self::TabId(_) => ContextType::TabId,
            Self::UnsignedInteger(_) => ContextType::UnsignedInteger,
            Self::PaneId(_) => ContextType::PaneId,
            Self::PaneType(_) => ContextType::PaneType,
            Self::AbsolutePath(_) => ContextType::AbsolutePath,
            Self::String(_) => ContextType::String,
            Self::InvocationSource(_) => ContextType::InvocationSource,
            Self::WorktreeId(_) => ContextType::WorktreeId,
            Self::AgentId(_) => ContextType::AgentId,
            Self::Url(_) => ContextType::Url,
            Self::LinkHandlerId(_) => ContextType::LinkHandlerId,
        }
    }
}

/// The closed v1 context-reference registry.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum ContextPath {
    OriginHostKind,
    OriginServerId,
    OriginClientId,
    OriginSessionId,
    OriginWorkspaceId,
    OriginTabId,
    OriginTabIndex,
    OriginPaneId,
    OriginPaneType,
    OriginPaneCwd,
    OriginSelectionText,
    OriginInvocationSource,
    OriginWorktreeId,
    OriginWorktreePath,
    OriginAgentId,
    OriginLinkUrl,
    OriginLinkHandlerId,
}

impl ContextPath {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::OriginHostKind => "origin.host.kind",
            Self::OriginServerId => "origin.server.id",
            Self::OriginClientId => "origin.client.id",
            Self::OriginSessionId => "origin.session.id",
            Self::OriginWorkspaceId => "origin.workspace.id",
            Self::OriginTabId => "origin.tab.id",
            Self::OriginTabIndex => "origin.tab.index",
            Self::OriginPaneId => "origin.pane.id",
            Self::OriginPaneType => "origin.pane.type",
            Self::OriginPaneCwd => "origin.pane.cwd",
            Self::OriginSelectionText => "origin.selection.text",
            Self::OriginInvocationSource => "origin.invocation.source",
            Self::OriginWorktreeId => "origin.worktree.id",
            Self::OriginWorktreePath => "origin.worktree.path",
            Self::OriginAgentId => "origin.agent.id",
            Self::OriginLinkUrl => "origin.link.url",
            Self::OriginLinkHandlerId => "origin.link.handler.id",
        }
    }

    pub const fn value_type(self) -> ContextType {
        match self {
            Self::OriginHostKind => ContextType::HostKind,
            Self::OriginServerId => ContextType::ServerId,
            Self::OriginClientId => ContextType::ClientId,
            Self::OriginSessionId => ContextType::SessionId,
            Self::OriginWorkspaceId => ContextType::WorkspaceId,
            Self::OriginTabId => ContextType::TabId,
            Self::OriginTabIndex => ContextType::UnsignedInteger,
            Self::OriginPaneId => ContextType::PaneId,
            Self::OriginPaneType => ContextType::PaneType,
            Self::OriginPaneCwd => ContextType::AbsolutePath,
            Self::OriginSelectionText => ContextType::String,
            Self::OriginInvocationSource => ContextType::InvocationSource,
            Self::OriginWorktreeId => ContextType::WorktreeId,
            Self::OriginWorktreePath => ContextType::AbsolutePath,
            Self::OriginAgentId => ContextType::AgentId,
            Self::OriginLinkUrl => ContextType::Url,
            Self::OriginLinkHandlerId => ContextType::LinkHandlerId,
        }
    }

    pub fn parse(value: &str) -> Option<Self> {
        Some(match value {
            "origin.host.kind" => Self::OriginHostKind,
            "origin.server.id" => Self::OriginServerId,
            "origin.client.id" => Self::OriginClientId,
            "origin.session.id" => Self::OriginSessionId,
            "origin.workspace.id" => Self::OriginWorkspaceId,
            "origin.tab.id" => Self::OriginTabId,
            "origin.tab.index" => Self::OriginTabIndex,
            "origin.pane.id" => Self::OriginPaneId,
            "origin.pane.type" => Self::OriginPaneType,
            "origin.pane.cwd" => Self::OriginPaneCwd,
            "origin.selection.text" => Self::OriginSelectionText,
            "origin.invocation.source" => Self::OriginInvocationSource,
            "origin.worktree.id" => Self::OriginWorktreeId,
            "origin.worktree.path" => Self::OriginWorktreePath,
            "origin.agent.id" => Self::OriginAgentId,
            "origin.link.url" => Self::OriginLinkUrl,
            "origin.link.handler.id" => Self::OriginLinkHandlerId,
            _ => return None,
        })
    }

    pub fn resolve(self, origin: &OriginContext) -> Option<ContextValue> {
        match self {
            Self::OriginHostKind => Some(ContextValue::HostKind(origin.host_kind)),
            Self::OriginServerId => Some(ContextValue::ServerId(origin.server_id.clone())),
            Self::OriginClientId => origin.client_id.clone().map(ContextValue::ClientId),
            Self::OriginSessionId => origin.session_id.clone().map(ContextValue::SessionId),
            Self::OriginWorkspaceId => origin.workspace_id.clone().map(ContextValue::WorkspaceId),
            Self::OriginTabId => origin.tab_id.clone().map(ContextValue::TabId),
            Self::OriginTabIndex => origin.tab_index.map(ContextValue::UnsignedInteger),
            Self::OriginPaneId => origin.pane_id.clone().map(ContextValue::PaneId),
            Self::OriginPaneType => origin.pane_type.map(ContextValue::PaneType),
            Self::OriginPaneCwd => origin.pane_cwd.clone().map(ContextValue::AbsolutePath),
            Self::OriginSelectionText => origin.selection_text.clone().map(ContextValue::String),
            Self::OriginInvocationSource => Some(ContextValue::InvocationSource(origin.invocation_source)),
            Self::OriginWorktreeId => origin.worktree_id.clone().map(ContextValue::WorktreeId),
            Self::OriginWorktreePath => origin.worktree_path.clone().map(ContextValue::AbsolutePath),
            Self::OriginAgentId => origin.agent_id.clone().map(ContextValue::AgentId),
            Self::OriginLinkUrl => origin.link_url.clone().map(ContextValue::Url),
            Self::OriginLinkHandlerId => origin.link_handler_id.clone().map(ContextValue::LinkHandlerId),
        }
    }
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct ContextReference {
    pub path: ContextPath,
}

impl ContextReference {
    pub fn parse(value: &str, span: SourceSpan) -> Result<Self, ConfigDiagnostic> {
        ContextPath::parse(value)
            .map(|path| Self { path })
            .ok_or_else(|| {
                ConfigDiagnostic::error(
                    DiagnosticCode::InvalidContextReference,
                    format!("unknown context reference `{value}`"),
                    span,
                )
            })
    }

    pub fn expected_type(&self) -> ContextType {
        self.path.value_type()
    }

    pub fn resolve(&self, origin: &OriginContext) -> Option<ContextValue> {
        self.path.resolve(origin)
    }
}
