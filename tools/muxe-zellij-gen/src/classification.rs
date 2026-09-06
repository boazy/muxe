//! Explicit classifications for Zellij's public plugin shim surface.

use std::str::FromStr;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum FunctionClass {
    Exposed,
    Internal,
    Query,
    Unsupported,
}

impl FunctionClass {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Exposed => "exposed",
            Self::Internal => "internal",
            Self::Query => "query",
            Self::Unsupported => "unsupported",
        }
    }
}

impl FromStr for FunctionClass {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "exposed" => Ok(Self::Exposed),
            "internal" => Ok(Self::Internal),
            "query" => Ok(Self::Query),
            "unsupported" => Ok(Self::Unsupported),
            _ => Err(format!("unknown function classification {value:?}")),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum ConverterClass {
    Builtin,
    Mirror,
    Unsupported,
}

impl ConverterClass {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Builtin => "builtin",
            Self::Mirror => "mirror",
            Self::Unsupported => "unsupported",
        }
    }
}

impl FromStr for ConverterClass {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "builtin" => Ok(Self::Builtin),
            "mirror" => Ok(Self::Mirror),
            "unsupported" => Ok(Self::Unsupported),
            _ => Err(format!("unknown converter classification {value:?}")),
        }
    }
}
