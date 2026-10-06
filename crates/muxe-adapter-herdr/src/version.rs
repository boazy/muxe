//! Herdr release numbers and the minimum release this adapter supports.
//!
//! The adapter gates compatibility on the live server's release only. Herdr's
//! numbered binary protocol serves same-install terminal attach and live
//! handoff; Herdr tells JSON API clients to ignore unknown fields and to treat
//! unsupported methods as ordinary errors, so the protocol number is never a
//! compatibility bound here.

use std::fmt;

/// One Herdr release number, `MAJOR.MINOR.PATCH`.
///
/// Ordering compares the components numerically, matching Herdr's own
/// `min_herdr_version` check. Herdr compares that minimum against its base
/// release and ignores the build channel, so a preview build of a release
/// satisfies the same minimum as the stable build.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct HerdrRelease {
    major: u32,
    minor: u32,
    patch: u32,
}

impl HerdrRelease {
    /// Oldest Herdr release whose socket API this adapter supports.
    pub const MINIMUM_SUPPORTED: Self = Self::new(0, 8, 2);

    #[must_use]
    pub const fn new(major: u32, minor: u32, patch: u32) -> Self {
        Self {
            major,
            minor,
            patch,
        }
    }

    /// Parses exactly three dot-separated decimal components.
    #[must_use]
    pub(crate) fn parse(text: &str) -> Option<Self> {
        let mut parts = text.split('.');
        let release = Self::new(
            component(parts.next()?)?,
            component(parts.next()?)?,
            component(parts.next()?)?,
        );
        parts.next().is_none().then_some(release)
    }
}

impl fmt::Display for HerdrRelease {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}.{}.{}", self.major, self.minor, self.patch)
    }
}

fn component(text: &str) -> Option<u32> {
    if text.is_empty() || !text.bytes().all(|byte| byte.is_ascii_digit()) {
        return None;
    }
    text.parse().ok()
}

/// The version string reported by the establishing pong of one runtime epoch,
/// with its parsed release.
///
/// Herdr reports its base release for stable builds and appends
/// `-<channel>[.<build>]` for other channels, for example `0.9.3-preview.42`.
/// Consumers never recover this value by parsing the opaque live-server
/// identity.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HerdrServerVersion {
    reported: String,
    release: HerdrRelease,
}

impl HerdrServerVersion {
    /// Parses a reported version: a release, optionally followed by a
    /// nonempty `-` channel suffix.
    #[must_use]
    pub(crate) fn parse(reported: &str) -> Option<Self> {
        let base = match reported.split_once('-') {
            Some((base, channel)) if !channel.is_empty() => base,
            Some(_) => return None,
            None => reported,
        };
        let release = HerdrRelease::parse(base)?;
        Some(Self {
            reported: reported.to_owned(),
            release,
        })
    }

    /// The exact reported version string.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.reported
    }

    /// The base release used for compatibility comparisons.
    #[must_use]
    pub const fn release(&self) -> HerdrRelease {
        self.release
    }
}

impl fmt::Display for HerdrServerVersion {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.reported.fmt(formatter)
    }
}
