use std::{fmt, str::FromStr};

/// A terminal-cell count or a percentage of the containing terminal area.
/// Parsing preserves the CLI's lexical grammar; placement validation owns bounds.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Dimension {
    Cells(u16),
    Percent(u16),
}

impl Dimension {
    /// The common portable size domain. Hosts impose additional placement bounds.
    #[must_use]
    pub const fn is_valid_size(self) -> bool {
        match self {
            Self::Cells(cells) => cells > 0,
            Self::Percent(percent) => percent > 0 && percent <= 100,
        }
    }
}

impl FromStr for Dimension {
    type Err = &'static str;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let (number, percent) = value
            .strip_suffix('%')
            .map_or((value, false), |number| (number, true));
        let number = number
            .parse()
            .map_err(|_| "dimension must be terminal cells or a percentage")?;
        Ok(if percent {
            Self::Percent(number)
        } else {
            Self::Cells(number)
        })
    }
}

impl fmt::Display for Dimension {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Cells(cells) => write!(formatter, "{cells}"),
            Self::Percent(percent) => write!(formatter, "{percent}%"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::Dimension;

    #[test]
    fn lexical_cli_grammar_is_separate_from_portable_size_bounds() {
        for (text, expected, valid) in [
            ("80%", Dimension::Percent(80), true),
            ("12", Dimension::Cells(12), true),
            ("1%", Dimension::Percent(1), true),
            ("100%", Dimension::Percent(100), true),
            ("0", Dimension::Cells(0), false),
            ("0%", Dimension::Percent(0), false),
            ("101%", Dimension::Percent(101), false),
            ("65535", Dimension::Cells(u16::MAX), true),
        ] {
            let parsed: Dimension = text.parse().unwrap();
            assert_eq!(parsed, expected);
            assert_eq!(parsed.is_valid_size(), valid);
            assert_eq!(parsed.to_string(), text);
        }
        for text in ["-1", "-1%", "65536", "65536%", "1.5%", "80%%", "80% "] {
            assert!(text.parse::<Dimension>().is_err(), "{text}");
        }
    }
}
