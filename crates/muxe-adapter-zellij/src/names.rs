//! Native-action namespace parsing and kebab-case mapping helpers.
//!
//! YAMLfacing names are lowercase kebab-case per the design: action variants,
//! enum values, and action fields. Generated raw mirrors use PascalCase variants
//! with snake_case fields, so this module owns the exact mechanical mapping
//! between the two. The mapping is total over the pinned inventory tables: an
//! unknown kebab name or field is a precise configuration error, never a guess.

use muxe_zellij_protocol::generated::{ACTION_VARIANTS, NATIVE_ZELLIJ_COMMANDS};

/// Namespace for low-level actions dispatched through `run_action`.
pub const ACTION_NAMESPACE: &str = "native.zellij.action:";
/// Namespace for the exposed high-level plugin commands.
pub const COMMAND_NAMESPACE: &str = "native.zellij.command:";

/// A parsed native-action discriminator.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum NativeType {
    /// Low-level action, kebab-case variant name (for example `new-tab`).
    Action(String),
    /// High-level plugin command, kebab-case function name.
    Command(String),
}

/// Splits a candidate type name into its namespace and kebab-case member.
///
/// Returns `None` for types outside both Zellij namespaces.
pub fn parse_native_type(type_name: &str) -> Option<NativeType> {
    if let Some(kebab) = type_name.strip_prefix(ACTION_NAMESPACE) {
        return Some(NativeType::Action(kebab.to_owned()));
    }
    if let Some(kebab) = type_name.strip_prefix(COMMAND_NAMESPACE) {
        return Some(NativeType::Command(kebab.to_owned()));
    }
    None
}

/// Whether a candidate type name belongs to either Zellij native namespace.
pub fn is_zellij_native_type(type_name: &str) -> bool {
    parse_native_type(type_name).is_some()
}

/// Whether a kebab-case command name is in the exposed v1 surface.
pub fn is_exposed_command(kebab: &str) -> bool {
    NATIVE_ZELLIJ_COMMANDS
        .iter()
        .any(|(name, _, _, _)| *name == kebab)
}

/// Converts a kebab-case action name to its PascalCase `Action` variant.
///
/// Returns `None` when no pinned variant matches, so callers fail closed with
/// the inventory table as the source of truth.
pub fn action_kebab_to_variant(kebab: &str) -> Option<&'static str> {
    ACTION_VARIANTS
        .iter()
        .find_map(|(variant, _)| (to_kebab(variant) == kebab).then_some(*variant))
}

/// Converts a PascalCase identifier to kebab-case (`NewTab` to `new-tab`).
pub fn to_kebab(name: &str) -> String {
    let mut kebab = String::with_capacity(name.len() + 4);
    for (index, character) in name.chars().enumerate() {
        if character.is_ascii_uppercase() {
            if index > 0 {
                kebab.push('-');
            }
            kebab.push(character.to_ascii_lowercase());
        } else {
            kebab.push(character);
        }
    }
    kebab
}

/// Converts a kebab-case field name to its snake_case mirror field.
pub fn field_to_snake(field: &str) -> String {
    field.replace('-', "_")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn namespaces_parse() {
        assert_eq!(
            parse_native_type("native.zellij.action:new-tab"),
            Some(NativeType::Action("new-tab".to_owned()))
        );
        assert_eq!(
            parse_native_type("native.zellij.command:close-focus"),
            Some(NativeType::Command("close-focus".to_owned()))
        );
        assert_eq!(parse_native_type("native.herdr.pane:resize"), None);
        assert_eq!(parse_native_type("tab:create"), None);
    }

    #[test]
    fn kebab_variant_round_trip_covers_inventory() {
        // Every pinned variant maps to a distinct kebab name and back.
        let mut seen = std::collections::BTreeSet::new();
        for (variant, _) in ACTION_VARIANTS {
            let kebab = to_kebab(variant);
            assert!(seen.insert(kebab.clone()), "duplicate kebab {kebab}");
            assert_eq!(action_kebab_to_variant(&kebab), Some(*variant));
        }
    }

    #[test]
    fn field_names_map_mechanically() {
        assert_eq!(
            field_to_snake("should-change-focus-to-new-tab"),
            "should_change_focus_to_new_tab"
        );
        assert_eq!(field_to_snake("pane-id"), "pane_id");
    }
}
