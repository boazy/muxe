//! One semantic definition for Muxe pager conditions.
//!
//! The compiler lowers the accepted CEL AST into [`ConditionIr`] once, the shared evaluator in
//! this crate executes it, and both the owned IR in `muxe-core` and the zero-copy archived wire
//! form in `muxe-protocol` evaluate through borrowed [`ConditionNode`] views. No second
//! short-circuit, comparison, or conversion implementation may live anywhere else.
//!
//! Conditions are closed Boolean expressions over `pages.count` / `pages.current` (1-based pager
//! geometry) and integer arithmetic. Compile-time type checking in `muxe-core` guarantees the
//! published tree is Boolean; the evaluator therefore still returns a fallible [`Result`] for the
//! runtime-only failures that remain possible (see [`ConditionEvaluationError`]).

use core::fmt;

/// 1-based pager geometry available to conditions.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PagesContext {
    pub count: u64,
    pub current: u64,
}

/// The single condition IR. Owned by the compiler; the wire crate mirrors it variant-for-variant.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ConditionIr {
    Bool(bool),
    Integer(i64),
    PagesCount,
    PagesCurrent,
    Not(Box<Self>),
    And(Box<Self>, Box<Self>),
    Or(Box<Self>, Box<Self>),
    Equal(Box<Self>, Box<Self>),
    NotEqual(Box<Self>, Box<Self>),
    Less(Box<Self>, Box<Self>),
    LessEqual(Box<Self>, Box<Self>),
    Greater(Box<Self>, Box<Self>),
    GreaterEqual(Box<Self>, Box<Self>),
    Add(Box<Self>, Box<Self>),
    Subtract(Box<Self>, Box<Self>),
    Multiply(Box<Self>, Box<Self>),
    Divide(Box<Self>, Box<Self>),
    Modulo(Box<Self>, Box<Self>),
    Negate(Box<Self>),
    Conditional(Box<Self>, Box<Self>, Box<Self>),
}

impl ConditionIr {
    /// Reports whether the tree references pager geometry.
    #[must_use]
    pub fn uses_pages(&self) -> bool {
        ConditionNode::uses_pages(self)
    }
}

/// Borrowed view over one condition node. Implemented by the owned [`ConditionIr`] and by the
/// rkyv-archived wire form so the single [`evaluate`] entry point covers both without cloning the
/// archived tree or allocating on the evaluation path.
#[derive(Clone, Copy, Debug)]
pub enum NodeRef<'a, V: ConditionNode + ?Sized> {
    Bool(bool),
    Integer(i64),
    PagesCount,
    PagesCurrent,
    Not(&'a V),
    And(&'a V, &'a V),
    Or(&'a V, &'a V),
    Equal(&'a V, &'a V),
    NotEqual(&'a V, &'a V),
    Less(&'a V, &'a V),
    LessEqual(&'a V, &'a V),
    Greater(&'a V, &'a V),
    GreaterEqual(&'a V, &'a V),
    Add(&'a V, &'a V),
    Subtract(&'a V, &'a V),
    Multiply(&'a V, &'a V),
    Divide(&'a V, &'a V),
    Modulo(&'a V, &'a V),
    Negate(&'a V),
    Conditional(&'a V, &'a V, &'a V),
}

/// Capability a node view must provide: expose itself as a borrowed [`NodeRef`] and recurse
/// through shared helpers. Every method has a default body over [`Self::view`]; implementors only
/// define `view`.
pub trait ConditionNode {
    fn view(&self) -> NodeRef<'_, Self>;

    fn uses_pages(&self) -> bool {
        match self.view() {
            NodeRef::PagesCount | NodeRef::PagesCurrent => true,
            NodeRef::Bool(_) | NodeRef::Integer(_) => false,
            NodeRef::Not(value) | NodeRef::Negate(value) => value.uses_pages(),
            NodeRef::And(left, right)
            | NodeRef::Or(left, right)
            | NodeRef::Equal(left, right)
            | NodeRef::NotEqual(left, right)
            | NodeRef::Less(left, right)
            | NodeRef::LessEqual(left, right)
            | NodeRef::Greater(left, right)
            | NodeRef::GreaterEqual(left, right)
            | NodeRef::Add(left, right)
            | NodeRef::Subtract(left, right)
            | NodeRef::Multiply(left, right)
            | NodeRef::Divide(left, right)
            | NodeRef::Modulo(left, right) => left.uses_pages() || right.uses_pages(),
            NodeRef::Conditional(condition, left, right) => {
                condition.uses_pages() || left.uses_pages() || right.uses_pages()
            }
        }
    }
}

impl ConditionNode for ConditionIr {
    fn view(&self) -> NodeRef<'_, Self> {
        match self {
            Self::Bool(value) => NodeRef::Bool(*value),
            Self::Integer(value) => NodeRef::Integer(*value),
            Self::PagesCount => NodeRef::PagesCount,
            Self::PagesCurrent => NodeRef::PagesCurrent,
            Self::Not(value) => NodeRef::Not(value),
            Self::And(left, right) => NodeRef::And(left, right),
            Self::Or(left, right) => NodeRef::Or(left, right),
            Self::Equal(left, right) => NodeRef::Equal(left, right),
            Self::NotEqual(left, right) => NodeRef::NotEqual(left, right),
            Self::Less(left, right) => NodeRef::Less(left, right),
            Self::LessEqual(left, right) => NodeRef::LessEqual(left, right),
            Self::Greater(left, right) => NodeRef::Greater(left, right),
            Self::GreaterEqual(left, right) => NodeRef::GreaterEqual(left, right),
            Self::Add(left, right) => NodeRef::Add(left, right),
            Self::Subtract(left, right) => NodeRef::Subtract(left, right),
            Self::Multiply(left, right) => NodeRef::Multiply(left, right),
            Self::Divide(left, right) => NodeRef::Divide(left, right),
            Self::Modulo(left, right) => NodeRef::Modulo(left, right),
            Self::Negate(value) => NodeRef::Negate(value),
            Self::Conditional(condition, left, right) => {
                NodeRef::Conditional(condition, left, right)
            }
        }
    }
}

/// Typed value produced while evaluating one node.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ConditionValue {
    Bool(bool),
    Integer(i64),
}

impl ConditionValue {
    fn boolean(self) -> Result<bool, ConditionEvaluationError> {
        match self {
            Self::Bool(value) => Ok(value),
            Self::Integer(_) => Err(ConditionEvaluationError::TypeMismatch),
        }
    }

    fn integer(self) -> Result<i64, ConditionEvaluationError> {
        match self {
            Self::Integer(value) => Ok(value),
            Self::Bool(_) => Err(ConditionEvaluationError::TypeMismatch),
        }
    }
}

/// Runtime-only evaluation failures. Compile-time type checking rejects every non-Boolean operand
/// before publication, so after a successful compile the only reachable variants are
/// [`ConditionEvaluationError::DivisionByZero`] and
/// [`ConditionEvaluationError::ArithmeticOverflow`]:
/// integer division/modulo by zero and `i64` overflow in `+`, `-`, `*`, unary `-`, or the
/// `pages.*` (`u64`) to `i64` narrowing.
///
/// The UI contract propagates these through `UiError::Condition` after terminal restoration; the
/// `Result` return type is therefore load-bearing and must stay fallible.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ConditionEvaluationError {
    /// A non-Boolean value reached a Boolean position (or vice versa). Only reachable for IR
    /// trees built without the compile-time type check (tests, hand-built wire frames).
    TypeMismatch,
    /// A published condition evaluated to an integer instead of a Boolean. Only reachable for IR
    /// trees built without the compile-time type check.
    NonBooleanResult,
    /// Integer division (`/`) or remainder (`%`) by zero. CEL has no `null`/error propagation in
    /// this IR, so this is a deterministic error classified separately from type errors.
    DivisionByZero,
    /// Signed `i64` overflow in `+`, `-`, `*`, unary `-`, or the `pages.*` (`u64`) to `i64`
    /// narrowing (`u64` values above `i64::MAX` fail evaluation with this error).
    ArithmeticOverflow,
}

impl fmt::Display for ConditionEvaluationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::TypeMismatch => formatter.write_str("condition expected a matching value type"),
            Self::NonBooleanResult => formatter.write_str("condition result must be boolean"),
            Self::DivisionByZero => formatter.write_str("condition divides by zero"),
            Self::ArithmeticOverflow => formatter.write_str("condition arithmetic overflowed"),
        }
    }
}

impl core::error::Error for ConditionEvaluationError {}

fn pages_count(pages: PagesContext) -> Result<i64, ConditionEvaluationError> {
    u64_to_i64(pages.count)
}

fn pages_current(pages: PagesContext) -> Result<i64, ConditionEvaluationError> {
    u64_to_i64(pages.current)
}

fn u64_to_i64(value: u64) -> Result<i64, ConditionEvaluationError> {
    i64::try_from(value).map_err(|_| ConditionEvaluationError::ArithmeticOverflow)
}

fn checked_add(left: i64, right: i64) -> Result<i64, ConditionEvaluationError> {
    left.checked_add(right)
        .ok_or(ConditionEvaluationError::ArithmeticOverflow)
}

fn checked_sub(left: i64, right: i64) -> Result<i64, ConditionEvaluationError> {
    left.checked_sub(right)
        .ok_or(ConditionEvaluationError::ArithmeticOverflow)
}

fn checked_mul(left: i64, right: i64) -> Result<i64, ConditionEvaluationError> {
    left.checked_mul(right)
        .ok_or(ConditionEvaluationError::ArithmeticOverflow)
}

fn checked_div(left: i64, right: i64) -> Result<i64, ConditionEvaluationError> {
    if right == 0 {
        return Err(ConditionEvaluationError::DivisionByZero);
    }
    left.checked_div(right)
        .ok_or(ConditionEvaluationError::ArithmeticOverflow)
}

fn checked_rem(left: i64, right: i64) -> Result<i64, ConditionEvaluationError> {
    if right == 0 {
        return Err(ConditionEvaluationError::DivisionByZero);
    }
    left.checked_rem(right)
        .ok_or(ConditionEvaluationError::ArithmeticOverflow)
}

fn checked_neg(value: i64) -> Result<i64, ConditionEvaluationError> {
    value
        .checked_neg()
        .ok_or(ConditionEvaluationError::ArithmeticOverflow)
}

fn evaluate_value<N: ConditionNode + ?Sized>(
    node: &N,
    pages: PagesContext,
) -> Result<ConditionValue, ConditionEvaluationError> {
    match node.view() {
        NodeRef::Bool(value) => Ok(ConditionValue::Bool(value)),
        NodeRef::Integer(value) => Ok(ConditionValue::Integer(value)),
        NodeRef::PagesCount => Ok(ConditionValue::Integer(pages_count(pages)?)),
        NodeRef::PagesCurrent => Ok(ConditionValue::Integer(pages_current(pages)?)),
        NodeRef::Not(value) => Ok(ConditionValue::Bool(
            !evaluate_value(value, pages)?.boolean()?,
        )),
        NodeRef::Negate(value) => Ok(ConditionValue::Integer(checked_neg(
            evaluate_value(value, pages)?.integer()?,
        )?)),
        NodeRef::And(left, right) => {
            // Short-circuit: a false left never evaluates the right side, so
            // `false && <division by zero>` is false rather than an error.
            if !evaluate_value(left, pages)?.boolean()? {
                return Ok(ConditionValue::Bool(false));
            }
            Ok(ConditionValue::Bool(
                evaluate_value(right, pages)?.boolean()?,
            ))
        }
        NodeRef::Or(left, right) => {
            // Short-circuit mirror: `true || <division by zero>` is true.
            if evaluate_value(left, pages)?.boolean()? {
                return Ok(ConditionValue::Bool(true));
            }
            Ok(ConditionValue::Bool(
                evaluate_value(right, pages)?.boolean()?,
            ))
        }
        NodeRef::Equal(left, right) => Ok(ConditionValue::Bool(
            evaluate_value(left, pages)? == evaluate_value(right, pages)?,
        )),
        NodeRef::NotEqual(left, right) => Ok(ConditionValue::Bool(
            evaluate_value(left, pages)? != evaluate_value(right, pages)?,
        )),
        NodeRef::Less(left, right) => compare(left, right, pages, |left, right| left < right),
        NodeRef::LessEqual(left, right) => compare(left, right, pages, |left, right| left <= right),
        NodeRef::Greater(left, right) => compare(left, right, pages, |left, right| left > right),
        NodeRef::GreaterEqual(left, right) => {
            compare(left, right, pages, |left, right| left >= right)
        }
        NodeRef::Add(left, right) => Ok(ConditionValue::Integer(checked_add(
            evaluate_value(left, pages)?.integer()?,
            evaluate_value(right, pages)?.integer()?,
        )?)),
        NodeRef::Subtract(left, right) => Ok(ConditionValue::Integer(checked_sub(
            evaluate_value(left, pages)?.integer()?,
            evaluate_value(right, pages)?.integer()?,
        )?)),
        NodeRef::Multiply(left, right) => Ok(ConditionValue::Integer(checked_mul(
            evaluate_value(left, pages)?.integer()?,
            evaluate_value(right, pages)?.integer()?,
        )?)),
        NodeRef::Divide(left, right) => Ok(ConditionValue::Integer(checked_div(
            evaluate_value(left, pages)?.integer()?,
            evaluate_value(right, pages)?.integer()?,
        )?)),
        NodeRef::Modulo(left, right) => Ok(ConditionValue::Integer(checked_rem(
            evaluate_value(left, pages)?.integer()?,
            evaluate_value(right, pages)?.integer()?,
        )?)),
        NodeRef::Conditional(condition, left, right) => {
            if evaluate_value(condition, pages)?.boolean()? {
                evaluate_value(left, pages)
            } else {
                evaluate_value(right, pages)
            }
        }
    }
}

fn compare<N: ConditionNode + ?Sized>(
    left: &N,
    right: &N,
    pages: PagesContext,
    predicate: impl FnOnce(i64, i64) -> bool,
) -> Result<ConditionValue, ConditionEvaluationError> {
    Ok(ConditionValue::Bool(predicate(
        evaluate_value(left, pages)?.integer()?,
        evaluate_value(right, pages)?.integer()?,
    )))
}

/// Evaluates one borrowed condition node. This is the single evaluator: the owned IR in
/// `muxe-core` and the archived wire form in `muxe-protocol` both delegate here, so
/// short-circuit, comparison, and conversion semantics exist exactly once.
///
/// # Errors
///
/// Returns [`ConditionEvaluationError::TypeMismatch`] when a mistyped subtree reaches an
/// operator, [`ConditionEvaluationError::NonBooleanResult`] when the root evaluates to an
/// integer, [`ConditionEvaluationError::DivisionByZero`] for integer division or remainder by
/// zero, and [`ConditionEvaluationError::ArithmeticOverflow`] for signed integer overflow or
/// narrowing a `pages.*` (`u64`) value to `i64`.
pub fn evaluate<N: ConditionNode + ?Sized>(
    node: &N,
    pages: PagesContext,
) -> Result<bool, ConditionEvaluationError> {
    match evaluate_value(node, pages)? {
        ConditionValue::Bool(value) => Ok(value),
        ConditionValue::Integer(_) => Err(ConditionEvaluationError::NonBooleanResult),
    }
}

/// Evaluates one borrowed condition subtree to its typed value. Exposed for parity checks that
/// compare owned and archived evaluation below the top-level Boolean gate.
///
/// # Errors
///
/// Returns [`ConditionEvaluationError::TypeMismatch`] when a mistyped subtree reaches an
/// operator, [`ConditionEvaluationError::DivisionByZero`] for integer division or remainder by
/// zero, and [`ConditionEvaluationError::ArithmeticOverflow`] for signed integer overflow or
/// narrowing a `pages.*` (`u64`) value to `i64`. It never returns
/// [`ConditionEvaluationError::NonBooleanResult`], which is emitted only by [`evaluate`] when its
/// root value is an integer.
pub fn evaluate_typed<N: ConditionNode + ?Sized>(
    node: &N,
    pages: PagesContext,
) -> Result<ConditionValue, ConditionEvaluationError> {
    evaluate_value(node, pages)
}

/// Static type of a condition subtree: every published condition must check as [`ConditionType::Boolean`].
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ConditionType {
    Boolean,
    Integer,
}

/// Type-checks one borrowed condition tree. Returns the type of the root; every operator enforces
/// its operand types, so `true && 1`, `1 < true`, `!pages.count`, arithmetic on Booleans, and a
/// top-level integer all fail here, before publication, instead of during evaluation.
///
/// # Errors
///
/// Returns [`TypeError`] when an operator receives an operand of the wrong type, comparison
/// operands have different types, or the two branches of `?:` have different types.
pub fn type_check<N: ConditionNode + ?Sized>(node: &N) -> Result<ConditionType, TypeError> {
    match node.view() {
        NodeRef::Bool(_) => Ok(ConditionType::Boolean),
        NodeRef::Integer(_) | NodeRef::PagesCount | NodeRef::PagesCurrent => {
            Ok(ConditionType::Integer)
        }
        NodeRef::Not(value) => {
            require_boolean(
                type_check(value)?,
                "operator `!` requires a boolean operand",
            )?;
            Ok(ConditionType::Boolean)
        }
        NodeRef::Negate(value) => {
            require_integer(type_check(value)?, "unary `-` requires an integer operand")?;
            Ok(ConditionType::Integer)
        }
        NodeRef::And(left, right) => {
            require_boolean(type_check(left)?, "operator `&&` requires boolean operands")?;
            require_boolean(
                type_check(right)?,
                "operator `&&` requires boolean operands",
            )?;
            Ok(ConditionType::Boolean)
        }
        NodeRef::Or(left, right) => {
            require_boolean(type_check(left)?, "operator `||` requires boolean operands")?;
            require_boolean(
                type_check(right)?,
                "operator `||` requires boolean operands",
            )?;
            Ok(ConditionType::Boolean)
        }
        NodeRef::Equal(left, right) | NodeRef::NotEqual(left, right) => {
            let left = type_check(left)?;
            let right = type_check(right)?;
            if left != right {
                return Err(TypeError::new(
                    "comparison `==`/`!=` requires both sides to have the same type",
                ));
            }
            Ok(ConditionType::Boolean)
        }
        NodeRef::Less(left, right)
        | NodeRef::LessEqual(left, right)
        | NodeRef::Greater(left, right)
        | NodeRef::GreaterEqual(left, right) => {
            require_integer(
                type_check(left)?,
                "ordering comparison requires integer operands",
            )?;
            require_integer(
                type_check(right)?,
                "ordering comparison requires integer operands",
            )?;
            Ok(ConditionType::Boolean)
        }
        NodeRef::Add(left, right)
        | NodeRef::Subtract(left, right)
        | NodeRef::Multiply(left, right)
        | NodeRef::Divide(left, right)
        | NodeRef::Modulo(left, right) => {
            require_integer(type_check(left)?, "arithmetic requires integer operands")?;
            require_integer(type_check(right)?, "arithmetic requires integer operands")?;
            Ok(ConditionType::Integer)
        }
        NodeRef::Conditional(condition, left, right) => {
            require_boolean(type_check(condition)?, "the `?:` condition must be boolean")?;
            let left = type_check(left)?;
            let right = type_check(right)?;
            if left != right {
                return Err(TypeError::new(
                    "the `?:` branches must both be boolean or both be integer",
                ));
            }
            Ok(left)
        }
    }
}

fn require_boolean(actual: ConditionType, message: &'static str) -> Result<(), TypeError> {
    if actual == ConditionType::Boolean {
        Ok(())
    } else {
        Err(TypeError::new(message))
    }
}

fn require_integer(actual: ConditionType, message: &'static str) -> Result<(), TypeError> {
    if actual == ConditionType::Integer {
        Ok(())
    } else {
        Err(TypeError::new(message))
    }
}

/// A compile-time type error: the message names the offending operator and the expected type.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct TypeError {
    message: &'static str,
}

impl TypeError {
    const fn new(message: &'static str) -> Self {
        Self { message }
    }

    #[must_use]
    pub const fn message(self) -> &'static str {
        self.message
    }
}

impl fmt::Display for TypeError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.message)
    }
}

impl core::error::Error for TypeError {}

#[cfg(test)]
mod tests {
    use super::*;

    fn pages(count: u64, current: u64) -> PagesContext {
        PagesContext { count, current }
    }

    #[test]
    fn short_circuit_skips_erroring_branches() {
        let erroring: ConditionIr = ConditionIr::Divide(
            Box::new(ConditionIr::Integer(1)),
            Box::new(ConditionIr::Integer(0)),
        );
        let and = ConditionIr::And(
            Box::new(ConditionIr::Bool(false)),
            Box::new(erroring.clone()),
        );
        assert_eq!(evaluate(&and, pages(1, 1)), Ok(false));
        let or = ConditionIr::Or(Box::new(ConditionIr::Bool(true)), Box::new(erroring));
        assert_eq!(evaluate(&or, pages(1, 1)), Ok(true));
    }

    #[test]
    fn division_by_zero_and_overflow_are_classified() {
        let div = ConditionIr::Divide(
            Box::new(ConditionIr::Integer(1)),
            Box::new(ConditionIr::Integer(0)),
        );
        assert_eq!(
            evaluate_typed(&div, pages(1, 1)),
            Err(ConditionEvaluationError::DivisionByZero)
        );
        let overflow = ConditionIr::Add(
            Box::new(ConditionIr::Integer(i64::MAX)),
            Box::new(ConditionIr::Integer(1)),
        );
        assert_eq!(
            evaluate_typed(&overflow, pages(1, 1)),
            Err(ConditionEvaluationError::ArithmeticOverflow)
        );
        let negate_min = ConditionIr::Negate(Box::new(ConditionIr::Integer(i64::MIN)));
        assert_eq!(
            evaluate_typed(&negate_min, pages(1, 1)),
            Err(ConditionEvaluationError::ArithmeticOverflow)
        );
    }

    #[test]
    fn type_check_rejects_mistyped_operands() {
        for tree in [
            ConditionIr::And(
                Box::new(ConditionIr::Bool(true)),
                Box::new(ConditionIr::Integer(1)),
            ),
            ConditionIr::Not(Box::new(ConditionIr::Integer(1))),
        ] {
            assert!(type_check(&tree).is_err());
            assert!(type_check(&tree).unwrap_err().message().len() > 8);
        }
        // A top-level integer checks as `Integer`; the compiler rejects it as a condition
        // because a published condition must be `Boolean`.
        assert_eq!(
            type_check(&ConditionIr::Integer(1)),
            Ok(ConditionType::Integer)
        );
    }

    #[test]
    fn conditional_evaluates_the_taken_branch_only() {
        let tree = ConditionIr::Conditional(
            Box::new(ConditionIr::Bool(false)),
            Box::new(ConditionIr::Divide(
                Box::new(ConditionIr::Integer(1)),
                Box::new(ConditionIr::Integer(0)),
            )),
            Box::new(ConditionIr::Bool(true)),
        );
        assert_eq!(evaluate(&tree, pages(1, 1)), Ok(true));
    }
}
