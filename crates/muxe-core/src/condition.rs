//! Lowered, type-checked condition programs for Muxe's closed pager CEL context.
//!
//! The compiler parses with the `cel` facade, lowers the accepted AST into the shared
//! [`muxe_condition::ConditionIr`] once, and type-checks the closed Boolean expression before
//! publication. UI snapshots carry the lowered IR and evaluate it through the single shared
//! evaluator; no UI/protocol participant reparses raw CEL source per render or at attachment time.

use cel::Program;
use cel::common::ast::{CallExpr, Expr, IdedExpr, LiteralValue, operators};
pub use muxe_condition::{ConditionEvaluationError, ConditionIr, ConditionType, PagesContext};

use crate::diagnostic::{ConfigDiagnostic, DiagnosticCode, SourceSpan};

// `PagesContext`, `ConditionIr`, `ConditionType`, and `ConditionEvaluationError` are the single
// shared definitions from `muxe-condition`, re-exported above so existing consumers keep working.

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ConditionProgram {
    source: String,
    ir: ConditionIr,
}

impl ConditionProgram {
    /// Compiles one closed condition expression: parses with the CEL facade, lowers the accepted
    /// AST into the shared IR once, and type-checks the closed Boolean expression.
    ///
    /// # Errors
    ///
    /// Returns a diagnostic when CEL parsing fails, when the AST uses a construct the IR cannot
    /// represent, or when the lowered expression does not type-check to Boolean.
    pub fn compile(source: &str, span: SourceSpan) -> Result<Self, ConfigDiagnostic> {
        // The CEL AST normalizes `0x10` to `Int(16)`, losing the spelling, so a source
        // pre-scan rejects hexadecimal integer literals with the actionable diagnostic.
        if let Some(literal) = find_hex_literal(source) {
            return Err(ConfigDiagnostic::error(
                DiagnosticCode::InvalidCondition,
                unsupported(
                    "hexadecimal integer literal",
                    "conditions support only decimal integers; rewrite the literal in decimal",
                ) + &format!(" near `{literal}`"),
                span,
            ));
        }
        let program = Program::compile(source).map_err(|error| {
            ConfigDiagnostic::error(
                DiagnosticCode::InvalidCondition,
                error.to_string(),
                span.clone(),
            )
        })?;
        let ir = lower_expression(program.expression()).map_err(|message| {
            ConfigDiagnostic::error(DiagnosticCode::InvalidCondition, message, span.clone())
        })?;
        if muxe_condition::type_check(&ir).map_err(|error| {
            ConfigDiagnostic::error(
                DiagnosticCode::InvalidCondition,
                format!("condition type error: {}", error.message()),
                span.clone(),
            )
        })? != ConditionType::Boolean
        {
            return Err(ConfigDiagnostic::error(
                DiagnosticCode::InvalidCondition,
                "condition type error: a condition must be a boolean expression, not an integer",
                span,
            ));
        }
        Ok(Self {
            source: source.to_owned(),
            ir,
        })
    }

    #[must_use]
    pub fn source(&self) -> &str {
        &self.source
    }

    #[must_use]
    pub fn ir(&self) -> &ConditionIr {
        &self.ir
    }

    #[must_use]
    pub fn uses_pages(&self) -> bool {
        self.ir.uses_pages()
    }

    /// Evaluates the expression for one pager state through the single shared evaluator.
    ///
    /// # Errors
    ///
    /// Returns an error when evaluation cannot produce a Boolean result. After a successful
    /// compile the only reachable failures are division by zero and integer overflow; type
    /// errors are rejected before publication.
    pub fn evaluate(&self, pages: PagesContext) -> Result<bool, ConditionEvaluationError> {
        muxe_condition::evaluate(&self.ir, pages)
    }
}
/// Lowers one accepted CEL AST into the shared IR. Every rejected construct names the construct
/// and the source fragment; CEL parse errors themselves surface from `Program::compile` above.
fn lower_expression(expression: &IdedExpr) -> Result<ConditionIr, String> {
    match &expression.expr {
        Expr::Literal(literal) => lower_literal(literal),
        Expr::Ident(name) => Err(unsupported(
            name,
            "bare identifiers are not conditions; only `pages.count` and `pages.current` are visible",
        )),
        Expr::Select(select) => lower_select(select),
        Expr::Call(call) => lower_call(call),
        Expr::List(_) => Err(unsupported(
            "list literal",
            "conditions support only booleans, integers, and `pages.*`; list literals are unsupported",
        )),
        Expr::Map(_) => Err(unsupported(
            "map literal",
            "conditions support only booleans, integers, and `pages.*`; map literals are unsupported",
        )),
        Expr::Struct(_) => Err(unsupported(
            "struct literal",
            "conditions support only booleans, integers, and `pages.*`; struct literals are unsupported",
        )),
        Expr::Comprehension(_) => Err(unsupported(
            "comprehension",
            "conditions do not support `map`/`filter`/`exists` comprehensions",
        )),
        Expr::Unspecified => Err("empty condition expression".to_owned()),
    }
}

fn lower_literal(literal: &LiteralValue) -> Result<ConditionIr, String> {
    match literal {
        LiteralValue::Boolean(value) => Ok(ConditionIr::Bool(**value)),
        LiteralValue::Int(value) => Ok(ConditionIr::Integer(**value)),
        LiteralValue::UInt(_) => Err(unsupported(
            "unsigned integer literal",
            "conditions support only signed 64-bit integers; drop the `u` suffix",
        )),
        LiteralValue::Double(_) => Err(unsupported(
            "floating-point literal",
            "conditions support only booleans and integers; floating-point literals are unsupported",
        )),
        LiteralValue::String(_) => Err(unsupported(
            "string literal",
            "conditions support only booleans, integers, and `pages.*`; string literals are unsupported",
        )),
        LiteralValue::Bytes(_) => Err(unsupported(
            "bytes literal",
            "conditions support only booleans, integers, and `pages.*`; bytes literals are unsupported",
        )),
        LiteralValue::Null => Err(unsupported(
            "null literal",
            "conditions support only booleans, integers, and `pages.*`; null is unsupported",
        )),
    }
}

fn lower_select(select: &cel::common::ast::SelectExpr) -> Result<ConditionIr, String> {
    if select.test {
        return Err(unsupported(
            "presence test",
            "`has(...)` macros are unsupported; compare `pages.count` or `pages.current` directly",
        ));
    }
    if select.field != "count" && select.field != "current" {
        return Err(unsupported(
            "member access",
            "only `pages.count` and `pages.current` are visible to conditions",
        ));
    }
    let field = select.field.as_str();
    match &select.operand.expr {
        Expr::Ident(name) if name == "pages" => match field {
            "count" => Ok(ConditionIr::PagesCount),
            "current" => Ok(ConditionIr::PagesCurrent),
            _ => Err(unsupported(
                "member access",
                "only `pages.count` and `pages.current` are visible to conditions",
            )),
        },
        Expr::Select(_) | Expr::Call(_) | Expr::Ident(_) => Err(unsupported(
            "member access",
            "only `pages.count` and `pages.current` are visible; chained member access is unsupported",
        )),
        _ => Err(unsupported(
            "member access",
            "only `pages.count` and `pages.current` are visible to conditions",
        )),
    }
}

fn lower_call(call: &CallExpr) -> Result<ConditionIr, String> {
    let name = call.func_name.as_str();
    if name == operators::CONDITIONAL {
        return lower_conditional(call);
    }
    if name == operators::LOGICAL_AND || name == operators::LOGICAL_OR {
        return lower_logical(name, call);
    }
    if name == operators::LOGICAL_NOT {
        let [operand] = call.args.as_slice() else {
            return Err("operator `!` requires exactly one operand".to_owned());
        };
        return Ok(ConditionIr::Not(Box::new(lower_expression(operand)?)));
    }
    if name == operators::NEGATE {
        let [operand] = call.args.as_slice() else {
            return Err("unary `-` requires exactly one operand".to_owned());
        };
        return Ok(ConditionIr::Negate(Box::new(lower_expression(operand)?)));
    }
    if let Some(build) = comparison_builder(name) {
        let [left, right] = call.args.as_slice() else {
            return Err(format!(
                "operator `{}` requires exactly two operands",
                display_op(name)
            ));
        };
        return Ok(build(
            Box::new(lower_expression(left)?),
            Box::new(lower_expression(right)?),
        ));
    }
    if let Some(build) = arithmetic_builder(name) {
        let [left, right] = call.args.as_slice() else {
            return Err(format!(
                "operator `{}` requires exactly two operands",
                display_op(name)
            ));
        };
        return Ok(build(
            Box::new(lower_expression(left)?),
            Box::new(lower_expression(right)?),
        ));
    }
    if name == operators::IN {
        return Err(unsupported(
            "`in` operator",
            "conditions do not support membership tests; compare integers directly",
        ));
    }
    if name == operators::INDEX || name == operators::OPT_INDEX {
        return Err(unsupported(
            "index access",
            "conditions do not support indexing; compare `pages.count` or `pages.current` directly",
        ));
    }
    if name == operators::OPT_SELECT {
        return Err(unsupported(
            "optional member access",
            "only `pages.count` and `pages.current` are visible to conditions",
        ));
    }
    Err(unsupported(
        "function call",
        "conditions support only `!`, `&&`, `||`, comparisons, integer arithmetic, and `?:`; function and macro calls (including `size`) are unsupported",
    ))
}

fn lower_conditional(call: &CallExpr) -> Result<ConditionIr, String> {
    let [condition, when_true, when_false] = call.args.as_slice() else {
        return Err("the `?:` operator requires exactly three operands".to_owned());
    };
    Ok(ConditionIr::Conditional(
        Box::new(lower_expression(condition)?),
        Box::new(lower_expression(when_true)?),
        Box::new(lower_expression(when_false)?),
    ))
}

fn lower_logical(name: &str, call: &CallExpr) -> Result<ConditionIr, String> {
    let [left, right] = call.args.as_slice() else {
        return Err(format!(
            "operator `{}` requires exactly two operands",
            display_op(name)
        ));
    };
    let left = lower_expression(left)?;
    let right = lower_expression(right)?;
    if name == operators::LOGICAL_AND {
        Ok(ConditionIr::And(Box::new(left), Box::new(right)))
    } else {
        Ok(ConditionIr::Or(Box::new(left), Box::new(right)))
    }
}
/// Constructor for one lowered binary node.
type BinaryBuilder = fn(Box<ConditionIr>, Box<ConditionIr>) -> ConditionIr;

fn comparison_builder(name: &str) -> Option<BinaryBuilder> {
    match name {
        _ if name == operators::EQUALS => Some(ConditionIr::Equal),
        _ if name == operators::NOT_EQUALS => Some(ConditionIr::NotEqual),
        _ if name == operators::LESS => Some(ConditionIr::Less),
        _ if name == operators::LESS_EQUALS => Some(ConditionIr::LessEqual),
        _ if name == operators::GREATER => Some(ConditionIr::Greater),
        _ if name == operators::GREATER_EQUALS => Some(ConditionIr::GreaterEqual),
        _ => None,
    }
}

fn arithmetic_builder(name: &str) -> Option<BinaryBuilder> {
    match name {
        _ if name == operators::ADD => Some(ConditionIr::Add),
        _ if name == operators::SUBSTRACT => Some(ConditionIr::Subtract),
        _ if name == operators::MULTIPLY => Some(ConditionIr::Multiply),
        _ if name == operators::DIVIDE => Some(ConditionIr::Divide),
        _ if name == operators::MODULO => Some(ConditionIr::Modulo),
        _ => None,
    }
}

fn display_op(name: &str) -> &str {
    match name {
        _ if name == operators::EQUALS => "==",
        _ if name == operators::NOT_EQUALS => "!=",
        _ if name == operators::LESS => "<",
        _ if name == operators::LESS_EQUALS => "<=",
        _ if name == operators::GREATER => ">",
        _ if name == operators::GREATER_EQUALS => ">=",
        _ if name == operators::ADD => "+",
        _ if name == operators::SUBSTRACT => "-",
        _ if name == operators::MULTIPLY => "*",
        _ if name == operators::DIVIDE => "/",
        _ if name == operators::MODULO => "%",
        _ if name == operators::LOGICAL_AND => "&&",
        _ if name == operators::LOGICAL_OR => "||",
        _ => name,
    }
}

fn unsupported(construct: &str, help: &str) -> String {
    format!("unsupported construct in condition: {construct} ({help})")
}

/// Scans the raw source for a hexadecimal integer literal outside string literals. The CEL AST
/// normalizes `0x10` to `Int(16)`, so the spelling is unrecoverable after parsing; this pre-scan
/// keeps the rejection actionable. Returns the offending literal text.
fn find_hex_literal(source: &str) -> Option<String> {
    let bytes = source.as_bytes();
    let mut index = 0;
    let mut quote: Option<u8> = None;
    while index < bytes.len() {
        let byte = bytes[index];
        if let Some(open) = quote {
            if byte == b'\\' {
                index += 2;
                continue;
            }
            if byte == open {
                quote = None;
            }
            index += 1;
            continue;
        }
        if byte == b'\'' || byte == b'"' {
            quote = Some(byte);
            index += 1;
            continue;
        }
        let is_hex_prefix = byte == b'0'
            && index + 1 < bytes.len()
            && (bytes[index + 1] == b'x' || bytes[index + 1] == b'X');
        let preceded_by_word =
            index > 0 && (bytes[index - 1].is_ascii_alphanumeric() || bytes[index - 1] == b'_');
        if is_hex_prefix && !preceded_by_word {
            let mut end = index + 2;
            while end < bytes.len() && bytes[end].is_ascii_hexdigit() {
                end += 1;
            }
            if end > index + 2 {
                return Some(source[index..end].to_owned());
            }
            return Some(source[index..index + 2].to_owned());
        }
        index += 1;
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::diagnostic::SourceId;

    fn span() -> SourceSpan {
        SourceSpan::new(SourceId::new("test"), 0, 1)
    }

    fn evaluate_source(
        source: &str,
        pages: PagesContext,
    ) -> Result<bool, ConditionEvaluationError> {
        ConditionProgram::compile(source, span())
            .unwrap()
            .evaluate(pages)
    }

    #[test]
    fn evaluates_pager_expression_without_reparsing() {
        let program =
            ConditionProgram::compile("pages.current < pages.count && pages.count > 1", span())
                .unwrap();
        assert!(program.uses_pages());
        assert!(
            program
                .evaluate(PagesContext {
                    current: 1,
                    count: 2
                })
                .unwrap()
        );
        assert!(
            !program
                .evaluate(PagesContext {
                    current: 2,
                    count: 2
                })
                .unwrap()
        );
    }

    #[test]
    fn accepts_cel_arithmetic_negative_and_conditional() {
        let pages = PagesContext {
            count: 3,
            current: 2,
        };
        for (source, expected) in [
            ("pages.count + 1 > 2", true),
            ("-1 < 0", true),
            ("pages.current == pages.count - 1", true),
            ("true ? pages.count > 0 : false", true),
            ("!(pages.count == 0)", true),
            ("pages.count * 2 == 6", true),
            ("pages.count % 2 == 1", true),
            ("pages.count / 3 == 1", true),
            ("-(pages.count) < 0", true),
        ] {
            assert_eq!(evaluate_source(source, pages), Ok(expected), "{source}");
        }
        assert_eq!(
            evaluate_source(
                "pages.count + 1 > 2",
                PagesContext {
                    count: 1,
                    current: 1
                }
            ),
            Ok(false)
        );
        assert_eq!(
            evaluate_source(
                "pages.current == pages.count - 1",
                PagesContext {
                    count: 3,
                    current: 1
                }
            ),
            Ok(false)
        );
    }

    #[test]
    fn rejects_non_boolean_operands_at_compile_time() {
        for source in [
            "true && 1",
            "1",
            "1 < true",
            "!pages.count",
            "pages.count + true > 1",
            "true ? 1 : false",
        ] {
            let error = ConditionProgram::compile(source, span()).expect_err("must not compile");
            assert_eq!(error.code, DiagnosticCode::InvalidCondition, "{source}");
        }
    }

    #[test]
    fn rejects_unsupported_cel_constructs_with_actionable_diagnostics() {
        for (source, fragment) in [
            ("\"a\" == \"b\"", "string"),
            ("size([1, 2]) > 1", "size"),
            ("1 in [1]", "`in`"),
            ("foo.bar > 1", "member"),
            ("pages.count.foo > 1", "member"),
            ("[1, 2][0] > 1", "index"),
            ("1.5 > 1", "floating"),
            ("1u > 1", "unsigned"),
            ("null == null", "null"),
            ("b\"a\" == b\"b\"", "bytes"),
            ("has(pages.count)", "has"),
            ("[1].exists(x, x > 0)", "comprehension"),
            ("0x10 > 15", "hexadecimal"),
            ("0XFF == 255", "hexadecimal"),
        ] {
            let error = ConditionProgram::compile(source, span()).expect_err("must not compile");
            assert_eq!(error.code, DiagnosticCode::InvalidCondition, "{source}");
            assert!(
                error.message.contains(fragment),
                "{source}: {}",
                error.message
            );
        }
    }

    #[test]
    fn rejects_chained_comparison_as_a_type_error() {
        // CEL parses `1 < 2 < 3` as `(1 < 2) < 3`; lowering succeeds but the closed
        // expression does not type-check because `<` needs integer operands.
        let error = ConditionProgram::compile("1 < 2 < 3", span()).expect_err("must not compile");
        assert_eq!(error.code, DiagnosticCode::InvalidCondition);
        assert!(error.message.contains("integer"), "{}", error.message);
    }

    #[test]
    fn division_by_zero_and_overflow_fail_at_evaluation() {
        let pages = PagesContext {
            count: 3,
            current: 1,
        };
        assert_eq!(
            evaluate_source("pages.count / 0 > 1", pages),
            Err(ConditionEvaluationError::DivisionByZero)
        );
        assert_eq!(
            evaluate_source("pages.count % 0 == 0", pages),
            Err(ConditionEvaluationError::DivisionByZero)
        );
        assert_eq!(
            evaluate_source("9035891482771791360 + 9035891482771791360 > 0", pages),
            Err(ConditionEvaluationError::ArithmeticOverflow)
        );
    }
}
