//! Lossless typed condition IR for Muxe's closed pager CEL context.
//!
//! The compiler first delegates syntax acceptance to the `cel` facade, then lowers the expression
//! into this immutable IR. UI snapshots carry this IR and evaluate it directly; no UI/protocol
//! participant reparses raw CEL source per render or at attachment time.

use cel::Program;

use crate::diagnostic::{ConfigDiagnostic, DiagnosticCode, SourceSpan};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PagesContext {
    pub count: u64,
    pub current: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ConditionProgram {
    source: String,
    ir: ConditionIr,
}

impl ConditionProgram {
    /// Compiles one closed condition expression.
    ///
    /// # Errors
    ///
    /// Returns a diagnostic when parsing or lowering the expression fails.
    pub fn compile(source: &str, span: SourceSpan) -> Result<Self, ConfigDiagnostic> {
        Program::compile(source).map_err(|error| {
            ConfigDiagnostic::error(
                DiagnosticCode::InvalidCondition,
                error.to_string(),
                span.clone(),
            )
        })?;
        let mut parser = ConditionParser::new(source);
        let ir = parser.parse().map_err(|message| {
            ConfigDiagnostic::error(DiagnosticCode::InvalidCondition, message, span)
        })?;
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

    /// Evaluates the expression for one pager state.
    ///
    /// # Errors
    ///
    /// Returns an error when evaluation cannot produce a Boolean result.
    pub fn evaluate(&self, pages: PagesContext) -> Result<bool, ConditionEvaluationError> {
        match self.ir.evaluate(pages)? {
            ConditionValue::Bool(value) => Ok(value),
            ConditionValue::Integer(_) => Err(ConditionEvaluationError::NonBooleanResult),
        }
    }
}

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
}

impl ConditionIr {
    fn uses_pages(&self) -> bool {
        match self {
            Self::PagesCount | Self::PagesCurrent => true,
            Self::Bool(_) | Self::Integer(_) => false,
            Self::Not(value) => value.uses_pages(),
            Self::And(left, right)
            | Self::Or(left, right)
            | Self::Equal(left, right)
            | Self::NotEqual(left, right)
            | Self::Less(left, right)
            | Self::LessEqual(left, right)
            | Self::Greater(left, right)
            | Self::GreaterEqual(left, right) => left.uses_pages() || right.uses_pages(),
        }
    }

    fn evaluate(&self, pages: PagesContext) -> Result<ConditionValue, ConditionEvaluationError> {
        use ConditionIr as Ir;
        match self {
            Ir::Bool(value) => Ok(ConditionValue::Bool(*value)),
            Ir::Integer(value) => Ok(ConditionValue::Integer(*value)),
            Ir::PagesCount => Ok(ConditionValue::Integer(
                i64::try_from(pages.count).unwrap_or(i64::MAX),
            )),
            Ir::PagesCurrent => Ok(ConditionValue::Integer(
                i64::try_from(pages.current).unwrap_or(i64::MAX),
            )),
            Ir::Not(value) => Ok(ConditionValue::Bool(!value.evaluate(pages)?.bool()?)),
            Ir::And(left, right) => {
                let left = left.evaluate(pages)?.bool()?;
                Ok(ConditionValue::Bool(left && right.evaluate(pages)?.bool()?))
            }
            Ir::Or(left, right) => {
                let left = left.evaluate(pages)?.bool()?;
                Ok(ConditionValue::Bool(left || right.evaluate(pages)?.bool()?))
            }
            Ir::Equal(left, right) => Ok(ConditionValue::Bool(
                left.evaluate(pages)? == right.evaluate(pages)?,
            )),
            Ir::NotEqual(left, right) => Ok(ConditionValue::Bool(
                left.evaluate(pages)? != right.evaluate(pages)?,
            )),
            Ir::Less(left, right) => compare(left, right, pages, |left, right| left < right),
            Ir::LessEqual(left, right) => compare(left, right, pages, |left, right| left <= right),
            Ir::Greater(left, right) => compare(left, right, pages, |left, right| left > right),
            Ir::GreaterEqual(left, right) => {
                compare(left, right, pages, |left, right| left >= right)
            }
        }
    }
}

fn compare(
    left: &ConditionIr,
    right: &ConditionIr,
    pages: PagesContext,
    predicate: impl FnOnce(i64, i64) -> bool,
) -> Result<ConditionValue, ConditionEvaluationError> {
    Ok(ConditionValue::Bool(predicate(
        left.evaluate(pages)?.integer()?,
        right.evaluate(pages)?.integer()?,
    )))
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ConditionValue {
    Bool(bool),
    Integer(i64),
}

impl ConditionValue {
    fn bool(self) -> Result<bool, ConditionEvaluationError> {
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

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ConditionEvaluationError {
    TypeMismatch,
    NonBooleanResult,
}

struct ConditionParser<'a> {
    input: &'a str,
    position: usize,
}

impl<'a> ConditionParser<'a> {
    fn new(input: &'a str) -> Self {
        Self { input, position: 0 }
    }

    fn parse(&mut self) -> Result<ConditionIr, String> {
        let expression = self.or()?;
        self.whitespace();
        if self.position != self.input.len() {
            return Err(format!(
                "unsupported CEL condition syntax near `{}`",
                &self.input[self.position..]
            ));
        }
        Ok(expression)
    }

    fn or(&mut self) -> Result<ConditionIr, String> {
        let mut expression = self.and()?;
        while self.consume("||") {
            expression = ConditionIr::Or(Box::new(expression), Box::new(self.and()?));
        }
        Ok(expression)
    }

    fn and(&mut self) -> Result<ConditionIr, String> {
        let mut expression = self.comparison()?;
        while self.consume("&&") {
            expression = ConditionIr::And(Box::new(expression), Box::new(self.comparison()?));
        }
        Ok(expression)
    }

    fn comparison(&mut self) -> Result<ConditionIr, String> {
        let left = self.unary()?;
        for (operator, build) in [
            (
                "==",
                ConditionIr::Equal as fn(Box<ConditionIr>, Box<ConditionIr>) -> ConditionIr,
            ),
            ("!=", ConditionIr::NotEqual),
            ("<=", ConditionIr::LessEqual),
            (">=", ConditionIr::GreaterEqual),
            ("<", ConditionIr::Less),
            (">", ConditionIr::Greater),
        ] {
            if self.consume(operator) {
                return Ok(build(Box::new(left), Box::new(self.unary()?)));
            }
        }
        Ok(left)
    }

    fn unary(&mut self) -> Result<ConditionIr, String> {
        if self.consume("!") {
            return Ok(ConditionIr::Not(Box::new(self.unary()?)));
        }
        self.atom()
    }

    fn atom(&mut self) -> Result<ConditionIr, String> {
        self.whitespace();
        if self.consume("(") {
            let expression = self.or()?;
            if !self.consume(")") {
                return Err("unclosed CEL condition parenthesis".to_owned());
            }
            return Ok(expression);
        }
        for (token, expression) in [
            ("pages.count", ConditionIr::PagesCount),
            ("pages.current", ConditionIr::PagesCurrent),
            ("true", ConditionIr::Bool(true)),
            ("false", ConditionIr::Bool(false)),
        ] {
            if self.consume_word(token) {
                return Ok(expression);
            }
        }
        let start = self.position;
        while self.input[self.position..]
            .chars()
            .next()
            .is_some_and(|character| character.is_ascii_digit())
        {
            self.position += 1;
        }
        if start != self.position {
            return self.input[start..self.position]
                .parse::<i64>()
                .map(ConditionIr::Integer)
                .map_err(|_| "CEL integer is out of range".to_owned());
        }
        Err(format!(
            "unsupported CEL condition syntax near `{}`",
            &self.input[self.position..]
        ))
    }

    fn consume(&mut self, token: &str) -> bool {
        self.whitespace();
        if self.input[self.position..].starts_with(token) {
            self.position += token.len();
            true
        } else {
            false
        }
    }

    fn consume_word(&mut self, token: &str) -> bool {
        self.whitespace();
        let remainder = &self.input[self.position..];
        if !remainder.starts_with(token) {
            return false;
        }
        if remainder[token.len()..]
            .chars()
            .next()
            .is_some_and(|character| character.is_ascii_alphanumeric() || character == '_')
        {
            return false;
        }
        self.position += token.len();
        true
    }

    fn whitespace(&mut self) {
        while self.input[self.position..]
            .chars()
            .next()
            .is_some_and(char::is_whitespace)
        {
            self.position += 1;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::diagnostic::SourceId;

    #[test]
    fn evaluates_pager_expression_without_reparsing() {
        let program = ConditionProgram::compile(
            "pages.current < pages.count && pages.count > 1",
            SourceSpan::new(SourceId::new("test"), 0, 1),
        )
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
}
