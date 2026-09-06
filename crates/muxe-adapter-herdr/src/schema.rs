use std::{
    cmp::Ordering,
    collections::{BTreeMap, BTreeSet},
    fmt,
    sync::{Arc, Mutex},
};

use regex::Regex;
use serde_json::{Map, Number, Value};
use sha2::{Digest, Sha256};
use thiserror::Error;

/// The version is part of both runtime-schema and configured-request cache keys.
pub const VALIDATOR_FORMAT_VERSION: u32 = 1;

/// A parsed Herdr API document. It deliberately retains the raw document so `$ref` values
/// stay meaningful to diagnostics and to the runtime compatibility cache.
#[derive(Clone, Debug)]
pub struct ApiSchema {
    raw: Value,
    protocol: u64,
    schema_version: u64,
    canonical_request_sha256: String,
    methods: BTreeMap<String, Vec<MethodSchema>>,
    patterns: Arc<Mutex<BTreeMap<String, Result<Regex, String>>>>,
}

#[derive(Clone, Debug)]
pub struct MethodSchema {
    pub name: String,
    params: ParameterSchema,
}
#[derive(Clone, Debug)]
enum ParameterSchema {
    Reference(String),
    Inline { schema: Value, schema_path: String },
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
#[error("{code} at {instance_path} against {schema_path}: {detail}")]
pub struct ValidationError {
    pub code: ValidationCode,
    pub instance_path: String,
    pub schema_path: String,
    pub detail: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ValidationCode {
    MissingMethod,
    Type,
    Required,
    AdditionalProperty,
    PropertyName,
    Const,
    Enum,
    Minimum,
    Maximum,
    Pattern,
    MinLength,
    MaxLength,
    MinItems,
    MaxItems,
    MinProperties,
    MaxProperties,
    UniqueItems,
    AnyOf,
    OneOf,
    Reference,
    UnsupportedConstruct,
    MalformedSchema,
}

impl fmt::Display for ValidationCode {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::MissingMethod => "missing_method",
            Self::Type => "type",
            Self::Required => "required",
            Self::AdditionalProperty => "additional_property",
            Self::PropertyName => "property_name",
            Self::Const => "const",
            Self::Enum => "enum",
            Self::Minimum => "minimum",
            Self::Maximum => "maximum",
            Self::Pattern => "pattern",
            Self::MinLength => "min_length",
            Self::MaxLength => "max_length",
            Self::MinItems => "min_items",
            Self::MaxItems => "max_items",
            Self::MinProperties => "min_properties",
            Self::MaxProperties => "max_properties",
            Self::UniqueItems => "unique_items",
            Self::AnyOf => "any_of",
            Self::OneOf => "one_of",
            Self::Reference => "reference",
            Self::UnsupportedConstruct => "unsupported_construct",
            Self::MalformedSchema => "malformed_schema",
        })
    }
}

impl ApiSchema {
    pub fn parse(raw: Value) -> Result<Self, ValidationError> {
        let protocol = raw
            .get("protocol")
            .and_then(Value::as_u64)
            .ok_or_else(|| malformed("#", "top-level protocol must be an unsigned integer"))?;
        let schema_version = raw
            .get("schema_version")
            .and_then(Value::as_u64)
            .ok_or_else(|| malformed("#", "top-level schema_version must be an unsigned integer"))?;
        let request = raw
            .pointer("/schemas/request")
            .ok_or_else(|| malformed("#", "schema has no schemas.request object"))?;
        let branches = request
            .get("oneOf")
            .and_then(Value::as_array)
            .ok_or_else(|| malformed("#/schemas/request", "request.oneOf must be an array"))?;
        let mut methods = BTreeMap::new();
        for (index, branch) in branches.iter().enumerate() {
            let Some(properties) = branch.get("properties").and_then(Value::as_object) else {
                continue;
            };
            let Some(method) = properties
                .get("method")
                .and_then(|method| method.get("const"))
                .and_then(Value::as_str)
            else {
                continue;
            };
            let Some(params) = properties.get("params") else {
                continue;
            };
            let params = match params.get("$ref").and_then(Value::as_str) {
                Some(reference) => ParameterSchema::Reference(reference.to_owned()),
                None => ParameterSchema::Inline {
                    schema: params.clone(),
                    schema_path: format!("#/schemas/request/oneOf/{index}/properties/params"),
                },
            };
            methods
                .entry(method.to_owned())
                .or_insert_with(Vec::new)
                .push(MethodSchema {
                    name: method.to_owned(),
                    params,
                });
        }

        let canonical_request_sha256 = sha256_hex(&canonical_json(request));
        Ok(Self {
            raw,
            protocol,
            schema_version,
            canonical_request_sha256,
            methods,
            patterns: Arc::new(Mutex::new(BTreeMap::new())),
        })
    }

    pub fn protocol(&self) -> u64 {
        self.protocol
    }

    pub fn schema_version(&self) -> u64 {
        self.schema_version
    }

    pub fn canonical_request_sha256(&self) -> &str {
        &self.canonical_request_sha256
    }

    pub fn method(&self, name: &str) -> Option<&MethodSchema> {
        self.methods.get(name).and_then(|methods| match methods.as_slice() {
            [method] => Some(method),
            _ => None,
        })
    }

    pub fn methods(&self) -> impl Iterator<Item = &MethodSchema> {
        self.methods.values().flatten()
    }

    pub fn validate_method(&self, method: &str, params: &Value) -> Result<(), ValidationError> {
        let method_schemas = self.methods.get(method).ok_or_else(|| ValidationError {
            code: ValidationCode::MissingMethod,
            instance_path: "#".to_owned(),
            schema_path: "#/schemas/request".to_owned(),
            detail: format!("Herdr method {method:?} is not declared by this schema"),
        })?;
        let method_schema = match method_schemas.as_slice() {
            [method_schema] => method_schema,
            _ => {
                return Err(malformed(
                    "#/schemas/request",
                    format!("Herdr method {method:?} has multiple request schema branches"),
                ));
            }
        };
        let (root, schema_path) = match &method_schema.params {
            ParameterSchema::Reference(reference) => {
                (self.resolve_reference(reference, "#")?, reference.as_str())
            }
            ParameterSchema::Inline { schema, schema_path } => (schema, schema_path.as_str()),
        };
        self.validate(root, params, "#", schema_path, 0)
    }

    pub fn canonical_request_schema(&self) -> Value {
        self.raw
            .pointer("/schemas/request")
            .cloned()
            .expect("request presence was checked when constructing ApiSchema")
    }

    fn validate(
        &self,
        schema: &Value,
        value: &Value,
        instance_path: &str,
        schema_path: &str,
        reference_depth: usize,
    ) -> Result<(), ValidationError> {
        if reference_depth > 128 {
            return Err(error(
                ValidationCode::Reference,
                instance_path,
                schema_path,
                "reference depth exceeds 128",
            ));
        }
        let object = schema
            .as_object()
            .ok_or_else(|| malformed(schema_path, "schema node must be an object"))?;
        self.reject_unknown_keywords(object, instance_path, schema_path)?;

        if let Some(reference) = string_keyword(object, "$ref", schema_path)? {
            let resolved = self.resolve_reference(reference, schema_path)?;
            self.validate(
                resolved,
                value,
                instance_path,
                reference,
                reference_depth + 1,
            )?;
        }
        if let Some(constant) = object.get("const") {
            if value != constant {
                return Err(error(
                    ValidationCode::Const,
                    instance_path,
                    schema_path,
                    "value does not equal schema const",
                ));
            }
        }
        if let Some(values) = object.get("enum") {
            let values = values
                .as_array()
                .ok_or_else(|| malformed(schema_path, "enum must be an array"))?;
            if !values.iter().any(|candidate| candidate == value) {
                return Err(error(
                    ValidationCode::Enum,
                    instance_path,
                    schema_path,
                    "value is not one of the declared enum alternatives",
                ));
            }
        }
        if let Some(types) = object.get("type") {
            self.validate_type(types, value, instance_path, schema_path)?;
        }
        if let Some(format) = string_keyword(object, "format", schema_path)? {
            self.validate_format(format, value, instance_path, schema_path)?;
        }
        self.validate_number_constraints(object, value, instance_path, schema_path)?;
        self.validate_string_constraints(object, value, instance_path, schema_path)?;
        self.validate_array_constraints(object, value, instance_path, schema_path, reference_depth)?;
        self.validate_object_constraints(object, value, instance_path, schema_path, reference_depth)?;
        self.validate_alternatives(object, value, instance_path, schema_path, reference_depth)
    }

    fn reject_unknown_keywords(
        &self,
        schema: &Map<String, Value>,
        instance_path: &str,
        schema_path: &str,
    ) -> Result<(), ValidationError> {
        const SUPPORTED: &[&str] = &[
            "$defs",
            "$ref",
            "$schema",
            "additionalProperties",
            "anyOf",
            "const",
            "default",
            "definitions",
            "description",
            "enum",
            "format",
            "items",
            "maxItems",
            "maxLength",
            "maxProperties",
            "maximum",
            "minItems",
            "minLength",
            "minProperties",
            "minimum",
            "oneOf",
            "pattern",
            "properties",
            "propertyNames",
            "required",
            "title",
            "type",
            "uniqueItems",
        ];
        for keyword in schema.keys() {
            if !SUPPORTED.contains(&keyword.as_str()) {
                return Err(error(
                    ValidationCode::UnsupportedConstruct,
                    instance_path,
                    schema_path,
                    format!("unsupported JSON Schema keyword {keyword:?}"),
                ));
            }
        }
        Ok(())
    }

    fn validate_type(
        &self,
        types: &Value,
        value: &Value,
        instance_path: &str,
        schema_path: &str,
    ) -> Result<(), ValidationError> {
        let accepted = match types {
            Value::String(kind) => value_has_type(value, kind),
            Value::Array(kinds) => kinds.iter().all(Value::is_string)
                && kinds
                    .iter()
                    .filter_map(Value::as_str)
                    .any(|kind| value_has_type(value, kind)),
            _ => return Err(malformed(schema_path, "type must be a string or a string array")),
        };
        if accepted {
            return Ok(());
        }
        Err(error(
            ValidationCode::Type,
            instance_path,
            schema_path,
            format!("value is not compatible with type {types}"),
        ))
    }

    fn validate_format(
        &self,
        format: &str,
        value: &Value,
        instance_path: &str,
        schema_path: &str,
    ) -> Result<(), ValidationError> {
        let valid = match format {
            "float" => value.as_number().is_none_or(|number| number.as_f64().is_some()),
            "int32" => value.as_number().is_none_or(|number| {
                number
                    .as_i64()
                    .is_some_and(|number| (i32::MIN as i64..=i32::MAX as i64).contains(&number))
            }),
            "uint" | "uint64" => value.as_number().is_none_or(|number| number.as_u64().is_some()),
            "uint16" => value.as_number().is_none_or(|number| {
                number.as_u64().is_some_and(|number| u16::try_from(number).is_ok())
            }),
            "uint32" => value.as_number().is_none_or(|number| {
                number.as_u64().is_some_and(|number| u32::try_from(number).is_ok())
            }),
            _ => {
                return Err(error(
                    ValidationCode::UnsupportedConstruct,
                    instance_path,
                    schema_path,
                    format!("unsupported JSON Schema format {format:?}"),
                ));
            }
        };
        if valid {
            Ok(())
        } else {
            Err(error(
                ValidationCode::Type,
                instance_path,
                schema_path,
                format!("number is not compatible with format {format:?}"),
            ))
        }
    }

    fn validate_number_constraints(
        &self,
        schema: &Map<String, Value>,
        value: &Value,
        instance_path: &str,
        schema_path: &str,
    ) -> Result<(), ValidationError> {
        let Some(number) = value.as_number() else {
            return Ok(());
        };
        if let Some(minimum) = number_keyword(schema, "minimum", schema_path)? {
            if compare_json_numbers(number, minimum, schema_path)? == Ordering::Less {
                return Err(error(
                    ValidationCode::Minimum,
                    instance_path,
                    schema_path,
                    format!("number must be at least {minimum}"),
                ));
            }
        }
        if let Some(maximum) = number_keyword(schema, "maximum", schema_path)? {
            if compare_json_numbers(number, maximum, schema_path)? == Ordering::Greater {
                return Err(error(
                    ValidationCode::Maximum,
                    instance_path,
                    schema_path,
                    format!("number must be at most {maximum}"),
                ));
            }
        }
        Ok(())
    }

    fn validate_string_constraints(
        &self,
        schema: &Map<String, Value>,
        value: &Value,
        instance_path: &str,
        schema_path: &str,
    ) -> Result<(), ValidationError> {
        let Some(string) = value.as_str() else {
            return Ok(());
        };
        let length = string.chars().count();
        if let Some(minimum) = usize_keyword(schema, "minLength", schema_path)? {
            if length < minimum {
                return Err(error(
                    ValidationCode::MinLength,
                    instance_path,
                    schema_path,
                    format!("string must contain at least {minimum} Unicode scalar values"),
                ));
            }
        }
        if let Some(maximum) = usize_keyword(schema, "maxLength", schema_path)? {
            if length > maximum {
                return Err(error(
                    ValidationCode::MaxLength,
                    instance_path,
                    schema_path,
                    format!("string must contain at most {maximum} Unicode scalar values"),
                ));
            }
        }
        if let Some(pattern) = string_keyword(schema, "pattern", schema_path)? {
            let expression = self.compile_pattern(pattern, instance_path, schema_path)?;
            if !expression.is_match(string) {
                return Err(error(
                    ValidationCode::Pattern,
                    instance_path,
                    schema_path,
                    format!("string does not match pattern {pattern:?}"),
                ));
            }
        }
        Ok(())
    }

    fn compile_pattern(
        &self,
        pattern: &str,
        instance_path: &str,
        schema_path: &str,
    ) -> Result<Regex, ValidationError> {
        let mut patterns = match self.patterns.lock() {
            Ok(patterns) => patterns,
            Err(poisoned) => {
                let mut patterns = poisoned.into_inner();
                patterns.clear();
                patterns
            }
        };
        let compiled = patterns
            .entry(pattern.to_owned())
            .or_insert_with(|| Regex::new(pattern).map_err(|source| source.to_string()));
        compiled.clone().map_err(|source| {
            error(
                ValidationCode::MalformedSchema,
                instance_path,
                schema_path,
                format!("invalid pattern {pattern:?}: {source}"),
            )
        })
    }

    fn validate_array_constraints(
        &self,
        schema: &Map<String, Value>,
        value: &Value,
        instance_path: &str,
        schema_path: &str,
        reference_depth: usize,
    ) -> Result<(), ValidationError> {
        let Some(values) = value.as_array() else {
            return Ok(());
        };
        if let Some(minimum) = usize_keyword(schema, "minItems", schema_path)? {
            if values.len() < minimum {
                return Err(error(
                    ValidationCode::MinItems,
                    instance_path,
                    schema_path,
                    format!("array must contain at least {minimum} items"),
                ));
            }
        }
        if let Some(maximum) = usize_keyword(schema, "maxItems", schema_path)? {
            if values.len() > maximum {
                return Err(error(
                    ValidationCode::MaxItems,
                    instance_path,
                    schema_path,
                    format!("array must contain at most {maximum} items"),
                ));
            }
        }
        if schema.get("uniqueItems") == Some(&Value::Bool(true)) {
            let mut unique = BTreeSet::new();
            for item in values {
                let canonical = canonical_json(item);
                if !unique.insert(canonical) {
                    return Err(error(
                        ValidationCode::UniqueItems,
                        instance_path,
                        schema_path,
                        "array items must be unique",
                    ));
                }
            }
        }
        if let Some(items) = schema.get("items") {
            for (index, item) in values.iter().enumerate() {
                self.validate(
                    items,
                    item,
                    &instance_child(instance_path, &index.to_string()),
                    &schema_child(schema_path, "items"),
                    reference_depth,
                )?;
            }
        }
        Ok(())
    }

    fn validate_object_constraints(
        &self,
        schema: &Map<String, Value>,
        value: &Value,
        instance_path: &str,
        schema_path: &str,
        reference_depth: usize,
    ) -> Result<(), ValidationError> {
        let Some(properties) = value.as_object() else {
            return Ok(());
        };
        if let Some(minimum) = usize_keyword(schema, "minProperties", schema_path)? {
            if properties.len() < minimum {
                return Err(error(
                    ValidationCode::MinProperties,
                    instance_path,
                    schema_path,
                    format!("object must contain at least {minimum} properties"),
                ));
            }
        }
        if let Some(maximum) = usize_keyword(schema, "maxProperties", schema_path)? {
            if properties.len() > maximum {
                return Err(error(
                    ValidationCode::MaxProperties,
                    instance_path,
                    schema_path,
                    format!("object must contain at most {maximum} properties"),
                ));
            }
        }
        let declared = schema
            .get("properties")
            .map(|properties| {
                properties
                    .as_object()
                    .ok_or_else(|| malformed(schema_path, "properties must be an object"))
            })
            .transpose()?;
        let required = schema
            .get("required")
            .map(|required| {
                required
                    .as_array()
                    .ok_or_else(|| malformed(schema_path, "required must be an array"))
            })
            .transpose()?;
        if let Some(required) = required {
            for name in required {
                let name = name
                    .as_str()
                    .ok_or_else(|| malformed(schema_path, "required entries must be strings"))?;
                if !properties.contains_key(name) {
                    return Err(error(
                        ValidationCode::Required,
                        instance_path,
                        schema_path,
                        format!("required property {name:?} is missing"),
                    ));
                }
            }
        }
        if let Some(property_names) = schema.get("propertyNames") {
            for name in properties.keys() {
                self.validate(
                    property_names,
                    &Value::String(name.clone()),
                    &instance_child(instance_path, name),
                    &schema_child(schema_path, "propertyNames"),
                    reference_depth,
                )?;
            }
        }
        let additional = schema.get("additionalProperties");
        for (name, value) in properties {
            if let Some(property_schema) = declared.and_then(|declared| declared.get(name)) {
                self.validate(
                    property_schema,
                    value,
                    &instance_child(instance_path, name),
                    &schema_child(&schema_child(schema_path, "properties"), name),
                    reference_depth,
                )?;
                continue;
            }
            match additional {
                Some(Value::Bool(false)) => {
                    return Err(error(
                        ValidationCode::AdditionalProperty,
                        &instance_child(instance_path, name),
                        schema_path,
                        format!("property {name:?} is not declared"),
                    ));
                }
                Some(Value::Bool(true)) => {}
                None => {
                    return Err(error(
                        ValidationCode::AdditionalProperty,
                        &instance_child(instance_path, name),
                        schema_path,
                        format!("property {name:?} is not declared"),
                    ));
                }
                Some(additional_schema) => self.validate(
                    additional_schema,
                    value,
                    &instance_child(instance_path, name),
                    &schema_child(schema_path, "additionalProperties"),
                    reference_depth,
                )?,
            }
        }
        Ok(())
    }

    fn validate_alternatives(
        &self,
        schema: &Map<String, Value>,
        value: &Value,
        instance_path: &str,
        schema_path: &str,
        reference_depth: usize,
    ) -> Result<(), ValidationError> {
        for (keyword, exactly_one, code) in [
            ("anyOf", false, ValidationCode::AnyOf),
            ("oneOf", true, ValidationCode::OneOf),
        ] {
            let Some(alternatives) = schema.get(keyword) else {
                continue;
            };
            let alternatives = alternatives
                .as_array()
                .ok_or_else(|| malformed(schema_path, &format!("{keyword} must be an array")))?;
            let mut matching = 0;
            for (index, alternative) in alternatives.iter().enumerate() {
                let alternative_path =
                    schema_child(&schema_child(schema_path, keyword), &index.to_string());
                match self.validate(
                    alternative,
                    value,
                    instance_path,
                    &alternative_path,
                    reference_depth,
                ) {
                    Ok(()) => matching += 1,
                    Err(validation_error)
                        if is_schema_error(validation_error.code)
                            && self.alternative_could_match(
                                alternative,
                                value,
                                &alternative_path,
                                reference_depth,
                            )? =>
                    {
                        return Err(validation_error);
                    }
                    Err(_) => {}
                }
            }
            if matching == 0 || (exactly_one && matching != 1) {
                return Err(error(
                    code,
                    instance_path,
                    schema_path,
                    if exactly_one {
                        format!("value must match exactly one {keyword} alternative; matched {matching}")
                    } else {
                        format!("value must match at least one {keyword} alternative")
                    },
                ));
            }
        }
        Ok(())
    }

    fn alternative_could_match(
        &self,
        schema: &Value,
        value: &Value,
        schema_path: &str,
        reference_depth: usize,
    ) -> Result<bool, ValidationError> {
        if reference_depth > 128 {
            return Err(error(
                ValidationCode::Reference,
                "#",
                schema_path,
                "reference depth exceeds 128",
            ));
        }
        let object = schema
            .as_object()
            .ok_or_else(|| malformed(schema_path, "schema node must be an object"))?;
        if let Some(types) = object.get("type") {
            let matches_type = match types {
                Value::String(kind) => value_has_type(value, kind),
                Value::Array(kinds) => kinds.iter().all(Value::is_string)
                    && kinds
                        .iter()
                        .filter_map(Value::as_str)
                        .any(|kind| value_has_type(value, kind)),
                _ => return Err(malformed(schema_path, "type must be a string or a string array")),
            };
            if !matches_type {
                return Ok(false);
            }
        }
        if let Some(constant) = object.get("const") {
            if value != constant {
                return Ok(false);
            }
        }
        if let Some(values) = object.get("enum") {
            let values = values
                .as_array()
                .ok_or_else(|| malformed(schema_path, "enum must be an array"))?;
            if !values.iter().any(|candidate| candidate == value) {
                return Ok(false);
            }
        }
        if let Some(reference) = string_keyword(object, "$ref", schema_path)? {
            return self.alternative_could_match(
                self.resolve_reference(reference, schema_path)?,
                value,
                reference,
                reference_depth + 1,
            );
        }
        Ok(true)
    }

    fn resolve_reference<'a>(
        &'a self,
        reference: &str,
        schema_path: &str,
    ) -> Result<&'a Value, ValidationError> {
        if !reference.starts_with('#') {
            return Err(error(
                ValidationCode::UnsupportedConstruct,
                "#",
                schema_path,
                format!("external JSON Schema reference {reference:?} is unsupported"),
            ));
        }
        self.raw.pointer(&reference[1..]).ok_or_else(|| {
            error(
                ValidationCode::Reference,
                "#",
                schema_path,
                format!("reference {reference:?} does not resolve"),
            )
        })
    }
}

fn is_schema_error(code: ValidationCode) -> bool {
    matches!(
        code,
        ValidationCode::Reference
            | ValidationCode::UnsupportedConstruct
            | ValidationCode::MalformedSchema
    )
}

fn value_has_type(value: &Value, kind: &str) -> bool {
    match kind {
        "null" => value.is_null(),
        "boolean" => value.is_boolean(),
        "object" => value.is_object(),
        "array" => value.is_array(),
        "string" => value.is_string(),
        "number" => value.is_number(),
        "integer" => value
            .as_number()
            .is_some_and(|number| number.as_i64().is_some() || number.as_u64().is_some()),
        _ => false,
    }
}

fn string_keyword<'a>(
    schema: &'a Map<String, Value>,
    keyword: &str,
    schema_path: &str,
) -> Result<Option<&'a str>, ValidationError> {
    schema
        .get(keyword)
        .map(|value| {
            value
                .as_str()
                .ok_or_else(|| malformed(schema_path, &format!("{keyword} must be a string")))
        })
        .transpose()
}

fn number_keyword<'a>(
    schema: &'a Map<String, Value>,
    keyword: &str,
    schema_path: &str,
) -> Result<Option<&'a Number>, ValidationError> {
    schema
        .get(keyword)
        .map(|value| {
            value
                .as_number()
                .ok_or_else(|| malformed(schema_path, &format!("{keyword} must be a number")))
        })
        .transpose()
}

fn compare_json_numbers(
    left: &Number,
    right: &Number,
    schema_path: &str,
) -> Result<Ordering, ValidationError> {
    match (ExactInteger::from_number(left), ExactInteger::from_number(right)) {
        (Some(left), Some(right)) => Ok(left.cmp(right)),
        _ => left
            .as_f64()
            .zip(right.as_f64())
            .and_then(|(left, right)| left.partial_cmp(&right))
            .ok_or_else(|| malformed(schema_path, "number cannot be compared")),
    }
}

#[derive(Clone, Copy)]
enum ExactInteger {
    Signed(i64),
    Unsigned(u64),
}

impl ExactInteger {
    fn from_number(number: &Number) -> Option<Self> {
        if let Some(number) = number.as_i64() {
            Some(Self::Signed(number))
        } else {
            number.as_u64().map(Self::Unsigned)
        }
    }

    fn cmp(self, other: Self) -> Ordering {
        match (self, other) {
            (Self::Signed(left), Self::Signed(right)) => left.cmp(&right),
            (Self::Unsigned(left), Self::Unsigned(right)) => left.cmp(&right),
            (Self::Signed(left), Self::Unsigned(_)) if left < 0 => Ordering::Less,
            (Self::Signed(left), Self::Unsigned(right)) => (left as u64).cmp(&right),
            (Self::Unsigned(_), Self::Signed(right)) if right < 0 => Ordering::Greater,
            (Self::Unsigned(left), Self::Signed(right)) => left.cmp(&(right as u64)),
        }
    }
}

fn usize_keyword(
    schema: &Map<String, Value>,
    keyword: &str,
    schema_path: &str,
) -> Result<Option<usize>, ValidationError> {
    schema
        .get(keyword)
        .map(|value| {
            value
                .as_u64()
                .and_then(|value| usize::try_from(value).ok())
                .ok_or_else(|| malformed(schema_path, &format!("{keyword} must be a nonnegative integer")))
        })
        .transpose()
}

fn malformed(schema_path: &str, detail: impl Into<String>) -> ValidationError {
    error(
        ValidationCode::MalformedSchema,
        "#",
        schema_path,
        detail.into(),
    )
}

fn error(
    code: ValidationCode,
    instance_path: &str,
    schema_path: &str,
    detail: impl Into<String>,
) -> ValidationError {
    ValidationError {
        code,
        instance_path: instance_path.to_owned(),
        schema_path: schema_path.to_owned(),
        detail: detail.into(),
    }
}

fn instance_child(parent: &str, child: &str) -> String {
    format!("{parent}/{}", escape_json_pointer(child))
}

fn schema_child(parent: &str, child: &str) -> String {
    format!("{parent}/{}", escape_json_pointer(child))
}

fn escape_json_pointer(value: &str) -> String {
    value.replace('~', "~0").replace('/', "~1")
}

fn canonical_json(value: &Value) -> Vec<u8> {
    let mut value = value.clone();
    sort_json(&mut value);
    serde_json::to_vec(&value).expect("serde_json values are serializable")
}

fn sort_json(value: &mut Value) {
    match value {
        Value::Object(object) => {
            let mut entries = std::mem::take(object).into_iter().collect::<Vec<_>>();
            entries.sort_by(|left, right| left.0.cmp(&right.0));
            for (_, value) in &mut entries {
                sort_json(value);
            }
            object.extend(entries);
        }
        Value::Array(values) => {
            for value in values {
                sort_json(value);
            }
        }
        _ => {}
    }
}

fn sha256_hex(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn schema() -> ApiSchema {
        let raw = serde_json::from_str(include_str!(
            "../../../fixtures/herdr/herdr-api.schema.json"
        ))
        .unwrap();
        ApiSchema::parse(raw).unwrap()
    }

    fn inline_method_schema(params: Value) -> ApiSchema {
        ApiSchema::parse(serde_json::json!({
            "protocol": 20,
            "schema_version": 1,
            "schemas": {
                "request": {
                    "oneOf": [{
                        "properties": {
                            "method": {"const": "test.method"},
                            "params": params,
                        },
                    }],
                },
            },
        }))
        .unwrap()
    }


    #[test]
    fn rejects_missing_required_parameter() {
        let error = schema()
            .validate_method("pane.resize", &serde_json::json!({"amount": 0.1}))
            .unwrap_err();
        assert_eq!(error.code, ValidationCode::Required);
        assert!(error.detail.contains("direction"));
    }

    #[test]
    fn rejects_undeclared_configured_parameter() {
        let error = schema()
            .validate_method(
                "pane.resize",
                &serde_json::json!({"direction": "right", "unexpected": true}),
            )
            .unwrap_err();
        assert_eq!(error.code, ValidationCode::AdditionalProperty);
        assert_eq!(error.instance_path, "#/unexpected");
    }

    #[test]
    fn unknown_construct_blocks_the_affected_request() {
        let mut raw: Value = serde_json::from_str(include_str!(
            "../../../fixtures/herdr/herdr-api.schema.json"
        ))
        .unwrap();
        raw.pointer_mut("/schemas/request/$defs/PaneResizeParams")
            .unwrap()
            .as_object_mut()
            .unwrap()
            .insert("unevaluatedProperties".to_owned(), Value::Bool(false));
        let schema = ApiSchema::parse(raw).unwrap();
        let error = schema
            .validate_method("pane.resize", &serde_json::json!({"direction": "right"}))
            .unwrap_err();
        assert_eq!(error.code, ValidationCode::UnsupportedConstruct);
    }

    #[test]
    fn unused_inline_method_with_invalid_pattern_does_not_block_pane_resize() {
        let mut raw: Value = serde_json::from_str(include_str!(
            "../../../fixtures/herdr/herdr-api.schema.json"
        ))
        .unwrap();
        raw.pointer_mut("/schemas/request/oneOf")
            .unwrap()
            .as_array_mut()
            .unwrap()
            .push(serde_json::json!({
                "properties": {
                    "method": {"const": "future.inline"},
                    "params": {
                        "type": "object",
                        "properties": {
                            "value": {"type": "string", "pattern": "["},
                        },
                    },
                },
            }));
        let schema = ApiSchema::parse(raw).unwrap();
        assert!(schema
            .validate_method(
                "pane.resize",
                &serde_json::json!({"direction": "right", "amount": 0.1}),
            )
            .is_ok());
    }

    #[test]
    fn relevant_unsupported_one_of_branch_fails_closed() {
        let schema = inline_method_schema(serde_json::json!({
            "oneOf": [
                {"type": "integer"},
                {"type": "string", "unevaluatedProperties": false},
            ],
        }));
        let error = schema
            .validate_method("test.method", &serde_json::json!("value"))
            .unwrap_err();
        assert_eq!(error.code, ValidationCode::UnsupportedConstruct);
    }

    #[test]
    fn incompatible_unsupported_one_of_branch_does_not_block_matching_alternative() {
        let schema = inline_method_schema(serde_json::json!({
            "oneOf": [
                {"type": "integer"},
                {"type": "string", "unevaluatedProperties": false},
            ],
        }));
        assert!(schema
            .validate_method("test.method", &serde_json::json!(1))
            .is_ok());
    }

    #[test]
    fn compares_adjacent_large_integers_exactly() {
        let schema = inline_method_schema(serde_json::json!({
            "type": "integer",
            "minimum": 9_007_199_254_740_993u64,
        }));
        let error = schema
            .validate_method("test.method", &serde_json::json!(9_007_199_254_740_992u64))
            .unwrap_err();
        assert_eq!(error.code, ValidationCode::Minimum);
    }

    #[test]
    fn compiles_a_used_pattern_once_per_schema_lifetime() {
        let schema = inline_method_schema(serde_json::json!({
            "type": "string",
            "pattern": "^[a-z]+$",
        }));
        for value in ["alpha", "beta"] {
            assert!(schema
                .validate_method("test.method", &serde_json::json!(value))
                .is_ok());
        }
        let pattern_count = match schema.patterns.lock() {
            Ok(patterns) => patterns.len(),
            Err(poisoned) => poisoned.into_inner().len(),
        };
        assert_eq!(pattern_count, 1);
    }

    #[test]
    fn canonical_request_hash_ignores_member_order_and_response_drift() {
        let first = ApiSchema::parse(serde_json::json!({
            "protocol": 20,
            "schema_version": 1,
            "schemas": {
                "request": {"oneOf": []},
                "success_response": {"type": "string"},
            },
        }))
        .unwrap();
        let second = ApiSchema::parse(serde_json::json!({
            "schemas": {
                "success_response": {"type": "integer"},
                "request": {"oneOf": []},
            },
            "schema_version": 1,
            "protocol": 20,
        }))
        .unwrap();
        assert_eq!(
            first.canonical_request_sha256(),
            second.canonical_request_sha256()
        );
    }
}
