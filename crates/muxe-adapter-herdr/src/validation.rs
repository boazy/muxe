use std::collections::BTreeSet;

use muxe_core::{ConfigField, ConfigValue, ConfigValueKind, ContextType, NativeActionCandidate};
use serde_json::{Map, Number, Value};

use crate::{
    ApiSchema, ValidationCode, ValidationError,
    generated::{MethodMetadata, MethodTransport, metadata_for_native_type},
};

#[derive(Debug)]
pub struct CandidateValidationError {
    pub field: Option<String>,
    pub error: ValidationError,
}

pub fn validate_candidate(
    schema: &ApiSchema,
    candidate: &NativeActionCandidate,
) -> Result<&'static MethodMetadata, CandidateValidationError> {
    let metadata = metadata_for_native_type(&candidate.type_name).ok_or_else(|| CandidateValidationError {
        field: None,
        error: missing_method_error(&candidate.type_name),
    })?;
    if metadata.transport != MethodTransport::Unary {
        return Err(CandidateValidationError {
            field: None,
            error: ValidationError {
                code: ValidationCode::UnsupportedConstruct,
                instance_path: "#".to_owned(),
                schema_path: "#/schemas/request".to_owned(),
                detail: format!("Herdr method {:?} is an event stream", metadata.method),
            },
        });
    }
    let params = fields_to_json(&candidate.fields).map_err(|error| CandidateValidationError {
        field: None,
        error,
    })?;
    schema
        .validate_method(metadata.method, &Value::Object(params))
        .map_err(|error| CandidateValidationError {
            field: field_from_instance_path(&error.instance_path),
            error,
        })?;
    Ok(metadata)
}

pub fn fields_to_json(fields: &[ConfigField]) -> Result<Map<String, Value>, ValidationError> {
    let mut params = Map::new();
    let mut names = BTreeSet::new();
    for field in fields {
        let wire_name = yaml_to_wire_name(&field.name)?;
        if !names.insert(wire_name.clone()) {
            return Err(ValidationError {
                code: ValidationCode::AdditionalProperty,
                instance_path: format!("#/{wire_name}"),
                schema_path: "#/schemas/request".to_owned(),
                detail: format!("multiple YAML fields map to Herdr parameter {wire_name:?}"),
            });
        }
        params.insert(wire_name, value_to_json(&field.value)?);
    }
    Ok(params)
}

fn value_to_json(value: &ConfigValue) -> Result<Value, ValidationError> {
    Ok(match &value.kind {
        ConfigValueKind::Null => Value::Null,
        ConfigValueKind::Boolean(value) => Value::Bool(*value),
        ConfigValueKind::Integer(value) => Value::Number(Number::from(*value)),
        ConfigValueKind::Float(value) => Number::from_f64(*value).map(Value::Number).ok_or_else(|| {
            ValidationError {
                code: ValidationCode::Type,
                instance_path: "#".to_owned(),
                schema_path: "#/schemas/request".to_owned(),
                detail: "non-finite floating point values are not valid JSON".to_owned(),
            }
        })?,
        ConfigValueKind::String(value) => Value::String(value.clone()),
        ConfigValueKind::Context(reference) => context_placeholder(reference.expected_type()),
        ConfigValueKind::Sequence(values) => Value::Array(
            values
                .iter()
                .map(value_to_json)
                .collect::<Result<_, _>>()?,
        ),
        ConfigValueKind::Mapping(fields) => Value::Object(fields_to_json(fields)?),
    })
}

fn context_placeholder(context_type: ContextType) -> Value {
    match context_type {
        ContextType::UnsignedInteger => Value::Number(Number::from(0)),
        ContextType::HostKind => Value::String("herdr".to_owned()),
        ContextType::AbsolutePath => Value::String("/".to_owned()),
        _ => Value::String("muxe-context".to_owned()),
    }
}

fn yaml_to_wire_name(name: &str) -> Result<String, ValidationError> {
    if name.contains('_') {
        return Err(ValidationError {
            code: ValidationCode::AdditionalProperty,
            instance_path: format!("#/{name}"),
            schema_path: "#/schemas/request".to_owned(),
            detail: "Herdr YAML parameter names must use kebab-case, not underscores".to_owned(),
        });
    }
    Ok(name.replace('-', "_"))
}

fn field_from_instance_path(path: &str) -> Option<String> {
    path.strip_prefix("#/")
        .and_then(|path| path.split('/').next())
        .map(|field| field.replace('_', "-"))
}

fn missing_method_error(native_type: &str) -> ValidationError {
    ValidationError {
        code: ValidationCode::MissingMethod,
        instance_path: "#".to_owned(),
        schema_path: "#/schemas/request".to_owned(),
        detail: format!("unknown Herdr native action type {native_type:?}"),
    }
}
