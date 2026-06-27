use std::collections::HashMap;

use anyhow::{Context, Result};
use apache_avro::types::Value as AvroValue;
use ivm_core::{Row, Value};

/// Decode an Avro-encoded payload into a Row using a JSON schema string.
pub fn decode_avro(payload: &[u8], schema_json: &str) -> Result<Row> {
    let schema = apache_avro::Schema::parse_str(schema_json)?;
    let value = apache_avro::from_avro_datum(&schema, &mut &payload[..], None)?;
    avro_value_to_row(&value)
}

/// Encode a Row to Avro bytes using a JSON schema string.
pub fn encode_avro(row: &Row, schema_json: &str) -> Result<Vec<u8>> {
    let schema = apache_avro::Schema::parse_str(schema_json)?;
    let avro_val = row_to_avro_value(row, &schema)?;
    apache_avro::to_avro_datum(&schema, avro_val).context("Avro encode failed")
}

pub fn avro_value_to_row(value: &AvroValue) -> Result<Row> {
    let mut map = HashMap::new();
    if let AvroValue::Record(fields) = value {
        for (name, val) in fields {
            map.insert(name.clone(), avro_scalar_to_value(val));
        }
    }
    Ok(Row(map))
}

fn avro_scalar_to_value(val: &AvroValue) -> Value {
    match val {
        AvroValue::Null => Value::Null,
        AvroValue::Boolean(b) => Value::Bool(*b),
        AvroValue::Int(i) => Value::Int(*i as i64),
        AvroValue::Long(l) => Value::Int(*l),
        AvroValue::String(s) => Value::Str(s.clone()),
        AvroValue::Union(_, inner) => avro_scalar_to_value(inner),
        other => Value::Str(format!("{other:?}")),
    }
}

fn row_to_avro_value(row: &Row, schema: &apache_avro::Schema) -> Result<AvroValue> {
    let apache_avro::Schema::Record(record) = schema else {
        anyhow::bail!("Expected record schema");
    };
    let fields: Vec<(String, AvroValue)> = record
        .fields
        .iter()
        .filter_map(|f| {
            row.0.get(&f.name).map(|v| {
                (
                    f.name.clone(),
                    match v {
                        Value::Null => AvroValue::Null,
                        Value::Bool(b) => AvroValue::Boolean(*b),
                        Value::Int(i) => AvroValue::Long(*i),
                        Value::Str(s) => AvroValue::String(s.clone()),
                    },
                )
            })
        })
        .collect();
    Ok(AvroValue::Record(fields))
}

/// Schema Registry client wrapper for fetching subject schemas.
pub struct SchemaRegistry {
    url: String,
    client: reqwest::Client,
}

impl SchemaRegistry {
    pub fn new(url: impl Into<String>) -> Self {
        Self {
            url: url.into(),
            client: reqwest::Client::new(),
        }
    }

    pub async fn fetch_schema(&self, subject: &str) -> Result<String> {
        let url = format!("{}/subjects/{}/versions/latest", self.url, subject);
        let resp = self
            .client
            .get(&url)
            .send()
            .await
            .context("Schema registry request failed")?
            .json::<serde_json::Value>()
            .await
            .context("Failed to parse schema registry response")?;
        resp["schema"]
            .as_str()
            .map(|s| s.to_string())
            .context("Missing schema field in registry response")
    }

    pub async fn decode_with_registry(
        &self,
        subject: &str,
        payload: &[u8],
    ) -> Result<Row> {
        let schema_json = self.fetch_schema(subject).await?;
        decode_avro(strip_confluent_header(payload), &schema_json)
    }
}

/// Confluent wire format: magic byte + 4-byte schema ID + Avro payload.
fn strip_confluent_header(payload: &[u8]) -> &[u8] {
    if payload.len() > 5 && payload[0] == 0 {
        &payload[5..]
    } else {
        payload
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const SCHEMA: &str = r#"{
        "type": "record",
        "name": "User",
        "fields": [
            {"name": "id", "type": "long"},
            {"name": "name", "type": "string"}
        ]
    }"#;

    #[test]
    fn roundtrip_avro_row() {
        let row = Row(HashMap::from([
            ("id".into(), Value::Int(42)),
            ("name".into(), Value::Str("test".into())),
        ]));
        let encoded = encode_avro(&row, SCHEMA).unwrap();
        let decoded = decode_avro(&encoded, SCHEMA).unwrap();
        assert_eq!(decoded.get_int("id"), 42);
        assert_eq!(decoded.get_str("name"), Some("test"));
    }
}
