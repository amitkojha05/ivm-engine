use anyhow::Context;
use ivm_core::{Row, Value};
use prost_reflect::{DescriptorPool, DynamicMessage, ReflectMessage};
use std::collections::HashMap;

pub struct ProtoDecoder {
    pool: DescriptorPool,
    message_name: String,
}

impl ProtoDecoder {
    pub fn new(descriptor_set_bytes: &[u8], message_name: &str) -> anyhow::Result<Self> {
        let pool = DescriptorPool::decode(descriptor_set_bytes)
            .context("Failed to parse Protobuf descriptor set")?;
        Ok(Self {
            pool,
            message_name: message_name.into(),
        })
    }

    pub fn decode(&self, payload: &[u8]) -> anyhow::Result<Row> {
        let msg_desc = self
            .pool
            .get_message_by_name(&self.message_name)
            .context("Message type not found in descriptor pool")?;
        let dyn_msg = DynamicMessage::decode(msg_desc, payload)
            .context("Failed to decode Protobuf payload")?;

        let mut map = HashMap::new();
        for field in dyn_msg.descriptor().fields() {
            let value = dyn_msg.get_field(&field);
            let ivm_value = match value.as_ref() {
                prost_reflect::Value::I32(n) => Value::Int(*n as i64),
                prost_reflect::Value::I64(n) => Value::Int(*n),
                prost_reflect::Value::U32(n) => Value::Int(*n as i64),
                prost_reflect::Value::U64(n) => Value::Int(*n as i64),
                prost_reflect::Value::String(s) => Value::Str(s.clone()),
                prost_reflect::Value::Bool(b) => Value::Bool(*b),
                _ => Value::Null,
            };
            map.insert(field.name().to_string(), ivm_value);
        }
        Ok(Row(map))
    }
}
