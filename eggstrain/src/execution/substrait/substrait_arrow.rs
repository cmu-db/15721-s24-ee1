use arrow::datatypes::{DataType, Schema};
use std::sync::Arc;
use substrait::proto::{Type, NamedStruct};

/// https://docs.rs/substrait/latest/substrait/proto/struct.NamedStruct.html
/// https://docs.rs/substrait/latest/substrait/proto/type/struct.Struct.html
pub fn schema_translate(substrait_schema: NamedStruct) -> Arc<Schema> {
    for (i, name) in substrait_schema.names.iter().enumerate() {
        todo!()
    }

    todo!()
}

/// https://docs.rs/substrait/latest/substrait/proto/struct.Type.html
/// https://docs.rs/arrow/latest/arrow/datatypes/enum.DataType.html
pub fn type_translate(typ: Type) -> DataType {
    todo!()
}

