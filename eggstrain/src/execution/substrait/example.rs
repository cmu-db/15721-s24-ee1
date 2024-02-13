//! Want to be able to parse substrait into Arrow schema

use arrow::datatypes::{DataType, Field, Schema};
use substrait::proto::rel::RelType;
use substrait::proto::ReadRel;

/// We want the plan to look like this:
///
/// ```
/// |-+ Aggregate({sales = sum(quantity_price)}, group_by=(product_name, product_id))
///  |-+ InnerJoin(on=orders.product_id = products.product_id)
///   |- ReadTable(orders)
///     |-+ Filter(INDEX_IN("Computers", categories) IS NULL)
///      |- ReadTable(products)
/// ```
pub fn tpch_query1() -> RelType {
    // Go bottom up
    let read = ReadRel {
        common: None,
        base_schema: todo!(),
        filter: todo!(),
        best_effort_filter: todo!(),
        projection: todo!(),
        advanced_extension: todo!(),
        read_type: todo!(),
    };

    todo!()
}

pub fn tpch_lineitem_schema() -> Schema {
    let fields = vec![
        Field::new("L_ORDERKEY", DataType::UInt64, false),
        Field::new("L_PARTKEY", DataType::UInt64, false),
        Field::new("L_SUPPKEY", DataType::UInt64, false),
        Field::new("L_LINENUMBER", DataType::Int64, false),
        Field::new("L_QUANTITY", DataType::Decimal128(16, 16), false),
        Field::new("L_EXTENDEDPRICE", DataType::Decimal128(16, 16), false),
        Field::new("L_DISCOUNT", DataType::Decimal128(16, 16), false),
        Field::new("L_TAX", DataType::Decimal128(16, 16), false),
        Field::new("L_RETURNFLAG", DataType::Boolean, false),
        Field::new("L_LINESTATUS", DataType::Boolean, false),
        Field::new("L_SHIPDATE", DataType::Date32, false),
        Field::new("L_COMMITDATE", DataType::Date32, false),
        Field::new("L_RECEIPTDATE", DataType::Date32, false),
        Field::new("L_SHIPINSTRUCT", DataType::FixedSizeBinary(25), false),
        Field::new("L_SHIPMODE", DataType::FixedSizeBinary(10), false),
        Field::new("L_COMMENT", DataType::FixedSizeBinary(44), false),
    ];

    Schema::new(fields)
}
