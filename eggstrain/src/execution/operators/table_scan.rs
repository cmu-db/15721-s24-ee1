//! Special type of operator because it will not implement `UnaryOperator` or `BinaryOperator`

use substrait::proto::ReadRel;

#[derive(Default)]
struct TableScan {
    plan: ReadRel,
}
