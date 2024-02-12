//! TODO the scheduler team will provide this to us

use substrait::proto::{rel::*, ReadRel};

/// https://docs.rs/substrait/latest/substrait/proto/rel/enum.RelType.html
/// https://docs.rs/substrait/latest/substrait/proto/struct.ReadRel.html
pub async fn sample_plan() -> RelType {
    RelType::Read(Box::new(ReadRel {
        common: None,
        base_schema: None,
        filter: None,
        best_effort_filter: None,
        projection: None,
        advanced_extension: None,
        read_type: None,
    }))
}
