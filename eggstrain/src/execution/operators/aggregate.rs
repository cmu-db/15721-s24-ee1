use super::{Operator, UnaryOperator};
use async_trait::async_trait;

/// TODO docs
pub(crate) struct Aggregate {
    children: Vec<Arc<dyn ExecutionPlan>>,
}

/// TODO docs
impl Aggregate {
    pub(crate) fn new() -> Self {
        todo!();
    }

    fn aggregate_in_mem(&self, rb: RecordBatch) -> Result<RecordBatch> {
        todo!();
    }
}

/// TODO docs
impl Operator for Aggregate {
    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        todo!();
    }
}

/// TODO docs
#[async_trait]
impl UnaryOperator for Aggregte {
    type In = RecordBatch;
    type Out = RecordBatch;

    fn into_unary(self) -> Arc<dyn UnaryOperator<In = Self::In, Out = Self::Out>> {
        todo!();
    }

    async fn execute(
        &self,
        rx: broadcast::Receiver<Self::In>,
        tx: broadcast::Sender<Self::Out>,
    ) {
        let mut batches = vec![];
        loop {
            todo!();
        }
        todo!();
    }
}
