use crate::BATCH_SIZE;

use super::{Operator, UnaryOperator};
use arrow::compute::{concat_batches, lexsort_to_indices, take};
use arrow::{datatypes::SchemaRef, record_batch::RecordBatch};
use async_trait::async_trait;
use datafusion::physical_expr::PhysicalSortExpr;
use datafusion::physical_plan::{sorts::sort::SortExec, ExecutionPlan};
use datafusion_common::Result;
use std::sync::Arc;
use tokio::sync::broadcast;
use tokio::sync::broadcast::error::RecvError;

/// TODO docs
pub(crate) struct Sort {
    sort_expr: Vec<PhysicalSortExpr>,
    input_schema: SchemaRef, // TODO
    children: Vec<Arc<dyn ExecutionPlan>>,
    limit_size: Option<usize>,
}

/// TODO docs
impl Sort {
    pub(crate) fn new(sort_plan: &SortExec) -> Self {
        Self {
            sort_expr: Vec::from(sort_plan.expr()),
            input_schema: sort_plan.children()[0].schema(),
            children: sort_plan.children(),
            limit_size: sort_plan.fetch(),
        }
    }

    fn sort_in_mem(&self, rb: RecordBatch) -> Result<RecordBatch> {
        assert_eq!(rb.schema(), self.input_schema);

        let expressions = self.sort_expr.clone();

        let sort_columns = expressions
            .iter()
            .map(|expr| expr.evaluate_to_sort_column(&rb))
            .collect::<Result<Vec<_>>>()?;

        let indices = lexsort_to_indices(&sort_columns, self.limit_size)?;

        let columns = rb
            .columns()
            .iter()
            .map(|c| take(c.as_ref(), &indices, None))
            .collect::<Result<_, _>>()?;

        Ok(RecordBatch::try_new(rb.schema(), columns)?)
        //TODO: do we need to drop rb here or will that happen on its own?
        //drop(rb);
    }
}

/// TODO docs
impl Operator for Sort {
    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        self.children.clone()
    }
}

/// TODO docs
#[async_trait]
impl UnaryOperator for Sort {
    type In = RecordBatch;
    type Out = RecordBatch;

    fn into_unary(self) -> Arc<dyn UnaryOperator<In = Self::In, Out = Self::Out>> {
        Arc::new(self)
    }

    async fn execute(
        &self,
        mut rx: broadcast::Receiver<Self::In>,
        tx: broadcast::Sender<Self::Out>,
    ) {
        let mut batches = vec![];
        loop {
            match rx.recv().await {
                Ok(batch) => {
                    batches.push(batch);
                }
                Err(e) => match e {
                    RecvError::Closed => break,
                    RecvError::Lagged(_) => todo!(),
                },
            }
        }

        let merged_batch = concat_batches(&self.input_schema, &batches);
        match merged_batch {
            Ok(merged_batch) => {
                let sorted_batch = self.sort_in_mem(merged_batch).unwrap();
                let mut current = 0;
                let total_rows = sorted_batch.num_rows();
                while current + BATCH_SIZE < total_rows {
                    let batch_to_send = sorted_batch.slice(current, BATCH_SIZE);
                    tx.send(batch_to_send)
                        .expect("Unable to send the sorted batch");
                    current += BATCH_SIZE;
                }
                let batch_to_send = sorted_batch.slice(current, total_rows - current);
                tx.send(batch_to_send)
                    .expect("Unable to send the last sorted batch");

                // TODO: do I have to call drop here manually or will rust take care of it?
                // drop(sorted_batch);
            }
            Err(_) => todo!("Could not concat the batches for sorting"),
        }
    }
}
