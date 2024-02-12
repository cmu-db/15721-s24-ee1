use super::*;
use arrow::{
    array::RecordBatch,
    datatypes::{DataType, Field, Fields, Schema},
};
use async_trait::async_trait;
use std::sync::Arc;
use tokio::sync::broadcast::error::RecvError;

pub struct Project {
    pub schema: Arc<Schema>,
    /// TODO:
    /// https://docs.rs/substrait/latest/substrait/proto/struct.ProjectRel.html
    /// Need to make these expressions rather than a bunch of columns
    pub expressions: Vec<usize>,
}

impl Project {
    pub fn new(_schema: Arc<Schema>, expressions: Vec<usize>) -> Self {
        // TODO placeholder
        let field_a = Field::new("a", DataType::Int64, false);
        let schema = Arc::new(Schema::new(vec![field_a]));
        let res = Self {
            schema,
            expressions,
        };

        let fields = res.schema.fields();

        debug_assert!(res.is_valid_projection(fields));
        res
    }

    fn is_valid_projection(&self, fields: &Fields) -> bool {
        self.expressions.iter().all(|&col| col < fields.len())
    }

    fn project_record_batch(&self, batch: RecordBatch) -> RecordBatch {
        let schema = batch.schema();

        let projected = self
            .expressions
            .iter()
            .map(|&i| (schema.field(i).name(), batch.column(i).clone()));

        RecordBatch::try_from_iter(projected).expect("Unable to create the RecordBatch")
    }
}

#[async_trait]
impl UnaryOperator for Project {
    type In = RecordBatch;
    type Out = RecordBatch;

    fn into_unary(self) -> Box<dyn UnaryOperator<In = Self::In, Out = Self::Out>> {
        Box::new(self)
    }

    async fn execute(&self, mut rx: Receiver<Self::In>, tx: Sender<Self::Out>) {
        // For now assume that each record batch has the same type

        loop {
            match rx.recv().await {
                Ok(batch) => {
                    debug_assert!(batch.schema() == self.schema, "RecordBatch {:?} does not have the correct schema. Schema is {:?}, supposed to be {:?}", batch, batch.schema(), self.schema);
                    tx.send(self.project_record_batch(batch))
                        .expect("Sending failed");
                }
                Err(e) => match e {
                    RecvError::Closed => break,
                    RecvError::Lagged(_) => todo!(),
                },
            }
        }
    }
}
