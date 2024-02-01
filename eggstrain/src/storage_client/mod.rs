//! Right now we have this in a submodule `storage_client.rs`, but the IO service
//! team would probably create a crate and we could import it easily into our `Cargo.toml` file

use datafusion::execution::SendableRecordBatchStream;

use std::sync::Arc;
use datafusion::common::arrow::array::{Int32Array, RecordBatch};
use datafusion::common::arrow::datatypes::{DataType, Field, Schema};


// Placeholder types to let this compile
type ColumnId = String;
type TableId = String;
type RecordId = usize;

/// For now, pretend that this is an opaque type that the
/// I/O Service team will provide to us in a crate.
/// This type should be `Sync` as well, to support
/// multiple instances of a `StorageClient`.
pub struct StorageClient;

/// Have some way to request specific types of data.
/// As long as it comes back as a `RecordBatch`,
/// we should be fine to have any type of request here.
pub enum BlobData {
    Table(TableId),
    Columns(TableId, Box<[ColumnId]>),
    Tuple(RecordId),
}

impl StorageClient {
    /// Have some sort of way to create a `StorageClient` on our local node.
    pub fn new(_id: usize) -> Self {
        Self
    }

    /// The only other function we need exposed would be a way to actually get data.
    /// What we should get is a stream of `Recordbatch`s, which is just Apache Arrow
    /// data in memory.
    ///
    /// The executor node really should not know what the underlying data is on the Blob data store.
    /// In our case it is Parquet, but since the Execution Engine is not in charge or loading
    /// those Parquet files, it should just receive it as in-memory Arrow data
    ///
    /// Note that we will likely re-export the `SendableRecordBatchRecord` from DataFusion
    /// and use that as the return type instead
    pub async fn request_data(
        &self,
        _request: BlobData,
    ) -> SendableRecordBatchStream {
        todo!()
    }

    pub async fn sample_request_data(_request: BlobData) -> SendableRecordBatchStream {
        todo!("Return some sample data")
    }

    /// https://docs.rs/datafusion/latest/datafusion/common/arrow/array/struct.RecordBatch.html
    pub async fn request_synchronous_data() -> RecordBatch {
        let id_array = Int32Array::from(vec![1, 2, 3, 4, 5]);
        let schema = Schema::new(vec![
            Field::new("id", DataType::Int32, false)
        ]);

        RecordBatch::try_new(
            Arc::new(schema),
            vec![Arc::new(id_array)]
        ).unwrap()
    }
}