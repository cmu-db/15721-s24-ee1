use arrow::{datatypes::SchemaRef, record_batch::RecordBatch};

/// Make this an opaque type that can be used later, we may want to make this contiguous and then
/// use offsets instead.
#[derive(Clone, Copy)]
pub struct RecordIndex {
    index: u32,
    row: u32, // by default this is just 0
}

/// TODO docs
impl RecordIndex {
    pub fn new(index: u32, row: u32) -> Self {
        Self { index, row }
    }

    pub fn update_row(&mut self, row: u32) {
        self.row = row;
    }
}

pub struct RecordBuffer {
    schema: SchemaRef,       // The schema for all of the record batches
    inner: Vec<RecordBatch>, // make this contiguous
}

impl RecordBuffer {
    pub fn new(schema: SchemaRef) -> Self {
        Self {
            schema,
            inner: vec![],
        }
    }

    pub fn with_capacity(schema: SchemaRef, capacity: usize) -> Self {
        Self {
            schema,
            inner: Vec::with_capacity(capacity),
        }
    }

    pub fn insert(&mut self, batch: RecordBatch) -> RecordIndex {
        assert_eq!(
            self.schema,
            batch.schema(),
            "Trying to insert a RecordBatch into a RecordBuffer with the incorrect schema"
        );
        assert!(
            (self.inner.len() as u32) < u32::MAX - 1,
            "Maximum size for a RecordBuffer is u32::MAX"
        );

        self.inner.push(batch);

        RecordIndex {
            index: (self.inner.len() - 1) as u32,
            row: 0,
        }
    }

    /// Retrieve the batch and row number associated with the RecordIndex
    pub fn get(&self, index: RecordIndex) -> Option<(&RecordBatch, u32)> {
        if (index.index as usize) >= self.inner.len() {
            return None;
        }

        Some((&self.inner[index.index as usize], index.row))
    }
}
