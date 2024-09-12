use arrow::row::{Row, RowConverter, Rows, SortField};
use arrow::{datatypes::SchemaRef, record_batch::RecordBatch};
use datafusion_common::Result;

/// Make this an opaque type that can be used later, we may want to make this contiguous and then
/// use offsets instead.
#[derive(Clone, Copy)]
pub struct RecordIndex {
    index: u32, // index into the vector of rows
    row: u32,   // index into the row group
}

/// TODO docs
impl RecordIndex {
    pub fn new(index: u32, row: u32) -> Self {
        Self { index, row }
    }

    // Use functional style due to easy copying
    pub fn with_row(&self, row: u32) -> Self {
        Self {
            index: self.index,
            row,
        }
    }
}

pub fn schema_to_fields(schema: SchemaRef) -> Vec<SortField> {
    schema
        .fields()
        .iter()
        .map(|f| SortField::new(f.data_type().clone()))
        .collect::<Vec<_>>()
}

pub struct RecordBuffer {
    schema: SchemaRef,
    converter: RowConverter,
    inner: Vec<Rows>, // vector of row groups
}

impl RecordBuffer {
    pub fn new(schema: SchemaRef) -> Self {
        let fields = schema_to_fields(schema.clone());
        Self {
            schema,
            converter: RowConverter::new(fields).expect("Unable to create a RowConverter"),
            inner: vec![],
        }
    }

    pub fn with_capacity(schema: SchemaRef, capacity: usize) -> Self {
        let fields = schema_to_fields(schema.clone());
        Self {
            schema,
            converter: RowConverter::new(fields).expect("Unable to create a RowConverter"),
            inner: Vec::with_capacity(capacity),
        }
    }

    pub fn converter(&self) -> &RowConverter {
        &self.converter
    }

    pub fn record_batch_to_rows(&self, batch: RecordBatch) -> Result<Rows> {
        // `Ok` to make use of `?` behavior
        Ok(self.converter.convert_columns(batch.columns())?)
    }

    pub fn insert(&mut self, batch: RecordBatch) -> Result<RecordIndex> {
        assert_eq!(
            self.schema,
            batch.schema(),
            "Trying to insert a RecordBatch into a RecordBuffer with the incorrect schema"
        );
        assert!(
            (self.inner.len() as u32) < u32::MAX - 1,
            "Maximum size for a RecordBuffer is u32::MAX"
        );

        let rows = self.record_batch_to_rows(batch)?;
        self.inner.push(rows);

        Ok(RecordIndex {
            index: (self.inner.len() - 1) as u32,
            row: 0,
        })
    }

    /// Retrieve the row group and row number associated with the RecordIndex
    pub fn get_group(&self, index: RecordIndex) -> Option<(&Rows, u32)> {
        if (index.index as usize) >= self.inner.len() {
            return None;
        }

        Some((&self.inner[index.index as usize], index.row))
    }

    /// Retrieve row / tuple associated with the RecordIndex
    pub fn get(&self, index: RecordIndex) -> Option<Row> {
        if (index.index as usize) >= self.inner.len() {
            return None;
        }

        Some(self.inner[index.index as usize].row(index.row as usize))
    }
}
