use std::io;
use std::path::Path;

use io::BufWriter;
use io::Write;

use arrow::datatypes::Schema;
use arrow::datatypes::SchemaRef;

use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

pub fn file2schema(f: std::fs::File) -> Result<SchemaRef, io::Error> {
    ParquetRecordBatchReaderBuilder::try_new(f)
        .map(|b| b.schema().clone())
        .map_err(io::Error::other)
}

pub fn name2schema<P>(filename: P) -> Result<SchemaRef, io::Error>
where
    P: AsRef<Path>,
{
    let f = std::fs::File::open(filename)?;
    file2schema(f)
}

pub fn print_schema_simple(s: &SchemaRef) -> Result<(), io::Error> {
    let sch: &Schema = s.as_ref();
    println!("{sch}");
    Ok(())
}

pub fn print_schema_as_json(s: &SchemaRef) -> Result<(), io::Error> {
    let sch: &Schema = s.as_ref();
    let o = io::stdout();
    let mut ol = o.lock();
    {
        let mut bw = BufWriter::new(&mut ol);
        serde_json::to_writer(&mut bw, sch)?;
        bw.flush()?;
    }
    ol.flush()?;
    Ok(())
}
