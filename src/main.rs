use std::io;
use std::process::ExitCode;

use arrow::datatypes::SchemaRef;

use rs_parquet_schema_show::name2schema;
use rs_parquet_schema_show::print_schema_as_json;

fn env2parquet_filename() -> Result<String, io::Error> {
    std::env::var("ENV_PARQUET_FILE_NAME")
        .map_err(|e| format!("env var ENV_PARQUET_FILE_NAME missing: {e}"))
        .map_err(io::Error::other)
}

fn env2schema() -> Result<SchemaRef, io::Error> {
    let pname: String = env2parquet_filename()?;
    name2schema(pname)
}

fn sub() -> Result<(), io::Error> {
    let s: SchemaRef = env2schema()?;
    print_schema_as_json(&s)?;
    Ok(())
}

fn main() -> ExitCode {
    match sub() {
        Ok(_) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("{e}");
            ExitCode::FAILURE
        }
    }
}
