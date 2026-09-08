use serde::{Deserialize, Serialize};

pub const STREAM_MAGIC: &[u8; 4] = b"DMP1";
pub const STREAM_VERSION: u16 = 1;

#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RecordType {
    Header = 0x01,
    PreData = 0x02,
    TableSchema = 0x03,
    TableDataSlice = 0x04,
    Sequence = 0x05,
    PostData = 0x06,
    Routine = 0x07,
    Trailer = 0xFF,
}

impl TryFrom<u8> for RecordType {
    type Error = u8;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0x01 => Ok(RecordType::Header),
            0x02 => Ok(RecordType::PreData),
            0x03 => Ok(RecordType::TableSchema),
            0x04 => Ok(RecordType::TableDataSlice),
            0x05 => Ok(RecordType::Sequence),
            0x06 => Ok(RecordType::PostData),
            0x07 => Ok(RecordType::Routine),
            0xFF => Ok(RecordType::Trailer),
            other => Err(other),
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct StreamHeader {
    pub version: u16,
    pub engine: String,
    pub database: String,
    pub server_version: String,
    pub dumper_version: String,
    pub start_time: i64,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct PreDataRecord {
    pub name: String,
    pub sql: String,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct TableColumnMeta {
    pub name: String,
    pub data_type: String,
    pub is_nullable: bool,
    pub default_val: Option<String>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct TableSchemaRecord {
    pub schema_name: String,
    pub table_name: String,
    pub columns: Vec<TableColumnMeta>,
    pub create_sql: String,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct TableDataSliceRecord {
    pub schema_name: String,
    pub table_name: String,
    pub slice_seq: u64,
    pub is_last: bool,
    pub data: Vec<u8>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct SequenceRecord {
    pub schema_name: String,
    pub sequence_name: String,
    pub last_value: i64,
    pub is_called: bool,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct PostDataRecord {
    pub schema_name: String,
    pub table_name: String,
    pub name: String,
    pub sql: String,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct RoutineRecord {
    pub schema_name: String,
    pub name: String,
    pub routine_type: String, // VIEW, FUNCTION, TRIGGER
    pub sql: String,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct StreamTrailer {
    pub total_records: u64,
    pub total_logical_bytes: u64,
    pub stream_hash_hex: String,
}

#[derive(Debug, Clone)]
pub enum StreamRecord {
    Header(StreamHeader),
    PreData(PreDataRecord),
    TableSchema(TableSchemaRecord),
    TableDataSlice(TableDataSliceRecord),
    Sequence(SequenceRecord),
    PostData(PostDataRecord),
    Routine(RoutineRecord),
    Trailer(StreamTrailer),
}
