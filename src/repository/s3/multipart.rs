use crate::repository::s3::client::S3Client;
use serde::Serialize;

#[derive(Serialize)]
pub struct CompletedPart {
    pub part_number: usize,
    pub etag: String,
}

pub struct MultipartUploadSession<'a> {
    pub client: &'a S3Client,
    pub key: String,
    pub upload_id: String,
    pub completed_parts: Vec<CompletedPart>,
}

impl<'a> MultipartUploadSession<'a> {
    pub fn new(client: &'a S3Client, key: String, upload_id: String) -> Self {
        Self {
            client,
            key,
            upload_id,
            completed_parts: Vec::new(),
        }
    }

    pub fn add_completed_part(&mut self, part_number: usize, etag: String) {
        self.completed_parts
            .push(CompletedPart { part_number, etag });
    }

    pub fn build_complete_xml(&self) -> String {
        let mut xml = String::from("<CompleteMultipartUpload>\n");
        for part in &self.completed_parts {
            xml.push_str(&format!(
                "  <Part>\n    <PartNumber>{}</PartNumber>\n    <ETag>{}</ETag>\n  </Part>\n",
                part.part_number, part.etag
            ));
        }
        xml.push_str("</CompleteMultipartUpload>");
        xml
    }
}
