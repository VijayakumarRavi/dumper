use std::collections::BTreeMap;
use std::time::Duration;
use chrono::Utc;
use reqwest::{Client, Method, Response, StatusCode};
use rand::Rng;
use crate::error::DumperError;
use crate::repository::backend::StorageBackend;
use crate::repository::s3::sigv4::SigV4Signer;

pub struct S3Client {
    client: Client,
    endpoint: String,
    bucket: String,
    prefix: String,
    region: String,
    access_key: String,
    secret_key: String,
    session_token: Option<String>,
}

impl S3Client {
    pub fn new(
        endpoint: Option<String>,
        bucket: String,
        prefix: String,
        region: String,
        access_key: String,
        secret_key: String,
        session_token: Option<String>,
    ) -> Result<Self, DumperError> {
        let default_endpoint = format!("https://s3.{}.amazonaws.com", region);
        let endpoint_url = endpoint.unwrap_or(default_endpoint);

        let client = Client::builder()
            .timeout(Duration::from_secs(60))
            .connect_timeout(Duration::from_secs(10))
            .build()
            .map_err(|e| DumperError::S3(format!("Failed to build HTTP client: {}", e)))?;

        Ok(Self {
            client,
            endpoint: endpoint_url.trim_end_matches('/').to_string(),
            bucket,
            prefix: prefix.trim_matches('/').to_string(),
            region,
            access_key,
            secret_key,
            session_token,
        })
    }

    /// Parse repository string `s3://bucket/prefix`
    pub fn parse_s3_url(url_str: &str) -> Result<(String, String), DumperError> {
        if !url_str.starts_with("s3://") {
            return Err(DumperError::Config(format!(
                "Invalid S3 repository URL '{}', must start with 's3://'",
                url_str
            )));
        }

        let without_scheme = &url_str[5..];
        let mut parts = without_scheme.splitn(2, '/');
        let bucket = parts.next().unwrap_or("").to_string();
        let prefix = parts.next().unwrap_or("").to_string();

        if bucket.is_empty() {
            return Err(DumperError::Config(
                "S3 repository URL must specify a bucket name (e.g. s3://my-bucket/backups)".into(),
            ));
        }

        Ok((bucket, prefix))
    }

    fn full_key(&self, rel_path: &str) -> String {
        let clean_path = rel_path.trim_start_matches('/');
        if self.prefix.is_empty() {
            clean_path.to_string()
        } else {
            format!("{}/{}", self.prefix, clean_path)
        }
    }

    async fn send_request_with_retry(
        &self,
        method: Method,
        key: &str,
        query_params: BTreeMap<String, String>,
        payload: Vec<u8>,
    ) -> Result<Response, DumperError> {
        let max_retries = 4;
        let mut delay = Duration::from_millis(200);

        for attempt in 0..=max_retries {
            let now = Utc::now();
            let canonical_uri = format!("/{}/{}", self.bucket, key);

            let mut headers = BTreeMap::new();
            let parsed_endpoint = url::Url::parse(&self.endpoint)
                .map_err(|e| DumperError::S3(format!("Invalid endpoint URL: {}", e)))?;
            let host = parsed_endpoint.host_str().unwrap_or("s3.amazonaws.com");
            headers.insert("host".into(), host.to_string());

            if let Some(ref token) = self.session_token {
                headers.insert("x-amz-security-token".into(), token.clone());
            }

            let signer = SigV4Signer::new(&self.access_key, &self.secret_key, &self.region);
            let (amz_date, payload_hash, auth) = signer.sign(
                method.as_str(),
                &canonical_uri,
                &query_params,
                &headers,
                &payload,
                now,
            );

            let mut request_url = format!("{}/{}", self.endpoint, self.bucket);
            if !key.is_empty() {
                request_url.push('/');
                request_url.push_str(key);
            }

            let mut req = self
                .client
                .request(method.clone(), &request_url)
                .header("x-amz-date", amz_date)
                .header("x-amz-content-sha256", payload_hash)
                .header("Authorization", auth);

            if let Some(ref token) = self.session_token {
                req = req.header("x-amz-security-token", token);
            }

            for (k, v) in &query_params {
                req = req.query(&[(k, v)]);
            }

            if !payload.is_empty() || method == Method::PUT || method == Method::POST {
                req = req.body(payload.clone());
            }

            match req.send().await {
                Ok(resp) => {
                    let status = resp.status();
                    if status.is_success() || status == StatusCode::NOT_FOUND {
                        return Ok(resp);
                    }

                    if status == StatusCode::UNAUTHORIZED || status == StatusCode::FORBIDDEN {
                        let err_body = resp.text().await.unwrap_or_default();
                        return Err(DumperError::S3(format!(
                            "S3 Authentication/Authorization failed: HTTP {} error on {} {}: {}",
                            status, method, key, err_body
                        )));
                    }

                    if (status.is_server_error() || status == StatusCode::TOO_MANY_REQUESTS)
                        && attempt < max_retries
                    {
                        let jitter = rand::thread_rng().gen_range(0..100);
                        tokio::time::sleep(delay + Duration::from_millis(jitter)).await;
                        delay *= 2;
                        continue;
                    }

                    let err_body = resp.text().await.unwrap_or_default();
                    return Err(DumperError::S3(format!(
                        "S3 HTTP {} error on {} {}: {}",
                        status, method, key, err_body
                    )));
                }
                Err(e) => {
                    if attempt < max_retries {
                        let jitter = rand::thread_rng().gen_range(0..100);
                        tokio::time::sleep(delay + Duration::from_millis(jitter)).await;
                        delay *= 2;
                        continue;
                    }
                    return Err(DumperError::S3(format!("S3 connection failed: {}", e)));
                }
            }
        }

        Err(DumperError::S3("Exceeded maximum S3 retries".into()))
    }
}

impl StorageBackend for S3Client {
    async fn put_object(&self, path: &str, data: &[u8]) -> Result<(), DumperError> {
        let key = self.full_key(path);
        let resp = self
            .send_request_with_retry(Method::PUT, &key, BTreeMap::new(), data.to_vec())
            .await?;
        if !resp.status().is_success() {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            return Err(DumperError::S3(format!(
                "Failed to PUT S3 object '{}' (HTTP {}): {}",
                key, status, body
            )));
        }
        Ok(())
    }

    async fn get_object(&self, path: &str) -> Result<Vec<u8>, DumperError> {
        let key = self.full_key(path);
        let resp = self
            .send_request_with_retry(Method::GET, &key, BTreeMap::new(), Vec::new())
            .await?;
        if resp.status() == StatusCode::NOT_FOUND {
            return Err(DumperError::Repository(format!("Object '{}' not found", path)));
        }
        if !resp.status().is_success() {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            return Err(DumperError::S3(format!(
                "Failed to GET S3 object '{}' (HTTP {}): {}",
                key, status, body
            )));
        }
        let bytes = resp
            .bytes()
            .await
            .map_err(|e| DumperError::S3(format!("Failed to read S3 response body: {}", e)))?;
        Ok(bytes.to_vec())
    }

    async fn object_exists(&self, path: &str) -> Result<bool, DumperError> {
        let key = self.full_key(path);
        let resp = self
            .send_request_with_retry(Method::HEAD, &key, BTreeMap::new(), Vec::new())
            .await?;
        Ok(resp.status().is_success())
    }

    async fn delete_object(&self, path: &str) -> Result<(), DumperError> {
        let key = self.full_key(path);
        let resp = self
            .send_request_with_retry(Method::DELETE, &key, BTreeMap::new(), Vec::new())
            .await?;
        if !resp.status().is_success() && resp.status() != StatusCode::NOT_FOUND {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            return Err(DumperError::S3(format!(
                "Failed to DELETE S3 object '{}' (HTTP {}): {}",
                key, status, body
            )));
        }
        Ok(())
    }

    async fn list_objects(&self, prefix: &str) -> Result<Vec<String>, DumperError> {
        let search_prefix = self.full_key(prefix);
        let mut results = Vec::new();
        let mut continuation_token: Option<String> = None;

        loop {
            let mut query = BTreeMap::new();
            query.insert("list-type".into(), "2".into());
            query.insert("prefix".into(), search_prefix.clone());
            if let Some(ref token) = continuation_token {
                query.insert("continuation-token".into(), token.clone());
            }

            let resp = self
                .send_request_with_retry(Method::GET, "", query, Vec::new())
                .await?;
            if !resp.status().is_success() {
                let status = resp.status();
                let body = resp.text().await.unwrap_or_default();
                return Err(DumperError::S3(format!(
                    "Failed to list S3 objects with prefix '{}' (HTTP {}): {}",
                    search_prefix, status, body
                )));
            }

            let body = resp.text().await.map_err(|e| DumperError::S3(e.to_string()))?;

            // Lightweight XML parsing of <Key> and <NextContinuationToken>
            for key_match in extract_xml_tags(&body, "Key") {
                // Strip the repository prefix
                let clean_key = if !self.prefix.is_empty() && key_match.starts_with(&self.prefix) {
                    key_match[self.prefix.len()..].trim_start_matches('/').to_string()
                } else {
                    key_match
                };
                results.push(clean_key);
            }

            let tokens = extract_xml_tags(&body, "NextContinuationToken");
            if let Some(next) = tokens.into_iter().next() {
                continuation_token = Some(next);
            } else {
                break;
            }
        }

        results.sort();
        Ok(results)
    }
}

fn extract_xml_tags(xml: &str, tag: &str) -> Vec<String> {
    let open_tag = format!("<{}>", tag);
    let close_tag = format!("</{}>", tag);
    let mut results = Vec::new();

    let mut cursor = xml;
    while let Some(start_pos) = cursor.find(&open_tag) {
        let content_start = start_pos + open_tag.len();
        let rest = &cursor[content_start..];
        if let Some(end_pos) = rest.find(&close_tag) {
            let val = &rest[..end_pos];
            results.push(val.to_string());
            cursor = &rest[end_pos + close_tag.len()..];
        } else {
            break;
        }
    }

    results
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_s3_url() {
        let (bucket, prefix) = S3Client::parse_s3_url("s3://my-backups/postgres").unwrap();
        assert_eq!(bucket, "my-backups");
        assert_eq!(prefix, "postgres");

        let (bucket2, prefix2) = S3Client::parse_s3_url("s3://standalone-bucket").unwrap();
        assert_eq!(bucket2, "standalone-bucket");
        assert_eq!(prefix2, "");
    }

    #[test]
    fn test_extract_xml_tags() {
        let xml = r#"<ListBucketResult><Key>blobs/ab/1234</Key><Key>blobs/cd/5678</Key><NextContinuationToken>token_xyz</NextContinuationToken></ListBucketResult>"#;
        let keys = extract_xml_tags(xml, "Key");
        assert_eq!(keys, vec!["blobs/ab/1234", "blobs/cd/5678"]);
        let tokens = extract_xml_tags(xml, "NextContinuationToken");
        assert_eq!(tokens, vec!["token_xyz"]);
    }
}
