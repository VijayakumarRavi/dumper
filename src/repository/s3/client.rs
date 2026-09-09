use crate::error::DumperError;
use crate::repository::backend::StorageBackend;
use crate::repository::s3::sigv4::SigV4Signer;
use chrono::Utc;
use rand::Rng;
use reqwest::{Client, Method, Response, StatusCode};
use std::collections::BTreeMap;
use std::time::Duration;

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

    pub(crate) fn build_canonical_uri(&self, key: &str) -> String {
        let clean_key = key.trim_start_matches('/');
        if clean_key.is_empty() {
            format!("/{}", self.bucket)
        } else {
            format!("/{}/{}", self.bucket, clean_key)
        }
    }

    pub(crate) fn build_request_url(&self, key: &str) -> String {
        let endpoint = self.endpoint.trim_end_matches('/');
        let clean_key = key.trim_start_matches('/');
        if clean_key.is_empty() {
            format!("{}/{}", endpoint, self.bucket)
        } else {
            format!("{}/{}/{}", endpoint, self.bucket, clean_key)
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
            let canonical_uri = self.build_canonical_uri(key);

            let mut headers = BTreeMap::new();
            let parsed_endpoint = url::Url::parse(&self.endpoint)
                .map_err(|e| DumperError::S3(format!("Invalid endpoint URL: {}", e)))?;
            let host = parsed_endpoint.host_str().unwrap_or("s3.amazonaws.com");
            let host_header = match parsed_endpoint.port() {
                Some(port) => format!("{}:{}", host, port),
                None => host.to_string(),
            };
            headers.insert("host".into(), host_header.clone());

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

            let request_url = self.build_request_url(key);

            let mut req = self
                .client
                .request(method.clone(), &request_url)
                .header("host", host_header)
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
            return Err(DumperError::Repository(format!(
                "Object '{}' not found",
                path
            )));
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

    async fn get_object_size(&self, path: &str) -> Result<u64, DumperError> {
        let key = self.full_key(path);
        let resp = self
            .send_request_with_retry(Method::HEAD, &key, BTreeMap::new(), Vec::new())
            .await?;
        if resp.status() == StatusCode::NOT_FOUND {
            return Err(DumperError::Repository(format!(
                "Object '{}' not found",
                path
            )));
        }
        if !resp.status().is_success() {
            let status = resp.status();
            return Err(DumperError::S3(format!(
                "Failed to HEAD S3 object '{}' (HTTP {})",
                key, status
            )));
        }
        let len = resp
            .headers()
            .get(reqwest::header::CONTENT_LENGTH)
            .and_then(|v| v.to_str().ok())
            .and_then(|s| s.parse::<u64>().ok())
            .unwrap_or(0);
        Ok(len)
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

            let body = resp
                .text()
                .await
                .map_err(|e| DumperError::S3(e.to_string()))?;

            // Lightweight XML parsing of <Key> and <NextContinuationToken>
            for key_match in extract_xml_tags(&body, "Key") {
                // Strip the repository prefix
                let clean_key = if !self.prefix.is_empty() && key_match.starts_with(&self.prefix) {
                    key_match[self.prefix.len()..]
                        .trim_start_matches('/')
                        .to_string()
                } else {
                    key_match
                };
                results.push(clean_key);
            }

            let is_truncated = extract_xml_tags(&body, "IsTruncated")
                .first()
                .map(|s| s.trim().eq_ignore_ascii_case("true"));

            if is_truncated == Some(false) {
                break;
            }

            let tokens = extract_xml_tags(&body, "NextContinuationToken");
            if let Some(next) = tokens.into_iter().next().filter(|t| !t.trim().is_empty()) {
                continuation_token = Some(next);
            } else {
                break;
            }
        }

        results.sort();
        Ok(results)
    }
}

fn unescape_xml(s: &str) -> String {
    s.replace("&quot;", "\"")
        .replace("&apos;", "'")
        .replace("&lt;", "<")
        .replace("&gt;", ">")
        .replace("&amp;", "&")
}

fn extract_xml_tags(xml: &str, tag: &str) -> Vec<String> {
    let mut results = Vec::new();
    let mut cursor = xml;

    let search_prefix = format!("<{}", tag);
    let close_tag_start = format!("</{}", tag);

    while let Some(start_pos) = cursor.find(&search_prefix) {
        let after_start = &cursor[start_pos + search_prefix.len()..];
        let next_char = after_start.chars().next();
        match next_char {
            Some('>') => {
                // Exact tag: <tag>content</tag>
                let content_start = &after_start[1..];
                if let Some(close_pos) = content_start.find(&close_tag_start) {
                    let content = &content_start[..close_pos];
                    results.push(unescape_xml(content));
                    let after_close = &content_start[close_pos + close_tag_start.len()..];
                    if let Some(end_angle) = after_close.find('>') {
                        cursor = &after_close[end_angle + 1..];
                    } else {
                        break;
                    }
                } else {
                    break;
                }
            }
            Some('/') => {
                // Self-closing: <tag/>
                if let Some(stripped) = after_start.strip_prefix("/>") {
                    results.push(String::new());
                    cursor = stripped;
                } else {
                    cursor = after_start;
                }
            }
            Some(c) if c.is_ascii_whitespace() => {
                // Tag with attributes or namespaces: <tag attr="val">content</tag> or <tag attr="val" />
                if let Some(open_end) = after_start.find('>') {
                    let open_tag_content = &after_start[..open_end];
                    if open_tag_content.trim_end().ends_with('/') {
                        // Self-closing: <tag attr="val" />
                        results.push(String::new());
                        cursor = &after_start[open_end + 1..];
                    } else {
                        let content_start = &after_start[open_end + 1..];
                        if let Some(close_pos) = content_start.find(&close_tag_start) {
                            let content = &content_start[..close_pos];
                            results.push(unescape_xml(content));
                            let after_close = &content_start[close_pos + close_tag_start.len()..];
                            if let Some(end_angle) = after_close.find('>') {
                                cursor = &after_close[end_angle + 1..];
                            } else {
                                break;
                            }
                        } else {
                            break;
                        }
                    }
                } else {
                    break;
                }
            }
            _ => {
                // E.g. <Keyword> when searching for <Key> - skip past prefix
                cursor = after_start;
            }
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

    #[test]
    fn test_extract_xml_with_attributes_namespaces_and_entities() {
        let xml = r#"<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
            <Name>test-bucket</Name>
            <IsTruncated>false</IsTruncated>
            <Contents>
                <Key xmlns="http://s3.amazonaws.com/doc/2006-03-01/">blobs/12/&lt;data&gt;&amp;test&apos;file&quot;.dmp</Key>
                <Size>12345</Size>
            </Contents>
            <Contents>
                <Key attr="sample">blobs/34/regular.dmp</Key>
            </Contents>
            <Keyword>ignore_me</Keyword>
        </ListBucketResult>"#;

        let keys = extract_xml_tags(xml, "Key");
        assert_eq!(
            keys,
            vec!["blobs/12/<data>&test'file\".dmp", "blobs/34/regular.dmp"]
        );

        let truncated = extract_xml_tags(xml, "IsTruncated");
        assert_eq!(truncated, vec!["false"]);

        // Should NOT falsely match Keyword when searching for Key
        assert_eq!(extract_xml_tags(xml, "Keyword"), vec!["ignore_me"]);
    }

    #[test]
    fn test_canonical_uri_and_request_url_alignment() {
        let client = S3Client::new(
            Some("https://s3.amazonaws.com".into()),
            "my-backups".into(),
            "prod".into(),
            "us-east-1".into(),
            "test_access".into(),
            "test_secret".into(),
            None,
        )
        .unwrap();

        // 1. Empty key (used by list_objects)
        let empty_canon = client.build_canonical_uri("");
        let empty_url = client.build_request_url("");
        assert_eq!(empty_canon, "/my-backups");
        assert_eq!(empty_url, "https://s3.amazonaws.com/my-backups");
        let parsed_empty = url::Url::parse(&empty_url).unwrap();
        assert_eq!(parsed_empty.path(), empty_canon);

        // 2. Slash key
        let slash_canon = client.build_canonical_uri("/");
        let slash_url = client.build_request_url("/");
        assert_eq!(slash_canon, "/my-backups");
        assert_eq!(slash_url, "https://s3.amazonaws.com/my-backups");
        let parsed_slash = url::Url::parse(&slash_url).unwrap();
        assert_eq!(parsed_slash.path(), slash_canon);

        // 3. Object key
        let obj_canon = client.build_canonical_uri("blobs/ab/1234");
        let obj_url = client.build_request_url("blobs/ab/1234");
        assert_eq!(obj_canon, "/my-backups/blobs/ab/1234");
        assert_eq!(obj_url, "https://s3.amazonaws.com/my-backups/blobs/ab/1234");
        let parsed_obj = url::Url::parse(&obj_url).unwrap();
        assert_eq!(parsed_obj.path(), obj_canon);

        // 4. Object key with leading slash
        let leading_slash_canon = client.build_canonical_uri("/blobs/ab/1234");
        let leading_slash_url = client.build_request_url("/blobs/ab/1234");
        assert_eq!(leading_slash_canon, "/my-backups/blobs/ab/1234");
        assert_eq!(
            leading_slash_url,
            "https://s3.amazonaws.com/my-backups/blobs/ab/1234"
        );
        let parsed_leading = url::Url::parse(&leading_slash_url).unwrap();
        assert_eq!(parsed_leading.path(), leading_slash_canon);

        // 5. Endpoint with trailing slash
        let client_with_slash = S3Client::new(
            Some("http://127.0.0.1:9000/".into()),
            "my-backups".into(),
            "".into(),
            "us-east-1".into(),
            "test_access".into(),
            "test_secret".into(),
            None,
        )
        .unwrap();
        let url_trailing = client_with_slash.build_request_url("");
        assert_eq!(url_trailing, "http://127.0.0.1:9000/my-backups");
        let parsed_trailing = url::Url::parse(&url_trailing).unwrap();
        assert_eq!(parsed_trailing.path(), "/my-backups");
    }
}
