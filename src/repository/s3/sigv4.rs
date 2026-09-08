use hmac::{Hmac, Mac};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use chrono::{DateTime, Utc};

type HmacSha256 = Hmac<Sha256>;

pub struct SigV4Signer<'a> {
    pub access_key: &'a str,
    pub secret_key: &'a str,
    pub region: &'a str,
    pub service: &'a str,
}

impl<'a> SigV4Signer<'a> {
    pub fn new(access_key: &'a str, secret_key: &'a str, region: &'a str) -> Self {
        Self {
            access_key,
            secret_key,
            region,
            service: "s3",
        }
    }

    /// Sign request and return required HTTP headers: `(x-amz-date, x-amz-content-sha256, authorization)`
    pub fn sign(
        &self,
        method: &str,
        canonical_uri: &str,
        query_params: &BTreeMap<String, String>,
        headers: &BTreeMap<String, String>,
        payload: &[u8],
        now: DateTime<Utc>,
    ) -> (String, String, String) {
        let amz_date = now.format("%Y%m%dT%H%M%SZ").to_string();
        let date_stamp = now.format("%Y%m%d").to_string();

        let payload_hash = hex::encode(Sha256::digest(payload));

        // Canonical query string
        let mut canonical_query = String::new();
        for (i, (k, v)) in query_params.iter().enumerate() {
            if i > 0 {
                canonical_query.push('&');
            }
            canonical_query.push_str(&urlencoding::encode(k));
            canonical_query.push('=');
            canonical_query.push_str(&urlencoding::encode(v));
        }

        // Canonical headers
        let mut normalized_headers = BTreeMap::new();
        for (k, v) in headers {
            normalized_headers.insert(k.to_lowercase(), v.trim().to_string());
        }
        normalized_headers.insert("x-amz-date".into(), amz_date.clone());
        normalized_headers.insert("x-amz-content-sha256".into(), payload_hash.clone());

        let mut canonical_headers = String::new();
        let mut signed_headers_vec = Vec::new();
        for (k, v) in &normalized_headers {
            canonical_headers.push_str(k);
            canonical_headers.push(':');
            canonical_headers.push_str(v);
            canonical_headers.push('\n');
            signed_headers_vec.push(k.as_str());
        }
        let signed_headers = signed_headers_vec.join(";");

        // Canonical request
        let mut safe_uri = canonical_uri.to_string();
        if !safe_uri.starts_with('/') {
            safe_uri = format!("/{}", safe_uri);
        }

        let canonical_request = format!(
            "{}\n{}\n{}\n{}\n{}\n{}",
            method,
            safe_uri,
            canonical_query,
            canonical_headers,
            signed_headers,
            payload_hash
        );

        let canonical_request_hash = hex::encode(Sha256::digest(canonical_request.as_bytes()));

        // String to sign
        let credential_scope = format!("{}/{}/{}/aws4_request", date_stamp, self.region, self.service);
        let string_to_sign = format!(
            "AWS4-HMAC-SHA256\n{}\n{}\n{}",
            amz_date,
            credential_scope,
            canonical_request_hash
        );

        // Derive signing key
        let signing_key = self.get_signature_key(&date_stamp);
        let mut mac = HmacSha256::new_from_slice(&signing_key).expect("HMAC can take any key size");
        mac.update(string_to_sign.as_bytes());
        let signature = hex::encode(mac.finalize().into_bytes());

        let authorization = format!(
            "AWS4-HMAC-SHA256 Credential={}/{}, SignedHeaders={}, Signature={}",
            self.access_key, credential_scope, signed_headers, signature
        );

        (amz_date, payload_hash, authorization)
    }

    fn get_signature_key(&self, date_stamp: &str) -> Vec<u8> {
        let k_secret = format!("AWS4{}", self.secret_key);
        let k_date = hmac_sha256(k_secret.as_bytes(), date_stamp.as_bytes());
        let k_region = hmac_sha256(&k_date, self.region.as_bytes());
        let k_service = hmac_sha256(&k_region, self.service.as_bytes());
        hmac_sha256(&k_service, b"aws4_request")
    }
}

fn hmac_sha256(key: &[u8], data: &[u8]) -> Vec<u8> {
    let mut mac = HmacSha256::new_from_slice(key).expect("HMAC can take key of any length");
    mac.update(data);
    mac.finalize().into_bytes().to_vec()
}

// Minimal urlencoding helper
mod urlencoding {
    pub fn encode(data: &str) -> String {
        let mut result = String::with_capacity(data.len());
        for byte in data.bytes() {
            match byte {
                b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                    result.push(byte as char);
                }
                _ => {
                    result.push_str(&format!("%{:02X}", byte));
                }
            }
        }
        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sigv4_signing() {
        let signer = SigV4Signer::new("AKIAIOSFODNN7EXAMPLE", "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY", "us-east-1");
        let now = DateTime::from_timestamp(1369353600, 0).unwrap(); // 2013-05-24T00:00:00Z
        let mut headers = BTreeMap::new();
        headers.insert("host".into(), "examplebucket.s3.amazonaws.com".into());

        let (amz_date, payload_hash, auth) = signer.sign(
            "GET",
            "/test.txt",
            &BTreeMap::new(),
            &headers,
            b"",
            now,
        );

        assert_eq!(amz_date, "20130524T000000Z");
        assert_eq!(
            payload_hash,
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
        );
        assert!(auth.starts_with("AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20130524/us-east-1/s3/aws4_request"));
    }
}
