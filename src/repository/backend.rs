use std::future::Future;
use crate::error::DumperError;

pub trait StorageBackend: Send + Sync {
    fn put_object<'a>(&'a self, path: &'a str, data: &'a [u8]) -> impl Future<Output = Result<(), DumperError>> + Send + 'a;
    fn get_object<'a>(&'a self, path: &'a str) -> impl Future<Output = Result<Vec<u8>, DumperError>> + Send + 'a;
    fn object_exists<'a>(&'a self, path: &'a str) -> impl Future<Output = Result<bool, DumperError>> + Send + 'a;
    fn delete_object<'a>(&'a self, path: &'a str) -> impl Future<Output = Result<(), DumperError>> + Send + 'a;
    fn list_objects<'a>(&'a self, prefix: &'a str) -> impl Future<Output = Result<Vec<String>, DumperError>> + Send + 'a;
}
