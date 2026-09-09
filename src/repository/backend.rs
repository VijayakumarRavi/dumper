use crate::error::DumperError;
use std::future::Future;

pub trait StorageBackend: Send + Sync {
    fn put_object<'a>(
        &'a self,
        path: &'a str,
        data: &'a [u8],
    ) -> impl Future<Output = Result<(), DumperError>> + Send + 'a;
    fn get_object<'a>(
        &'a self,
        path: &'a str,
    ) -> impl Future<Output = Result<Vec<u8>, DumperError>> + Send + 'a;
    fn get_object_size<'a>(
        &'a self,
        path: &'a str,
    ) -> impl Future<Output = Result<u64, DumperError>> + Send + 'a;
    fn object_exists<'a>(
        &'a self,
        path: &'a str,
    ) -> impl Future<Output = Result<bool, DumperError>> + Send + 'a;
    fn delete_object<'a>(
        &'a self,
        path: &'a str,
    ) -> impl Future<Output = Result<(), DumperError>> + Send + 'a;
    fn list_objects<'a>(
        &'a self,
        prefix: &'a str,
    ) -> impl Future<Output = Result<Vec<String>, DumperError>> + Send + 'a;
    fn count_temp_files<'a>(
        &'a self,
    ) -> impl Future<Output = Result<usize, DumperError>> + Send + 'a {
        async { Ok(0) }
    }
    fn cleanup_temp_files<'a>(
        &'a self,
    ) -> impl Future<Output = Result<usize, DumperError>> + Send + 'a {
        async { Ok(0) }
    }
}
