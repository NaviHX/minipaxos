use std::future::Future;

use async_trait::async_trait;

#[async_trait]
pub trait Requester<T, E> {
    type Output;
    async fn request(&mut self, arg: T) -> Result<Self::Output, E>;
}

#[async_trait]
pub trait Server<T> {
    type Output;

    async fn run<F>(&mut self, f: F) -> Self::Output
    where
        F: Fn(T) -> Box<dyn Future<Output = Self::Output>>;
}
