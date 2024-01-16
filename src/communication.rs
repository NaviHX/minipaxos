use std::{future::Future, pin::Pin};

use async_trait::async_trait;

#[async_trait]
pub trait Requester<T, E> {
    type Output;
    async fn request(&mut self, arg: T) -> Result<Self::Output, E>;
}

#[async_trait]
pub trait Server<T, 'server> {
    type Output;

    async fn run<F>(&mut self, f: F)
    where
        F: 'server + Fn(T) -> Pin<Box<dyn Future<Output = Self::Output> + Send>> + Send;
}
