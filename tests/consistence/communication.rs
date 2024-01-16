use std::{pin::Pin, sync::Arc};
use tokio::sync::Mutex;

use async_trait::async_trait;
use minipaxos::communication::{Requester, Server};
use std::future::Future;

pub struct LocalServer<'server, Q, R> {
    process_block: Option<Box<dyn Fn(Q) -> Pin<Box<dyn Future<Output = R> + Send>> + Send + 'server>>,
}

#[derive(Clone)]
pub struct LocalRequester<'a, Q, R> {
    server: Arc<Mutex<LocalServer<'a, Q, R>>>,
}

impl<'server, Q, R> LocalServer<'server, Q, R> {
    pub fn new() -> Self {
        Self {
            process_block: None,
        }
    }

    pub fn new_requester(server: &Arc<Mutex<Self>>) -> LocalRequester<'server, Q, R> {
        LocalRequester {
            server: server.clone(),
        }
    }
}

#[async_trait]
impl<'server, Q, R> Server<'server, Q> for LocalServer<'server, Q, R> {
    type Output = R;

    async fn run<F>(&mut self, f: F)
    where
        F: 'server + Fn(Q) -> Pin<Box<dyn Future<Output = Self::Output> + Send>> + Send,
    {
        self.process_block = Some(Box::new(f));
    }
}

#[async_trait]
impl<'server, Q: Send, R> Requester<Q, ()> for LocalRequester<'server, Q, R> {
    type Output = R;
    async fn request(&mut self, arg: Q) -> Result<Self::Output, ()> {
        if let Some(block) = self.server.lock().await.process_block.as_ref() {
            return Ok(block(arg).await)
        }

        Err(())
    }
}
