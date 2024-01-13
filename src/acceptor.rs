#[derive(Clone)]
pub struct PrepareMessage {
    pub instance: u64,
    pub ballot: u64,
}
pub struct PrepareReply<T: Clone> {
    pub state: bool,
    pub ballot: Option<u64>,
    pub proposal: Option<T>,
}

#[derive(Clone)]
pub struct AcceptMessage<T: Clone> {
    pub instance: u64,
    pub ballot: u64,
    pub proposal: T,
}
pub struct AcceptReply {
    pub state: bool,
}

struct InnerAcceptor<T: Clone> {
    pub accepted_proposal: Option<T>,
    pub ballot: Option<u64>,
}

impl<T: Clone> InnerAcceptor<T> {
    pub fn new() -> Self {
        Self {
            accepted_proposal: None,
            ballot: None,
        }
    }

    pub fn prepare(&mut self, ballot: u64) -> PrepareReply<T> {
        let state = match self
            .ballot
            .as_ref()
            .map(|&my_ballot| my_ballot < ballot)
            .unwrap_or(true)
        {
            true => {
                self.ballot = Some(ballot);
                true
            }
            false => false,
        };

        PrepareReply {
            state,
            ballot: self.ballot,
            proposal: self.accepted_proposal.clone(),
        }
    }

    pub fn accept(&mut self, ballot: u64, proposal: T) -> AcceptReply {
        let state = if self
            .ballot
            .as_ref()
            .map(|&my_ballot| my_ballot == ballot)
            .unwrap_or(false)
        {
            self.accepted_proposal = Some(proposal);
            true
        } else {
            false
        };

        AcceptReply { state }
    }
}

use std::{collections::HashMap, sync::Arc};

use tokio::sync::mpsc::{self, Receiver};

pub type AcceptorID = Uuid;

pub struct Acceptor<T: Clone> {
    id: AcceptorID,
    inner_acceptors: HashMap<u64, InnerAcceptor<T>>,
    learn_request_tx: mpsc::Sender<LearnMessage<T>>,
}

use futures::future::join_all;
use tokio::sync::Mutex;
use uuid::Uuid;

use crate::{
    communication::{Requester, Server},
    learner::{LearnMessage, LearnReply},
};

type BoxedLearnRequester<T, E> = Box<dyn Requester<LearnMessage<T>, E, Output = LearnReply>>;

impl<T: Clone + 'static> Acceptor<T> {
    pub fn new(id: AcceptorID, buffer_size: usize) -> (Self, Receiver<LearnMessage<T>>) {
        let (tx, rx) = mpsc::channel(buffer_size);
        (
            Self {
                id,
                inner_acceptors: HashMap::new(),
                learn_request_tx: tx,
            },
            rx,
        )
    }

    pub fn prepare(
        &mut self,
        PrepareMessage { instance, ballot }: PrepareMessage,
    ) -> PrepareReply<T> {
        let acceptor = self
            .inner_acceptors
            .entry(instance)
            .or_insert_with(InnerAcceptor::new);
        acceptor.prepare(ballot)
    }

    pub fn accept(
        &mut self,
        AcceptMessage {
            instance,
            ballot,
            proposal,
        }: AcceptMessage<T>,
    ) -> AcceptReply {
        if let Some(acceptor) = self.inner_acceptors.get_mut(&instance) {
            let reply = acceptor.accept(ballot, proposal);
            return reply;
        }

        AcceptReply { state: false }
    }

    pub async fn send_learn<E>(
        learners: Arc<Mutex<Vec<BoxedLearnRequester<T, E>>>>,
        message: LearnMessage<T>,
    ) {
        let _ = join_all(learners.lock().await.iter_mut().map(|requester| {
            let message = message.clone();
            requester.request(message)
        }))
        .await;
    }

    pub async fn process_learn_request<E>(
        learners: Arc<Mutex<Vec<BoxedLearnRequester<T, E>>>>,
        mut receiver: Receiver<LearnMessage<T>>,
    ) {
        while let Some(msg) = receiver.recv().await {
            Self::send_learn(learners.clone(), msg).await;
        }
    }

    pub async fn serve_preparer(
        acceptor: Arc<Mutex<Self>>,
        mut server: impl Server<PrepareMessage, Output = PrepareReply<T>>,
    ) {
        server
            .run(move |message| {
                let acceptor = acceptor.clone();
                Box::new(async move { acceptor.lock().await.prepare(message) })
            })
            .await;
    }

    pub async fn serve_acceptor(
        acceptor: Arc<Mutex<Self>>,
        mut server: impl Server<AcceptMessage<T>, Output = AcceptReply>,
    ) {
        server
            .run(move |message| {
                let acceptor = acceptor.clone();
                Box::new(async move {
                    let reply = acceptor.lock().await.accept(message.clone());

                    if reply.state {
                        let new_learn_request = LearnMessage {
                            instance: message.instance,
                            acceptor_id: acceptor.lock().await.id,
                            proposal: message.proposal,
                        };

                        let _ = acceptor
                            .lock()
                            .await
                            .learn_request_tx
                            .send(new_learn_request)
                            .await;
                    }

                    reply
                })
            })
            .await;
    }
}

#[cfg(test)]
mod test {
    use super::InnerAcceptor;

    #[test]
    fn acceptor_test() {
        let mut acceptor = InnerAcceptor::new();
        let ballot = 1;
        let prepare_reply = acceptor.prepare(ballot);

        assert!(prepare_reply.state);
        assert!(prepare_reply.ballot.map(|b| b == 1).unwrap_or(false));
        assert!(prepare_reply.proposal.is_none());

        let accept_reply = acceptor.accept(ballot, true);
        assert!(accept_reply.state);
    }

    #[test]
    fn acceptor_with_proposal_test() {
        let mut acceptor = InnerAcceptor::new();
        acceptor.ballot = Some(2);
        acceptor.accepted_proposal = Some(2);

        let prepare_reply = acceptor.prepare(1);
        assert!(!prepare_reply.state);
        assert!(prepare_reply.ballot.map(|b| b == 2).unwrap_or(false));
        assert!(prepare_reply.proposal.map(|p| p == 2).unwrap_or(false));

        let prepare_reply = acceptor.prepare(3);
        assert!(prepare_reply.state);
        assert!(prepare_reply.ballot.map(|b| b == 3).unwrap_or(false));
        assert!(prepare_reply.proposal.map(|p| p == 2).unwrap_or(false));

        let accept_reply = acceptor.accept(3, 3);
        assert!(accept_reply.state);
        assert!(acceptor.ballot.map(|b| b == 3).unwrap_or(false));
        assert!(acceptor.accepted_proposal.map(|p| p == 3).unwrap_or(false));
    }
}
