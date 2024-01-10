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
            .unwrap_or(false)
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

use std::{
    collections::{HashMap, VecDeque},
    sync::Arc,
};

pub type AcceptorID = Uuid;

pub struct Acceptor<T: Clone> {
    id: AcceptorID,
    inner_acceptors: HashMap<u64, InnerAcceptor<T>>,
    learn_requests: VecDeque<LearnMessage<T>>,
}

use futures::future::join_all;
use uuid::Uuid;
use std::sync::Mutex as StdMutex;
use tokio::sync::Mutex;

use crate::{
    communication::{Requester, Server},
    learner::{LearnMessage, LearnReply},
};

type BoxedLearnRequester<T, E> = Box<dyn Requester<LearnMessage<T>, E, Output = LearnReply>>;

impl<T: Clone + 'static> Acceptor<T> {
    pub fn new(id: AcceptorID) -> Self {
        Self {
            id,
            inner_acceptors: HashMap::new(),
            learn_requests: VecDeque::new(),
        }
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

    pub async fn serve_preparer(
        acceptor: Arc<StdMutex<Self>>,
        mut server: impl Server<PrepareMessage, Output = PrepareReply<T>>,
    ) {
        server
            .run(move |message| {
                let acceptor = acceptor.clone();
                Box::new(async move { acceptor.lock().unwrap().prepare(message) })
            })
            .await;
    }

    pub async fn serve_acceptor(
        acceptor: Arc<StdMutex<Self>>,
        mut server: impl Server<AcceptMessage<T>, Output = AcceptReply>,
    ) {
        server
            .run(move |message| {
                let acceptor = acceptor.clone();
                Box::new(async move {
                    let reply = acceptor.lock().unwrap().accept(message.clone());

                    if reply.state {
                        let new_learn_request = LearnMessage {
                            instance: message.instance,
                            acceptor_id: acceptor.lock().unwrap().id,
                            proposal: message.proposal,
                        };

                        acceptor
                            .lock()
                            .unwrap()
                            .learn_requests
                            .push_back(new_learn_request);
                    }

                    reply
                })
            })
            .await;
    }
}
