use futures::future::join_all;
use rand::Rng;
use std::{marker::PhantomData, ops::Range, sync::Arc};
use tokio::sync::Mutex;

use crate::{
    acceptor::{AcceptMessage, AcceptReply, PrepareMessage, PrepareReply},
    communication::Requester,
};

type BoxedAcceptRequester<P, E> = Box<dyn Requester<AcceptMessage<P>, E, Output = AcceptReply>>;
type BoxedPrepareRequester<P, E> = Box<dyn Requester<PrepareMessage, E, Output = PrepareReply<P>>>;

pub struct Proposer<P: Clone> {
    propose_id: u64,

    phantom_p: PhantomData<P>,
}

impl<P: Clone> Proposer<P> {
    pub fn new() -> Self {
        Self {
            propose_id: 0,
            phantom_p: PhantomData,
        }
    }

    pub async fn propose<E1, E2>(
        &mut self,
        proposal: P,
        preparers: Arc<Mutex<Vec<BoxedPrepareRequester<P, E1>>>>,
        acceptors: Arc<Mutex<Vec<BoxedAcceptRequester<P, E2>>>>,
        sleep_range: Range<u64>,
    ) -> u64 {
        let mut ballot = 1;
        loop {
            let instance = self.propose_id;
            let prepare_message = PrepareMessage { instance, ballot };
            let replies: Vec<_> = join_all(
                preparers
                    .lock()
                    .await
                    .iter_mut()
                    .map(|preparer| preparer.request(prepare_message.clone())),
            )
            .await
            .into_iter()
            .filter_map(|result| result.ok())
            .collect();

            let max_ballot = replies
                .iter()
                .filter_map(|prepare_reply| prepare_reply.ballot)
                .max();
            let positive_count = replies
                .iter()
                .filter(|prepare_reply| prepare_reply.state)
                .count();
            let preparer_count = preparers.lock().await.len();

            if positive_count < (preparer_count + 1) / 2 {
                if let Some(max_ballot) = max_ballot {
                    ballot = max_ballot + 1;
                } else {
                    ballot += 1;
                }

                let sleep_duration = {
                    let mut rng = rand::thread_rng();
                    let v = rng.gen_range(sleep_range.clone());
                    std::time::Duration::from_millis(v)
                };
                tokio::time::sleep(sleep_duration).await;

                continue;
            }

            let accepted_proposal: Option<P> = replies
                .iter()
                .fold(
                    (0, None),
                    |(max_ballot, max_proposal),
                     PrepareReply {
                         ballot,
                         proposal,
                         state: _,
                     }| {
                        if proposal.is_some()
                            && (max_proposal.is_none()
                                || ballot.map(|ballot| ballot > max_ballot).unwrap_or(true))
                        {
                            (ballot.unwrap_or(0), proposal.clone())
                        } else {
                            (max_ballot, max_proposal)
                        }
                    },
                )
                .1;

            let (proposed, current_proposal) = if let Some(p) = accepted_proposal {
                (false, p)
            } else {
                (true, proposal.clone())
            };

            let results = join_all(acceptors.lock().await.iter_mut().map(|acceptor| {
                let message = AcceptMessage {
                    instance,
                    ballot,
                    proposal: current_proposal.clone(),
                };

                acceptor.request(message)
            }))
            .await;
            let positive_count = results
                .iter()
                .filter_map(|result| result.as_ref().ok())
                .filter(|result| result.state)
                .count();
            let acceptor_count = acceptors.lock().await.len();

            if positive_count >= (acceptor_count + 1) / 2 {
                self.propose_id += 1;

                if proposed {
                    break self.propose_id - 1;
                }
            }
        }
    }
}

impl<P: Clone> Default for Proposer<P> {
    fn default() -> Self {
        Self::new()
    }
}
