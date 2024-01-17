use futures::future::join_all;
use rand::Rng;
use std::{marker::PhantomData, ops::Range};

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
        mut preparers: impl AsMut<Vec<BoxedPrepareRequester<P, E1>>>,
        mut acceptors: impl AsMut<Vec<BoxedAcceptRequester<P, E2>>>,
        sleep_range: Range<u64>,
    ) -> u64 {
        let mut ballot = 1;
        loop {
            let instance = self.propose_id;
            let prepare_message = PrepareMessage { instance, ballot };
            let replies: Vec<_> = join_all(
                preparers
                    .as_mut()
                    .iter_mut()
                    .map(|preparer| preparer.request(prepare_message.clone())),
            )
            .await
            .into_iter()
            .filter_map(|result| result.ok())
            .collect();

            let max_ballot = Self::get_max_ballot(replies.as_slice());
            let positive_count = Self::get_positive_count(replies.as_slice());
            let preparer_count = preparers.as_mut().len();

            if positive_count < (preparer_count + 1) / 2 {
                if let Some(max_ballot) = max_ballot {
                    ballot = max_ballot.max(ballot) + 1;
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

            let accepted_proposal = Self::get_max_accepted_proposal(replies.as_slice());
            let (proposed, current_proposal) = if let Some(p) = accepted_proposal {
                (false, p)
            } else {
                (true, proposal.clone())
            };

            let results = join_all(acceptors.as_mut().iter_mut().map(|acceptor| {
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
            let acceptor_count = acceptors.as_mut().len();

            if positive_count >= (acceptor_count + 1) / 2 {
                self.propose_id += 1;

                if proposed {
                    break self.propose_id - 1;
                }
            }
        }
    }

    pub fn get_max_ballot(replies: &[PrepareReply<P>]) -> Option<u64> {
        replies
            .iter()
            .filter_map(|prepare_reply| prepare_reply.ballot)
            .max()
    }

    pub fn get_positive_count(replies: &[PrepareReply<P>]) -> usize {
        replies.iter().filter(|p| p.state).count()
    }

    pub fn get_max_accepted_proposal(replies: &[PrepareReply<P>]) -> Option<P> {
        replies
            .iter()
            .fold(
                (0, None),
                |(max_ballot, max_proposal),
                 PrepareReply {
                     state: _,
                     ballot,
                     proposal,
                 }| {
                    if proposal.is_some()
                        && (max_proposal.is_none()
                            || ballot.map(|ballot| ballot > max_ballot).unwrap_or(false))
                    {
                        (ballot.unwrap_or(0), proposal.clone())
                    } else {
                        (max_ballot, max_proposal)
                    }
                },
            )
            .1
    }
}

impl<P: Clone> Default for Proposer<P> {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod test {
    use crate::{acceptor::PrepareReply, proposer::Proposer};

    #[test]
    fn max_ballot_test() {
        let replies: &[PrepareReply<()>] = &[];
        assert_eq!(Proposer::get_max_ballot(replies), None);

        let replies: &[PrepareReply<()>] = &[PrepareReply {
            state: true,
            ballot: None,
            proposal: None,
        }];
        assert_eq!(Proposer::get_max_ballot(replies), None);

        let replies: &[PrepareReply<()>] = &[PrepareReply {
            state: true,
            ballot: Some(1),
            proposal: None,
        }];
        assert_eq!(Proposer::get_max_ballot(replies), Some(1));

        let replies: &[PrepareReply<()>] = &[
            PrepareReply {
                state: true,
                ballot: Some(1),
                proposal: None,
            },
            PrepareReply {
                state: true,
                ballot: Some(2),
                proposal: None,
            },
        ];
        assert_eq!(Proposer::get_max_ballot(replies), Some(2));
    }

    #[test]
    fn max_accepted_proposal_test() {
        let replies: &[PrepareReply<usize>] = &[];
        assert_eq!(Proposer::get_max_ballot(replies), None);

        let replies: &[PrepareReply<()>] = &[PrepareReply {
            state: true,
            ballot: None,
            proposal: None,
        }];
        assert_eq!(Proposer::get_max_accepted_proposal(replies), None);

        let replies: &[PrepareReply<usize>] = &[PrepareReply {
            state: true,
            ballot: Some(1),
            proposal: Some(1),
        }];
        assert_eq!(Proposer::get_max_accepted_proposal(replies), Some(1));

        let replies: &[PrepareReply<usize>] = &[
            PrepareReply {
                state: true,
                ballot: Some(1),
                proposal: Some(1),
            },
            PrepareReply {
                state: true,
                ballot: Some(2),
                proposal: Some(2),
            },
        ];
        assert_eq!(Proposer::get_max_accepted_proposal(replies), Some(2));

        let replies: &[PrepareReply<usize>] = &[
            PrepareReply {
                state: true,
                ballot: Some(1),
                proposal: Some(1),
            },
            PrepareReply {
                state: true,
                ballot: Some(2),
                proposal: None,
            },
        ];
        assert_eq!(Proposer::get_max_accepted_proposal(replies), Some(1));
    }
}
