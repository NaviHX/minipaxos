use futures::future::join_all;
use std::marker::PhantomData;

use crate::communication::Requester;
use crate::learner::ReadReply;

pub struct Reader<Q, R> {
    phantom_q: PhantomData<Q>,
    phantom_r: PhantomData<R>,
}

type BoxedReadRequester<Q, R, E> = Box<dyn Requester<Q, E, Output = ReadReply<R>>>;

impl<Q: Clone, R: Clone> Reader<Q, R> {
    pub fn new() -> Self {
        Self {
            phantom_q: PhantomData,
            phantom_r: PhantomData,
        }
    }

    pub async fn read<E>(
        &mut self,
        query: Q,
        mut learners: impl AsMut<Vec<BoxedReadRequester<Q, R, E>>>,
    ) -> Option<R> {
        join_all(
            learners
                .as_mut()
                .iter_mut()
                .map(|learner| learner.request(query.clone())),
        )
        .await
        .into_iter()
        .filter_map(|r| r.ok())
        .fold(None, |max_reply, ReadReply { processed, result }| {
            max_reply
                .map(|r: ReadReply<R>| {
                    if r.processed < processed {
                        ReadReply {
                            processed,
                            result: result.clone(),
                        }
                    } else {
                        ReadReply {
                            processed: r.processed,
                            result: r.result,
                        }
                    }
                })
                .or(Some(ReadReply { processed, result }))
        })
        .map(|r: ReadReply<R>| r.result)
    }
}

impl<Q: Clone, R: Clone> Default for Reader<Q, R> {
    fn default() -> Self {
        Self::new()
    }
}
