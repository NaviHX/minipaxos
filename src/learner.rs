use tracing::debug;

#[derive(Clone)]
pub struct LearnMessage<T: Clone> {
    pub instance: u64,
    pub acceptor_id: AcceptorID,
    pub proposal: T,
}
#[derive(Clone)]
pub struct LearnReply;

pub struct ReadReply<O> {
    pub processed: u64,
    pub result: O,
}

pub trait Backend<T, Q> {
    type Output;
    fn process(&mut self, proposal: T);
    fn read(&self, request: Q) -> Self::Output;
}

use std::cmp::Reverse;
use std::collections::{BinaryHeap, HashMap};
use std::marker::PhantomData;
use std::sync::Arc;
use tokio::sync::Mutex;

use crate::acceptor::AcceptorID;
use crate::communication::Server;

pub struct InnerLearner<T> {
    accepted_proposals: HashMap<AcceptorID, T>,
    acceptor_num: usize,
}

impl<T: Clone + Eq> InnerLearner<T> {
    pub fn new(acceptor_num: usize) -> Self {
        Self {
            accepted_proposals: HashMap::new(),
            acceptor_num,
        }
    }

    pub fn learn(&mut self, acceptor_id: AcceptorID, proposal: T) -> Option<T> {
        self.accepted_proposals
            .entry(acceptor_id)
            .and_modify(|v| *v = proposal.clone())
            .or_insert(proposal.clone());

        let learned_num = self.accepted_proposals.len();
        if learned_num < (self.acceptor_num + 1) / 2 {
            return None;
        }

        let mut quorom_proposal = None;
        let mut count = 0;
        for p in self.accepted_proposals.values() {
            if quorom_proposal.as_ref().map(|qp| qp == p).unwrap_or(true) {
                count += 1;

                if count == 1 {
                    quorom_proposal = Some(p.clone());
                }
            } else {
                count -= 1;
                if count == 0 {
                    quorom_proposal = None
                }
            }
        }

        if count + learned_num > self.acceptor_num {
            quorom_proposal
        } else {
            None
        }
    }
}

struct OrdTag<T, S>(T, S);

impl<T: PartialEq, S> PartialEq for OrdTag<T, S> {
    fn eq(&self, other: &Self) -> bool {
        self.0 == other.0
    }
}

impl<T: Eq, S> Eq for OrdTag<T, S> {}

impl<T: PartialOrd, S> PartialOrd for OrdTag<T, S> {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        self.0.partial_cmp(&other.0)
    }
}

impl<T: Ord, S> Ord for OrdTag<T, S> {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.0.cmp(&other.0)
    }
}

pub struct Learner<T, B, Q>
where
    B: Backend<T, Q>,
    T: Clone + Eq,
{
    backend: B,
    acceptor_num: usize,

    proposals: BinaryHeap<OrdTag<Reverse<u64>, T>>,
    processed: u64,
    inner_learners: HashMap<u64, InnerLearner<T>>,

    phantom_q: PhantomData<Q>,
}

#[derive(PartialEq, Eq)]
pub enum ProcessPoll {
    Ready,
    Wait,
}

impl From<bool> for ProcessPoll {
    fn from(value: bool) -> Self {
        match value {
            true => Self::Ready,
            false => Self::Wait,
        }
    }
}

impl<T, B, Q> Learner<T, B, Q>
where
    B: Backend<T, Q> + 'static + Send,
    T: Clone + Eq + 'static + Send,
    Q: Send + 'static,
{
    pub fn new(acceptor_num: usize, backend: B) -> Self {
        Self {
            backend,
            acceptor_num,
            proposals: BinaryHeap::new(),
            processed: 0,
            inner_learners: HashMap::new(),
            phantom_q: PhantomData,
        }
    }

    #[tracing::instrument(
        skip(self, proposal)
    )]
    pub fn learn(
        &mut self,
        LearnMessage {
            instance,
            acceptor_id: acceptor,
            proposal,
        }: LearnMessage<T>,
    ) -> LearnReply {
        debug!("Received Learn Request: {}", instance);

        if instance < self.processed {
            return LearnReply;
        }

        let learner = self
            .inner_learners
            .entry(instance)
            .or_insert_with(|| InnerLearner::new(self.acceptor_num));
        if let Some(quorom_proposal) = learner.learn(acceptor, proposal) {
            debug!("Push Learn Request: {}", instance);

            self.proposals
                .push(OrdTag(Reverse(instance), quorom_proposal));
            if instance == self.processed {
                while self.process() == ProcessPoll::Ready {}
                return LearnReply;
            }
        }

        LearnReply
    }

    pub fn process(&mut self) -> ProcessPoll {
        let peek = self.proposals.peek();
        if let Some(OrdTag(Reverse(instance), proposal)) = peek {
            if *instance == self.processed {
                self.backend.process(proposal.clone());
                self.proposals.pop();
                self.processed += 1;

                let peek = self.proposals.peek();
                return peek
                    .map(|OrdTag(Reverse(instance), _)| *instance == self.processed)
                    .map(ProcessPoll::from)
                    .unwrap_or(ProcessPoll::Wait);
            }
        }

        ProcessPoll::Wait
    }

    pub fn read(&self, request: Q) -> ReadReply<B::Output> {
        let res = self.backend.read(request);
        ReadReply {
            processed: self.processed,
            result: res,
        }
    }

    pub async fn serve_learner(
        learner: Arc<Mutex<Self>>,
        server: &mut impl Server<'_, LearnMessage<T>, Output = LearnReply>,
    ) {
        server
            .run(move |message| {
                let learner = learner.clone();
                Box::pin(async move { learner.lock().await.learn(message) })
            })
            .await;
    }

    pub async fn serve_reader(
        learner: Arc<Mutex<Self>>,
        server: &mut impl Server<'_, Q, Output = ReadReply<B::Output>>,
    ) {
        server
            .run(move |message| {
                let learner = learner.clone();
                Box::pin(async move { learner.lock().await.read(message) })
            })
            .await;
    }
}

#[cfg(test)]
mod test {
    use super::InnerLearner;

    #[test]
    fn learner_test() {
        let acceptor_id = [
            uuid::uuid!("00000000-0000-0000-0000-000000000001"),
            uuid::uuid!("00000000-0000-0000-0000-000000000002"),
            uuid::uuid!("00000000-0000-0000-0000-000000000003"),
        ];
        let acceptor_num = acceptor_id.len();
        let mut learner = InnerLearner::new(acceptor_num);

        assert!(learner.learn(acceptor_id[0], false).is_none());
        assert!(learner.learn(acceptor_id[1], true).is_none());
        assert!(learner.learn(acceptor_id[2], true).unwrap_or(false));
    }
}
