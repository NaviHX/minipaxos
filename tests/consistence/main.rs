use std::sync::Arc;

use minipaxos::{
    acceptor::Acceptor,
    communication::Requester,
    learner::Learner, proposer::Proposer, reader::Reader,
};

mod backend;
mod communication;

use backend::{KVDataBase, KVSet};
use communication::LocalServer;
use tokio::sync::Mutex;

use crate::backend::KVGet;

const LEARNER_NUM: usize = 3;
const ACCEPTOR_NUM: usize = 3;

#[tokio::test]
async fn consistence() {
    let learners: Vec<_> = (0..LEARNER_NUM)
        .map(|_| {
            let be = KVDataBase::new();
            let learner = Arc::new(Mutex::new(Learner::new(ACCEPTOR_NUM, be)));
            let learner_server = LocalServer::new();
            let reader_server = LocalServer::new();

            (learner, learner_server, reader_server)
        })
        .collect();

    let acceptors: Vec<_> = (0..ACCEPTOR_NUM)
        .map(|_| {
            let (acceptor, receiver) = Acceptor::<KVSet>::new(uuid::Uuid::new_v4(), 16);
            let acceptor = Arc::new(Mutex::new(acceptor));
            let accept_server = LocalServer::new();
            let prepare_server = LocalServer::new();

            (acceptor, prepare_server, accept_server, receiver)
        })
        .collect();

    let mut learn_requesters = vec![vec![]; ACCEPTOR_NUM];
    let mut read_requesters = vec![];
    for (learner, mut learner_server, mut reader_server) in learners.into_iter() {
        Learner::serve_learner(learner.clone(), &mut learner_server).await;
        let learner_server = Arc::new(Mutex::new(learner_server));

        for it in learn_requesters.iter_mut() {
            let learn_requester = LocalServer::new_requester(&learner_server);
            it.push(learn_requester);
        }

        Learner::serve_reader(learner.clone(), &mut reader_server).await;
        let reader_server = Arc::new(Mutex::new(reader_server));
        let read_requester = LocalServer::new_requester(&reader_server);
        let read_requester: Box<dyn Requester<_, _, Output = _>> = Box::new(read_requester);
        read_requesters.push(read_requester);
    }

    let mut prepare_requesters = vec![];
    let mut accept_requesters = vec![];
    for ((acceptor, mut prepare_server, mut accept_server, inner_receiver), learn_requesters) in
        acceptors.into_iter().zip(learn_requesters.into_iter())
    {
        Acceptor::serve_preparer(acceptor.clone(), &mut prepare_server).await;
        Acceptor::serve_acceptor(acceptor.clone(), &mut accept_server).await;
        let learn_requesters = learn_requesters.into_iter().map(|b| {
                let b: Box<dyn Requester<_, _, Output = _> + Send> = Box::new(b);
                b
        }).collect();
        tokio::spawn(Acceptor::process_learn_request(learn_requesters, inner_receiver));

        let prepare_server = Arc::new(Mutex::new(prepare_server));
        let accept_server = Arc::new(Mutex::new(accept_server));

        let prepare_requester = Box::new(LocalServer::new_requester(&prepare_server)) as Box<dyn Requester<_, _, Output = _>>;
        let accept_requester = Box::new(LocalServer::new_requester(&accept_server)) as Box<dyn Requester<_, _, Output = _>>;

        prepare_requesters.push(prepare_requester);
        accept_requesters.push(accept_requester);
    }

    let mut proposer = Proposer::new();
    proposer.propose(KVSet::new("1", "1"), &mut prepare_requesters, &mut accept_requesters, 50..100).await;
    proposer.propose(KVSet::new("2", "2"), &mut prepare_requesters, &mut accept_requesters, 50..100).await;
    proposer.propose(KVSet::new("3", "3"), &mut prepare_requesters, &mut accept_requesters, 50..100).await;
    proposer.propose(KVSet::new("4", "4"), &mut prepare_requesters, &mut accept_requesters, 50..100).await;

    let mut reader = Reader::new();
    assert_eq!(reader.read(KVGet::new("1"), &mut read_requesters).await, Some(Some("1".to_owned())));
    assert_eq!(reader.read(KVGet::new("2"), &mut read_requesters).await, Some(Some("2".to_owned())));
    assert_eq!(reader.read(KVGet::new("3"), &mut read_requesters).await, Some(Some("3".to_owned())));
    assert_eq!(reader.read(KVGet::new("4"), &mut read_requesters).await, Some(Some("4".to_owned())));
}
