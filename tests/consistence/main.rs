use std::sync::Arc;

use minipaxos::{
    acceptor::Acceptor,
    communication::Requester,
    learner::Learner, proposer::Proposer,
};

mod backend;
mod communication;

use backend::{KVDataBase, KVSet};
use communication::LocalServer;
use tokio::sync::Mutex;

const LEARNER_NUM: usize = 3;
const ACCEPTOR_NUM: usize = 3;

#[tokio::test]
async fn consistence() {
    let learners: Vec<_> = (0..LEARNER_NUM)
        .map(|_| {
            let be = KVDataBase::new();
            let learner = Arc::new(Mutex::new(Learner::new(ACCEPTOR_NUM, be)));
            let learner_server = LocalServer::new();

            (learner, learner_server)
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
    for (learner, mut learner_server) in learners.into_iter() {
        Learner::serve_learner(learner.clone(), &mut learner_server).await;
        let learner_server = Arc::new(Mutex::new(learner_server));

        for j in 0..ACCEPTOR_NUM {
            let learn_requester = LocalServer::new_requester(&learner_server);
            learn_requesters[j].push(learn_requester);
        }
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
}
