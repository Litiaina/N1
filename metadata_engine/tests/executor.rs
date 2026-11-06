use n1_link::PoolId;
mod common;
use m1::{Error, MetadataEngine, MetadataExecutor, Options, RecordKey, Table};
use std::{sync::mpsc, time::Duration};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cancelled_caller_keeps_worker_admission_and_shutdown_drains_accepted_mutation() {
    let directory = common::directory(None);
    let engine = MetadataEngine::open(directory.path(), Options::default()).unwrap();
    let executor = MetadataExecutor::new(engine.clone(), 1).unwrap();
    let (started, wait_started) = tokio::sync::oneshot::channel();
    let (release, wait_release) = mpsc::channel();
    let worker = executor.clone();
    let task = tokio::spawn(async move {
        worker
            .execute(move |engine| {
                started.send(()).unwrap();
                wait_release.recv_timeout(Duration::from_secs(10)).unwrap();
                engine.transact(1, |tx| {
                    tx.put(
                        Table::Entries,
                        &RecordKey::new(PoolId([1; 16]), "owned")?,
                        b"committed",
                    )
                })
            })
            .await
    });
    wait_started.await.unwrap();
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
    assert!(matches!(
        executor.execute(|_| Ok(())).await,
        Err(Error::Busy)
    ));
    let shutdown_worker = executor.clone();
    let shutdown = tokio::spawn(async move {
        shutdown_worker.shutdown().await;
    });
    tokio::task::yield_now().await;
    assert!(!shutdown.is_finished());
    release.send(()).unwrap();
    tokio::time::timeout(Duration::from_secs(10), shutdown)
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(
        executor.execute(|_| Ok(())).await,
        Err(Error::Closed)
    ));
    let key = RecordKey::new(PoolId([1; 16]), "owned").unwrap();
    assert_eq!(
        engine
            .snapshot()
            .get(Table::Entries, &key)
            .unwrap()
            .unwrap(),
        b"committed"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn worker_panics_are_reported_and_capacity_is_released() {
    let directory = common::directory(None);
    let engine = MetadataEngine::open(directory.path(), Options::default()).unwrap();
    let executor = MetadataExecutor::new(engine, 1).unwrap();
    assert!(matches!(
        executor
            .execute::<()>(|_| panic!("injected worker panic"))
            .await,
        Err(Error::Worker(_))
    ));
    executor.execute(|_| Ok(())).await.unwrap();
    executor.shutdown().await;
}
