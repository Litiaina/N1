//! Opt-in, bounded request timing. No credentials, names or LINK keys are logged.
//! Normal binaries exclude this module and all its call-site timers.
use axum::{extract::Request, middleware::Next, response::Response};
use serde::Serialize;
use std::{
    cell::RefCell,
    future::Future,
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
    time::{Instant, SystemTime, UNIX_EPOCH},
};

const MAX_EVENTS: usize = 128;
static NEXT: AtomicU64 = AtomicU64::new(1);
tokio::task_local! { static CURRENT: Arc<Trace>; }
thread_local! { static WORKER: RefCell<Option<Arc<Trace>>> = const { RefCell::new(None) }; }

pub(crate) struct WorkerContext(Option<Arc<Trace>>);
impl WorkerContext {
    pub(crate) fn enter(trace: Option<Arc<Trace>>) -> Self {
        Self(WORKER.with(|current| current.replace(trace)))
    }
}
impl Drop for WorkerContext {
    fn drop(&mut self) {
        WORKER.with(|current| current.replace(self.0.take()));
    }
}

#[derive(Serialize)]
struct Event {
    stage: &'static str,
    start_us: u64,
    elapsed_us: u64,
}

pub(crate) struct Trace {
    started: Instant,
    events: Mutex<Vec<Event>>,
    dropped: AtomicU64,
    pub(crate) conflicts: AtomicU64,
}

impl Trace {
    pub(crate) fn current() -> Option<Arc<Self>> {
        CURRENT
            .try_with(Arc::clone)
            .ok()
            .or_else(|| WORKER.with(|current| current.borrow().clone()))
    }

    pub(crate) fn record(&self, stage: &'static str, start: Instant, end: Instant) {
        let mut events = self.events.lock().unwrap_or_else(|e| e.into_inner());
        if events.len() < MAX_EVENTS {
            events.push(Event {
                stage,
                start_us: start.duration_since(self.started).as_micros() as u64,
                elapsed_us: end.duration_since(start).as_micros() as u64,
            });
        } else {
            self.dropped.fetch_add(1, Ordering::Relaxed);
        }
    }
}

pub(crate) async fn stage<F: Future>(name: &'static str, future: F) -> F::Output {
    let trace = Trace::current();
    let start = Instant::now();
    let result = future.await;
    if let Some(trace) = trace {
        trace.record(name, start, Instant::now());
    }
    result
}

pub(crate) async fn request(request: Request, next: Next) -> Response {
    if request.method() != axum::http::Method::POST
        || !request.uri().path().starts_with("/noa/v1/upload/one-shot/")
    {
        return next.run(request).await;
    }
    let id = NEXT.fetch_add(1, Ordering::Relaxed);
    let unix_start_us = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_micros() as u64;
    let trace = Arc::new(Trace {
        started: Instant::now(),
        events: Mutex::new(Vec::with_capacity(32)),
        dropped: AtomicU64::new(0),
        conflicts: AtomicU64::new(0),
    });
    let mut response = CURRENT.scope(trace.clone(), next.run(request)).await;
    let elapsed_us = trace.started.elapsed().as_micros() as u64;
    let mut events = trace.events.lock().unwrap_or_else(|e| e.into_inner());
    events.sort_by_key(|event| event.start_us);
    eprintln!(
        "M1_UPLOAD_PROFILE {}",
        serde_json::json!({"request":id,"unix_start_us":unix_start_us,
            "elapsed_us":elapsed_us,"status":response.status().as_u16(),
            "events":*events,"dropped":trace.dropped.load(Ordering::Relaxed),
            "transaction_conflicts":trace.conflicts.load(Ordering::Relaxed)})
    );
    response.headers_mut().insert(
        "x-m1-upload-profile",
        axum::http::HeaderValue::from_str(&id.to_string()).expect("numeric profile id"),
    );
    response
}
