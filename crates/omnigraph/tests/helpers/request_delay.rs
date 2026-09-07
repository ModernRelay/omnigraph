//! Synthetic latency at graph ObjectStore API boundaries, never a wire-request
//! emulator. The controller is retained by each store so Lance child tasks use
//! the same setting. Local staging uses its existing, unwrapped local store.
//! Active calls cover observed API futures through return and list/delete
//! streams through EOF or drop, including the injected delay. GET response body
//! transfer, wire requests/retries, RSS and unwrapped local I/O are not counted.
#![allow(dead_code)]

use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::task::{Context, Poll};
use std::time::Duration;

use async_trait::async_trait;
use futures::{Stream, StreamExt, stream::BoxStream};
use lance::io::WrappingObjectStore;
use object_store::path::Path;
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMultipartOptions, PutOptions, PutPayload, PutResult, Result, UploadPart,
};

#[derive(Debug, Default)]
struct State {
    enabled: AtomicBool,
    millis: AtomicU64,
    calls: AtomicU64,
    active_calls: AtomicU64,
    peak_active_calls: AtomicU64,
}

#[derive(Debug, Clone, Default)]
pub struct RequestDelay(Arc<State>);

pub struct ActiveDelay(RequestDelay);

/// Captures the controller so cancellation can release the original scope.
struct PendingCall(RequestDelay);

impl Drop for PendingCall {
    fn drop(&mut self) {
        self.0.0.active_calls.fetch_sub(1, Ordering::SeqCst);
    }
}

struct PendingStream<T> {
    inner: BoxStream<'static, T>,
    pending: Option<PendingCall>,
}

impl<T> Stream for PendingStream<T> {
    type Item = T;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<T>> {
        let this = self.get_mut();
        let next = this.inner.as_mut().poll_next(cx);
        if matches!(next, Poll::Ready(None)) {
            drop(this.pending.take());
        }
        next
    }
}

fn hold_stream<T: Send + 'static>(
    inner: BoxStream<'static, T>,
    pending: Option<PendingCall>,
) -> BoxStream<'static, T> {
    PendingStream { inner, pending }.boxed()
}

impl Drop for ActiveDelay {
    fn drop(&mut self) {
        self.0.0.enabled.store(false, Ordering::SeqCst);
    }
}

impl RequestDelay {
    pub fn activate(&self, millis: u64) -> ActiveDelay {
        assert!(
            !self.0.enabled.load(Ordering::SeqCst),
            "delay scopes must not overlap"
        );
        assert_eq!(
            self.active_calls(),
            0,
            "previous API calls must finish before reactivation"
        );
        self.0.calls.store(0, Ordering::SeqCst);
        self.0.peak_active_calls.store(0, Ordering::SeqCst);
        self.0.millis.store(millis, Ordering::SeqCst);
        self.0.enabled.store(true, Ordering::SeqCst);
        ActiveDelay(self.clone())
    }

    pub fn calls(&self) -> u64 {
        self.0.calls.load(Ordering::SeqCst)
    }

    pub fn active_calls(&self) -> u64 {
        self.0.active_calls.load(Ordering::SeqCst)
    }

    pub fn peak_active_calls(&self) -> u64 {
        self.0.peak_active_calls.load(Ordering::SeqCst)
    }

    async fn start(&self) -> Option<PendingCall> {
        if self.0.enabled.load(Ordering::SeqCst) {
            self.0.calls.fetch_add(1, Ordering::SeqCst);
            let active = self.0.active_calls.fetch_add(1, Ordering::SeqCst) + 1;
            self.0.peak_active_calls.fetch_max(active, Ordering::SeqCst);
            let pending = PendingCall(self.clone());
            let millis = self.0.millis.load(Ordering::SeqCst);
            if millis != 0 {
                tokio::time::sleep(Duration::from_millis(millis)).await;
            }
            Some(pending)
        } else {
            None
        }
    }
}

tokio::task_local! {
    static REQUEST_DELAY: RequestDelay;
}

pub async fn with_request_delay<F: Future>(delay: RequestDelay, body: F) -> F::Output {
    REQUEST_DELAY.scope(delay, body).await
}

/// Compose with the existing cost wrapper before any dataset is opened.
pub(super) fn wrap_counter(counter: Arc<dyn WrappingObjectStore>) -> Arc<dyn WrappingObjectStore> {
    match REQUEST_DELAY.try_with(Clone::clone) {
        Ok(delay) => Arc::new(DelayedCounter { counter, delay }),
        Err(_) => counter,
    }
}

#[derive(Debug)]
struct DelayedCounter {
    counter: Arc<dyn WrappingObjectStore>,
    delay: RequestDelay,
}

impl WrappingObjectStore for DelayedCounter {
    fn wrap(&self, prefix: &str, target: Arc<dyn ObjectStore>) -> Arc<dyn ObjectStore> {
        self.counter.wrap(
            prefix,
            Arc::new(DelayedStore {
                target,
                delay: self.delay.clone(),
            }),
        )
    }
}

#[derive(Debug)]
struct DelayedStore {
    target: Arc<dyn ObjectStore>,
    delay: RequestDelay,
}

impl fmt::Display for DelayedStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "DelayedStore({})", self.target)
    }
}

#[async_trait]
impl ObjectStore for DelayedStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> Result<PutResult> {
        let _pending = self.delay.start().await;
        self.target.put_opts(location, payload, opts).await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> Result<Box<dyn MultipartUpload>> {
        let _pending = self.delay.start().await;
        Ok(Box::new(DelayedUpload {
            target: self.target.put_multipart_opts(location, opts).await?,
            delay: self.delay.clone(),
        }))
    }

    async fn get_opts(&self, location: &Path, opts: GetOptions) -> Result<GetResult> {
        let _pending = self.delay.start().await;
        self.target.get_opts(location, opts).await
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, Result<Path>>,
    ) -> BoxStream<'static, Result<Path>> {
        let target = self.target.clone();
        let delay = self.delay.clone();
        futures::stream::once(async move {
            let pending = delay.start().await;
            hold_stream(target.delete_stream(locations), pending)
        })
        .flatten()
        .boxed()
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, Result<ObjectMeta>> {
        let target = self.target.clone();
        let delay = self.delay.clone();
        let prefix = prefix.cloned();
        futures::stream::once(async move {
            let pending = delay.start().await;
            hold_stream(target.list(prefix.as_ref()), pending)
        })
        .flatten()
        .boxed()
    }

    fn list_with_offset(
        &self,
        prefix: Option<&Path>,
        offset: &Path,
    ) -> BoxStream<'static, Result<ObjectMeta>> {
        let target = self.target.clone();
        let delay = self.delay.clone();
        let prefix = prefix.cloned();
        let offset = offset.clone();
        futures::stream::once(async move {
            let pending = delay.start().await;
            hold_stream(target.list_with_offset(prefix.as_ref(), &offset), pending)
        })
        .flatten()
        .boxed()
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> Result<ListResult> {
        let _pending = self.delay.start().await;
        self.target.list_with_delimiter(prefix).await
    }

    async fn copy_opts(&self, from: &Path, to: &Path, opts: CopyOptions) -> Result<()> {
        let _pending = self.delay.start().await;
        self.target.copy_opts(from, to, opts).await
    }
}

#[derive(Debug)]
struct DelayedUpload {
    target: Box<dyn MultipartUpload>,
    delay: RequestDelay,
}

#[async_trait]
impl MultipartUpload for DelayedUpload {
    fn put_part(&mut self, data: PutPayload) -> UploadPart {
        let part = self.target.put_part(data);
        let delay = self.delay.clone();
        Box::pin(async move {
            let _pending = delay.start().await;
            part.await
        })
    }

    async fn complete(&mut self) -> Result<PutResult> {
        let _pending = self.delay.start().await;
        self.target.complete().await
    }

    async fn abort(&mut self) -> Result<()> {
        let _pending = self.delay.start().await;
        self.target.abort().await
    }
}
