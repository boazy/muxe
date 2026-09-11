use std::{
    io,
    path::{Path, PathBuf},
    sync::Arc,
};

use serde_json::{Value, json};
use tempfile::TempDir;
use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
    net::UnixListener,
    sync::{Mutex, Notify, broadcast, watch},
    task::{JoinHandle, JoinSet},
};

/// One exact JSON-RPC exchange expected by [`RecordedUnixServer`].
#[derive(Clone, Debug)]
pub struct RecordedExchange {
    pub method: &'static str,
    pub params: Value,
    pub response: RecordedResponse,
}

/// The scripted response for one exact request.
///
/// `KeepOpen` acknowledges a retained subscription while the fixture continues accepting
/// subsequent independent unary connections. The retained stream stays open until the fixture
/// is dropped, making it suitable for production adapter connection tests.
#[derive(Clone, Debug)]
pub enum RecordedResponse {
    Result(Value),
    Error { code: String, message: String },
    Close,
    KeepOpen(Value),
}

/// A sequential, TempDir-owned Unix-socket fixture for concrete Herdr transport tests.
///
/// Every exchange uses a fresh connection, validates only the exact method and params, captures
/// the client-generated request ID, and correlates its scripted response with that captured ID.
/// It never starts or contacts a Herdr process.
pub struct RecordedUnixServer {
    _temp: TempDir,
    socket: PathBuf,
    requests: Arc<Mutex<Vec<Value>>>,
    requests_changed: Arc<Notify>,
    close_streams: watch::Sender<u64>,
    retained_events: broadcast::Sender<Value>,
    task: Option<JoinHandle<io::Result<()>>>,
}

impl RecordedUnixServer {
    pub fn start(temp: TempDir, exchanges: Vec<RecordedExchange>) -> io::Result<Self> {
        Self::start_inner(temp, exchanges, false)
    }

    /// Starts a recording server whose first request is followed by the raw connection made by
    /// `HerdrRuntime::probe_endpoint`. That probe deliberately sends no JSON-RPC request.
    pub fn start_with_endpoint_probe(
        temp: TempDir,
        exchanges: Vec<RecordedExchange>,
    ) -> io::Result<Self> {
        if exchanges.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "an endpoint-probe recording needs at least the ping exchange",
            ));
        }
        Self::start_inner(temp, exchanges, true)
    }

    fn start_inner(
        temp: TempDir,
        exchanges: Vec<RecordedExchange>,
        expect_endpoint_probe: bool,
    ) -> io::Result<Self> {
        let socket = temp.path().join("s");
        let listener = UnixListener::bind(&socket)?;
        let requests = Arc::new(Mutex::new(Vec::with_capacity(exchanges.len())));
        let requests_changed = Arc::new(Notify::new());
        let (close_streams, close_receiver) = watch::channel(0_u64);
        let (retained_events, _) = broadcast::channel(16);
        let task = tokio::spawn(serve(
            listener,
            exchanges,
            Arc::clone(&requests),
            Arc::clone(&requests_changed),
            close_receiver,
            retained_events.clone(),
            expect_endpoint_probe,
        ));
        Ok(Self {
            _temp: temp,
            socket,
            requests,
            requests_changed,
            close_streams,
            retained_events,
            task: Some(task),
        })
    }

    pub fn socket(&self) -> &Path {
        &self.socket
    }

    /// Returns the requests accepted so far without ending retained streams.
    pub async fn requests(&self) -> Vec<Value> {
        self.requests.lock().await.clone()
    }

    /// Waits until at least `minimum` scripted requests have been accepted.
    pub async fn wait_for_requests(&self, minimum: usize) {
        loop {
            let changed = self.requests_changed.notified();
            if self.requests.lock().await.len() >= minimum {
                return;
            }
            changed.await;
        }
    }

    /// Closes every currently retained stream. Later `KeepOpen` exchanges remain retained until
    /// the next call, allowing a production adapter to reconnect against one scripted listener.
    pub fn close_retained_streams(&self) {
        self.close_streams
            .send_modify(|generation| *generation = generation.wrapping_add(1));
    }

    /// Sends one exact host event to every retained subscription stream.
    pub fn send_retained_event(&self, event: Value) -> io::Result<()> {
        self.retained_events
            .send(event)
            .map(|_| ())
            .map_err(|error| {
                io::Error::new(
                    io::ErrorKind::BrokenPipe,
                    format!("no retained event subscription accepted the event: {error}"),
                )
            })
    }

    /// Waits for every finite scripted exchange and returns each complete raw request in order.
    /// Callers with a `KeepOpen` response must inspect [`Self::requests`] and then drop the
    /// fixture instead, because the retained stream intentionally does not complete.
    pub async fn finish(mut self) -> io::Result<Vec<Value>> {
        let task = self.task.take().expect("recorded socket task is retained");
        task.await
            .map_err(|error| io::Error::other(format!("recorded socket task failed: {error}")))??;
        Ok(self.requests.lock().await.clone())
    }
}

impl Drop for RecordedUnixServer {
    fn drop(&mut self) {
        if let Some(task) = self.task.take() {
            task.abort();
        }
    }
}

async fn serve(
    listener: UnixListener,
    exchanges: Vec<RecordedExchange>,
    requests: Arc<Mutex<Vec<Value>>>,
    requests_changed: Arc<Notify>,
    close_streams: watch::Receiver<u64>,
    retained_events: broadcast::Sender<Value>,
    expect_endpoint_probe: bool,
) -> io::Result<()> {
    let mut retained_streams = JoinSet::new();
    for exchange in exchanges {
        let (stream, _) = listener.accept().await?;
        let mut reader = BufReader::new(stream);
        let mut line = Vec::new();
        let read = reader.read_until(b'\n', &mut line).await?;
        if read == 0 {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "recorded Herdr client closed before its request",
            ));
        }
        let request: Value = serde_json::from_slice(&line).map_err(|error| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("recorded Herdr request was not JSON: {error}"),
            )
        })?;
        validate_request(&request, &exchange)?;
        let id = request
            .get("id")
            .and_then(Value::as_str)
            .filter(|id| !id.is_empty())
            .ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    "recorded Herdr request has no nonempty dynamic ID",
                )
            })?;
        let keep_open = matches!(&exchange.response, RecordedResponse::KeepOpen(_));
        let retained_generation = keep_open.then(|| *close_streams.borrow());
        let retained_event_receiver = keep_open.then(|| retained_events.subscribe());
        match exchange.response {
            RecordedResponse::Result(result) | RecordedResponse::KeepOpen(result) => {
                let response = json!({ "id": id, "result": result });
                reader
                    .get_mut()
                    .write_all(format!("{response}\n").as_bytes())
                    .await?;
                reader.get_mut().flush().await?;
            }
            RecordedResponse::Error { code, message } => {
                let response = json!({ "id": id, "error": { "code": code, "message": message } });
                reader
                    .get_mut()
                    .write_all(format!("{response}\n").as_bytes())
                    .await?;
                reader.get_mut().flush().await?;
            }
            RecordedResponse::Close => {}
        }
        requests.lock().await.push(request);
        requests_changed.notify_waiters();
        if let (Some(initial_generation), Some(mut event_receiver)) =
            (retained_generation, retained_event_receiver)
        {
            let mut retained_close_streams = close_streams.clone();
            retained_streams.spawn(async move {
                let mut retained = reader;
                loop {
                    tokio::select! {
                        changed = retained_close_streams.changed() => {
                            if changed.is_err()
                                || *retained_close_streams.borrow() != initial_generation
                            {
                                return;
                            }
                        }
                        event = event_receiver.recv() => {
                            let Ok(event) = event else {
                                return;
                            };
                            if retained
                                .get_mut()
                                .write_all(format!("{event}\n").as_bytes())
                                .await
                                .is_err()
                                || retained.get_mut().flush().await.is_err()
                            {
                                return;
                            }
                        }
                    }
                }
            });
        } else {
            drop(reader);
        }
        if expect_endpoint_probe && exchange.method == "ping" {
            let (probe, _) = listener.accept().await?;
            drop(probe);
        }
    }
    while retained_streams.join_next().await.is_some() {}
    Ok(())
}

fn validate_request(request: &Value, exchange: &RecordedExchange) -> io::Result<()> {
    let method = request.get("method").and_then(Value::as_str);
    if method != Some(exchange.method) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "recorded Herdr method mismatch: expected {:?}, got {method:?}",
                exchange.method
            ),
        ));
    }
    if request.get("params") != Some(&exchange.params) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "recorded Herdr params mismatch for {}: expected {}, got {}",
                exchange.method,
                exchange.params,
                request.get("params").unwrap_or(&Value::Null)
            ),
        ));
    }
    Ok(())
}
