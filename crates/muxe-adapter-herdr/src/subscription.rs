//! Retained Herdr event-subscription stream.
//!
//! Ordinary Herdr requests use fresh guarded connections. The event
//! subscription is the single exception: after its initial `events.subscribe`
//! request the connection stays open and the server pushes matching event lines
//! onto it. This module owns exactly that long-lived stream. No
//! ordinary request is ever multiplexed onto it, and no mutation is ever
//! replayed through it: reconnecting re-sends only the retained subscribe
//! request on a fresh connection, which establishes a subscription rather than
//! changing host state.
//!
//! Liveness is defined by stream behavior: EOF, malformed JSON, and transport
//! errors are reported as continuity loss. A valid subscription may remain
//! quiet indefinitely until its next matching event.

use std::time::Duration;

use serde_json::Value;
use tokio::{io::BufReader, net::UnixStream, time::timeout};

use crate::{
    generated::method_metadata,
    runtime::{HerdrRuntime, IncarnationLease},
    transport::{
        DeliveryState, SocketError, encode_request, parse_response, read_response_line, write_line,
    },
};

/// Configuration for one retained subscription. The `params` value is the exact
/// `events.subscribe` params retained for reconnects; the initial subscribe
/// response deadline is a transport wait bound, not a Muxe detach signal.
#[derive(Clone, Debug)]
pub struct SubscriptionConfig {
    pub params: Value,
    /// Bound on the initial subscribe response on a fresh connection.
    pub subscribe_timeout: Duration,
}

/// One opaque line received on the subscription stream.
#[derive(Clone, Debug, PartialEq)]
pub enum SubscriptionEvent {
    Event(Value),
}

/// The long-lived `events.subscribe` stream on its own dedicated connection.
#[derive(Debug)]
pub struct EventSubscription {
    reader: BufReader<UnixStream>,
    request_id: String,
    params: Value,
    lease: IncarnationLease,
}

impl EventSubscription {
    /// Sends the retained subscribe request only after a fresh observed stream
    /// matches the exact runtime/epoch lease. The initial response must be
    /// correlated before the subscription becomes installable.
    ///
    /// # Errors
    ///
    /// Returns [`SocketError::EndpointReplaced`] with `NotSent` when the
    /// runtime or connected endpoint differs from `lease`. Other transport and
    /// protocol failures preserve their normal delivery classification.
    pub(crate) async fn connect_expected(
        runtime: &HerdrRuntime,
        lease: IncarnationLease,
        config: SubscriptionConfig,
    ) -> Result<(Self, Value), SocketError> {
        let metadata =
            method_metadata("events.subscribe").ok_or_else(|| SocketError::Protocol {
                delivery: DeliveryState::NotSent,
                message: "bundled Herdr metadata does not declare events.subscribe".to_owned(),
            })?;
        if metadata.method != "events.subscribe" {
            return Err(SocketError::Protocol {
                delivery: DeliveryState::NotSent,
                message: "bundled events.subscribe metadata names the wrong method".to_owned(),
            });
        }
        let id = runtime.client().next_id()?;
        let mut line = encode_request(metadata.method, &id, &config.params)?;
        line.push(b'\n');
        let mut stream = runtime.connect_expected_subscription(&lease).await?;
        write_line(&mut stream, &line).await?;
        let mut reader = BufReader::new(stream);
        let response_line = timeout(config.subscribe_timeout, read_response_line(&mut reader))
            .await
            .map_err(|_| SocketError::Timeout {
                delivery: DeliveryState::MayHaveReachedHost,
            })??;
        let response =
            serde_json::from_slice(&response_line).map_err(|source| SocketError::InvalidJson {
                delivery: DeliveryState::MayHaveReachedHost,
                source,
            })?;
        match parse_response(&response, &id)? {
            crate::transport::HerdrResponse::Success(result) => Ok((
                Self {
                    reader,
                    request_id: id,
                    params: config.params,
                    lease,
                },
                result,
            )),
            crate::transport::HerdrResponse::Error { code, message } => {
                Err(SocketError::Protocol {
                    delivery: DeliveryState::MayHaveReachedHost,
                    message: format!("Herdr rejected events.subscribe with {code}: {message}"),
                })
            }
        }
    }

    /// Waits for the next stream line.
    ///
    /// # Errors
    ///
    /// Returns [`SocketError`] when the stream closes, transport fails, or the
    /// next line is not valid JSON.
    pub async fn next_event(&mut self) -> Result<SubscriptionEvent, SocketError> {
        let line = read_response_line(&mut self.reader).await?;
        let event = serde_json::from_slice(&line).map_err(|source| SocketError::InvalidJson {
            delivery: DeliveryState::MayHaveReachedHost,
            source,
        })?;
        Ok(SubscriptionEvent::Event(event))
    }

    /// The generated request ID of the retained subscribe request.
    pub fn request_id(&self) -> &str {
        &self.request_id
    }

    /// The retained subscribe params, re-sent verbatim on reconnect.
    pub fn params(&self) -> &Value {
        &self.params
    }

    #[must_use]
    pub(crate) fn lease(&self) -> &IncarnationLease {
        &self.lease
    }
}

#[cfg(test)]
mod tests {
    use std::{path::Path, sync::Arc};

    use serde_json::json;
    use tempfile::TempDir;
    use tokio::{
        io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader},
        net::UnixListener,
    };

    use super::*;
    use crate::{generated::BUNDLED_PROTOCOL, runtime::IncarnationEpoch};

    fn config() -> SubscriptionConfig {
        SubscriptionConfig {
            params: json!({"subscriptions": [{"type": "tab.focused"}]}),
            subscribe_timeout: Duration::from_secs(5),
        }
    }

    async fn read_request(stream: UnixStream) -> (BufReader<UnixStream>, String, Value) {
        let mut reader = BufReader::new(stream);
        let mut line = Vec::new();
        reader.read_until(b'\n', &mut line).await.unwrap();
        let request: Value = serde_json::from_slice(&line).unwrap();
        let id = request["id"].as_str().unwrap().to_owned();
        (reader, id, request)
    }

    async fn answer_ping(listener: &UnixListener) {
        let (stream, _) = listener.accept().await.unwrap();
        let (mut reader, id, request) = read_request(stream).await;
        assert_eq!(request["method"], "ping");
        reader
            .write_all(
                format!(
                    "{{\"id\":\"{id}\",\"result\":{{\"type\":\"pong\",\"protocol\":{BUNDLED_PROTOCOL},\"version\":\"0.8.2\"}}}}\n"
                )
                .as_bytes(),
            )
            .await
            .unwrap();
    }

    async fn test_runtime(listener: &Arc<UnixListener>, path: &Path) -> HerdrRuntime {
        let (runtime, ()) = tokio::join!(
            HerdrRuntime::for_subscription_test(path),
            answer_ping(listener)
        );
        runtime.unwrap()
    }

    async fn read_subscribe_line(stream: UnixStream) -> (BufReader<UnixStream>, String, Value) {
        let (reader, id, request) = read_request(stream).await;
        assert_eq!(request["method"], "events.subscribe");
        (reader, id, request["params"].clone())
    }

    #[tokio::test]
    async fn subscribe_keeps_stream_open_for_events() {
        let temp = TempDir::new().unwrap();
        let path = temp.path().join("herdr.sock");
        let listener = Arc::new(UnixListener::bind(&path).unwrap());
        let runtime = test_runtime(&listener, &path).await;
        let server_listener = Arc::clone(&listener);
        let server = tokio::spawn(async move {
            let (stream, _) = server_listener.accept().await.unwrap();
            let (mut reader, id, params) = read_subscribe_line(stream).await;
            assert_eq!(params["subscriptions"][0]["type"], "tab.focused");
            reader
                .write_all(
                    format!("{{\"id\":\"{id}\",\"result\":{{\"subscribed\":true}}}}\n").as_bytes(),
                )
                .await
                .unwrap();
            reader
                .write_all(b"{\"type\":\"tab.focused\",\"tab_id\":\"t1\"}\n")
                .await
                .unwrap();
        });
        let lease = runtime.lease(IncarnationEpoch::INITIAL);
        let (mut subscription, result) =
            EventSubscription::connect_expected(&runtime, lease, config())
                .await
                .unwrap();
        assert_eq!(result, json!({"subscribed": true}));
        assert_eq!(
            subscription.params(),
            &json!({"subscriptions": [{"type": "tab.focused"}]})
        );
        assert_eq!(
            subscription.next_event().await.unwrap(),
            SubscriptionEvent::Event(json!({"type": "tab.focused", "tab_id": "t1"}))
        );
        server.await.unwrap();
    }

    #[tokio::test]
    async fn subscribe_rejects_correlation_mismatch() {
        let temp = TempDir::new().unwrap();
        let path = temp.path().join("herdr.sock");
        let listener = Arc::new(UnixListener::bind(&path).unwrap());
        let runtime = test_runtime(&listener, &path).await;
        let server_listener = Arc::clone(&listener);
        let server = tokio::spawn(async move {
            let (stream, _) = server_listener.accept().await.unwrap();
            let (mut reader, _id, _params) = read_subscribe_line(stream).await;
            reader
                .write_all(b"{\"id\":\"different\",\"result\":{}}\n")
                .await
                .unwrap();
        });
        let lease = runtime.lease(IncarnationEpoch::INITIAL);
        let error = EventSubscription::connect_expected(&runtime, lease, config())
            .await
            .unwrap_err();
        server.await.unwrap();
        assert!(matches!(error, SocketError::Protocol { .. }));
        assert_eq!(error.delivery(), DeliveryState::MayHaveReachedHost);
    }

    #[tokio::test]
    async fn eof_after_subscribe_is_continuity_loss_for_reconnect() {
        let temp = TempDir::new().unwrap();
        let path = temp.path().join("herdr.sock");
        let listener = Arc::new(UnixListener::bind(&path).unwrap());
        let runtime = test_runtime(&listener, &path).await;
        let server_listener = Arc::clone(&listener);
        let server = tokio::spawn(async move {
            let (stream, _) = server_listener.accept().await.unwrap();
            let (mut reader, id, _params) = read_subscribe_line(stream).await;
            reader
                .write_all(format!("{{\"id\":\"{id}\",\"result\":{{}}}}\n").as_bytes())
                .await
                .unwrap();
        });
        let lease = runtime.lease(IncarnationEpoch::INITIAL);
        let (mut subscription, _) = EventSubscription::connect_expected(&runtime, lease, config())
            .await
            .unwrap();
        server.await.unwrap();
        let error = subscription.next_event().await.unwrap_err();
        assert!(matches!(error, SocketError::EarlyEof { .. }));
        assert_eq!(error.delivery(), DeliveryState::MayHaveReachedHost);
    }

    #[tokio::test]
    async fn quiet_stream_survives_old_silence_bound_then_reports_malformed_line() {
        tokio::time::pause();
        let temp = TempDir::new().unwrap();
        let path = temp.path().join("herdr.sock");
        let listener = Arc::new(UnixListener::bind(&path).unwrap());
        let runtime = test_runtime(&listener, &path).await;
        let (release_tx, release_rx) = tokio::sync::oneshot::channel();
        let server_listener = Arc::clone(&listener);
        let server = tokio::spawn(async move {
            let (stream, _) = server_listener.accept().await.unwrap();
            let (mut reader, id, _params) = read_subscribe_line(stream).await;
            reader
                .write_all(format!("{{\"id\":\"{id}\",\"result\":{{}}}}\n").as_bytes())
                .await
                .unwrap();
            release_rx.await.unwrap();
            reader
                .write_all(b"{\"type\":\"tab.focused\",\"tab_id\":\"t1\"}\n")
                .await
                .unwrap();
            reader.write_all(b"{not-json}\n").await.unwrap();
        });
        let lease = runtime.lease(IncarnationEpoch::INITIAL);
        let (mut subscription, _) = EventSubscription::connect_expected(&runtime, lease, config())
            .await
            .unwrap();
        let waiting = tokio::spawn(async move {
            let event = subscription.next_event().await;
            let malformed = subscription.next_event().await;
            (event, malformed)
        });
        tokio::task::yield_now().await;
        tokio::time::advance(Duration::from_secs(31)).await;
        release_tx.send(()).unwrap();
        let (event, malformed) = waiting.await.unwrap();
        assert_eq!(
            event.unwrap(),
            SubscriptionEvent::Event(json!({"type": "tab.focused", "tab_id": "t1"}))
        );
        assert!(matches!(
            malformed,
            Err(SocketError::InvalidJson {
                delivery: DeliveryState::MayHaveReachedHost,
                ..
            })
        ));
        server.await.unwrap();
    }

    #[tokio::test]
    async fn rebound_runtime_to_subscribe_writes_zero_bytes_and_cannot_publish() {
        let temp = TempDir::new().unwrap();
        let path = temp.path().join("herdr.sock");
        let listener_a = Arc::new(UnixListener::bind(&path).unwrap());
        let runtime = test_runtime(&listener_a, &path).await;
        let lease = runtime.lease(IncarnationEpoch::INITIAL);

        std::fs::remove_file(&path).unwrap();
        let listener_b = UnixListener::bind(&path).unwrap();
        let server_b = tokio::spawn(async move {
            let (mut stream, _) = listener_b.accept().await.unwrap();
            let mut bytes = Vec::new();
            tokio::time::timeout(Duration::from_secs(2), stream.read_to_end(&mut bytes))
                .await
                .expect("rejected subscription closes promptly")
                .unwrap();
            bytes
        });
        let error = EventSubscription::connect_expected(&runtime, lease, config())
            .await
            .expect_err("replacement cannot produce a publishable subscription");

        assert!(matches!(error, SocketError::EndpointReplaced { .. }));
        assert_eq!(error.delivery(), DeliveryState::NotSent);
        assert!(
            server_b.await.unwrap().is_empty(),
            "subscription guard sends zero bytes to the replacement"
        );
        drop(listener_a);
    }
}
