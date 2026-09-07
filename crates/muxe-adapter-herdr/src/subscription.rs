//! Retained Herdr event-subscription stream.
//!
//! Ordinary Herdr requests are one unary exchange per fresh connection (see
//! [`HerdrSocketClient`](crate::transport::HerdrSocketClient)). The event
//! subscription is the single exception: after its initial `events.subscribe`
//! request the connection stays open and the server multiplexes heartbeat and
//! event lines onto it. This module owns exactly that long-lived stream. No
//! ordinary request is ever multiplexed onto it, and no mutation is ever
//! replayed through it: reconnecting re-sends only the retained subscribe
//! request on a fresh connection, which establishes a subscription rather than
//! changing host state.
//!
//! Liveness needs no heartbeat wire-shape knowledge: every received line,
//! heartbeat or event, restarts the caller-configured silence deadline. An
//! expired deadline is a transport wait bound, not a Muxe detach, and every
//! post-flush failure already reports `MayHaveReachedHost` so the broker
//! surfaces `outcome_unknown` instead of retrying.

use std::time::Duration;

use serde_json::Value;
use tokio::{
    io::BufReader,
    net::UnixStream,
    time::{Instant, timeout},
};

use crate::{
    generated::method_metadata,
    transport::{
        DeliveryState, HerdrSocketClient, SocketError, encode_request, parse_response,
        read_response_line, write_line,
    },
};

/// Configuration for one retained subscription. The `params` value is the exact
/// `events.subscribe` params retained for reconnects; timeouts are transport
/// wait bounds, never Muxe detach signals.
#[derive(Clone, Debug)]
pub struct SubscriptionConfig {
    pub params: Value,
    /// Bound on the initial subscribe response on a fresh connection.
    pub subscribe_timeout: Duration,
    /// Silence bound restarted by every received line, heartbeat or event.
    pub max_silence: Duration,
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
    max_silence: Duration,
}

impl EventSubscription {
    /// Sends the retained subscribe request on a fresh connection and reads exactly
    /// one correlated initial response, keeping the stream open for events. The
    /// returned value is the server's subscribe result for the broker to record.
    ///
    /// # Errors
    ///
    /// Returns `SocketError` when the subscribe request cannot be sent, no
    /// correlated initial response arrives, or the server rejects the subscription.
    pub async fn connect(
        client: &HerdrSocketClient,
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
        let id = client.next_id()?;
        let mut line = encode_request(metadata.method, &id, &config.params)?;
        line.push(b'\n');
        let mut stream = client.connect_stream().await?;
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
                    max_silence: config.max_silence,
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

    /// Waits for the next stream line until [`SubscriptionConfig::max_silence`]
    /// elapses. Every received line restarts the bound on the following call; an
    /// expired bound reports `MayHaveReachedHost` so the broker treats a lost
    /// subscription as continuity loss, never as a detach or a replayable request.
    ///
    /// # Errors
    ///
    /// Returns `SocketError` when the silence bound expires or the next line is
    /// not valid JSON.
    pub async fn next_event(&mut self) -> Result<SubscriptionEvent, SocketError> {
        let line = timeout(self.max_silence, read_response_line(&mut self.reader))
            .await
            .map_err(|_| SocketError::Timeout {
                delivery: DeliveryState::MayHaveReachedHost,
            })??;
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

    /// Re-establishes the retained subscription on fresh connections until `grace`
    /// elapses, waiting `retry_interval` between attempts. Only the subscribe
    /// request itself is re-sent; host mutations are never replayed here. Attempts
    /// and sleeps use virtual-time-compatible clocks, so tests drive this
    /// deterministically with a paused clock.
    ///
    /// # Errors
    ///
    /// Returns the last `SocketError` when no reconnect succeeds within `grace`.
    pub async fn reconnect_with_grace(
        client: &HerdrSocketClient,
        config: SubscriptionConfig,
        grace: Duration,
        retry_interval: Duration,
    ) -> Result<(Self, Value), SocketError> {
        let deadline = Instant::now() + grace;
        let mut last_error = None;
        while Instant::now() < deadline {
            match Self::connect(client, config.clone()).await {
                success @ Ok(_) => return success,
                Err(error) => last_error = Some(error),
            }
            let remaining = deadline
                .checked_duration_since(Instant::now())
                .unwrap_or_default();
            if remaining.is_zero() {
                break;
            }
            tokio::time::sleep(retry_interval.min(remaining)).await;
        }
        Err(last_error.unwrap_or(SocketError::Timeout {
            delivery: DeliveryState::MayHaveReachedHost,
        }))
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;
    use tempfile::TempDir;
    use tokio::{
        io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
        net::UnixListener,
    };

    use super::*;
    use crate::transport::HerdrSocketClient;

    fn config() -> SubscriptionConfig {
        SubscriptionConfig {
            params: json!({"subscriptions": [{"type": "tab.focused"}]}),
            subscribe_timeout: Duration::from_secs(5),
            max_silence: Duration::from_secs(30),
        }
    }

    async fn read_subscribe_line(stream: UnixStream) -> (BufReader<UnixStream>, String, Value) {
        let mut reader = BufReader::new(stream);
        let mut line = Vec::new();
        reader.read_until(b'\n', &mut line).await.unwrap();
        let request: Value = serde_json::from_slice(&line).unwrap();
        assert_eq!(request["method"], "events.subscribe");
        let id = request["id"].as_str().unwrap().to_owned();
        (reader, id, request["params"].clone())
    }

    #[tokio::test]
    async fn subscribe_keeps_stream_open_for_events() {
        let temp = TempDir::new().unwrap();
        let path = temp.path().join("herdr.sock");
        let listener = UnixListener::bind(&path).unwrap();
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let (mut reader, id, params) = read_subscribe_line(stream).await;
            assert_eq!(params["subscriptions"][0]["type"], "tab.focused");
            reader
                .write_all(
                    format!("{{\"id\":\"{id}\",\"result\":{{\"subscribed\":true}}}}\n").as_bytes(),
                )
                .await
                .unwrap();
            reader
                .write_all(b"{\"type\":\"heartbeat\"}\n")
                .await
                .unwrap();
            reader
                .write_all(b"{\"type\":\"tab.focused\",\"tab_id\":\"t1\"}\n")
                .await
                .unwrap();
        });
        let client = HerdrSocketClient::new(&path);
        let (mut subscription, result) =
            EventSubscription::connect(&client, config()).await.unwrap();
        assert_eq!(result, json!({"subscribed": true}));
        assert_eq!(
            subscription.params(),
            &json!({"subscriptions": [{"type": "tab.focused"}]})
        );
        assert_eq!(
            subscription.next_event().await.unwrap(),
            SubscriptionEvent::Event(json!({"type": "heartbeat"}))
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
        let listener = UnixListener::bind(&path).unwrap();
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let (mut reader, _id, _params) = read_subscribe_line(stream).await;
            reader
                .write_all(b"{\"id\":\"different\",\"result\":{}}\n")
                .await
                .unwrap();
        });
        let client = HerdrSocketClient::new(&path);
        let error = EventSubscription::connect(&client, config())
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
        let listener = UnixListener::bind(&path).unwrap();
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let (mut reader, id, _params) = read_subscribe_line(stream).await;
            reader
                .write_all(format!("{{\"id\":\"{id}\",\"result\":{{}}}}\n").as_bytes())
                .await
                .unwrap();
        });
        let client = HerdrSocketClient::new(&path);
        let (mut subscription, _) = EventSubscription::connect(&client, config()).await.unwrap();
        server.await.unwrap();
        let error = subscription.next_event().await.unwrap_err();
        assert!(matches!(error, SocketError::EarlyEof { .. }));
        assert_eq!(error.delivery(), DeliveryState::MayHaveReachedHost);
    }

    #[tokio::test]
    async fn silence_deadline_reports_unknown_outcome_not_detach() {
        tokio::time::pause();
        let temp = TempDir::new().unwrap();
        let path = temp.path().join("herdr.sock");
        let listener = UnixListener::bind(&path).unwrap();
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let (mut reader, id, _params) = read_subscribe_line(stream).await;
            reader
                .write_all(format!("{{\"id\":\"{id}\",\"result\":{{}}}}\n").as_bytes())
                .await
                .unwrap();
            tokio::time::sleep(Duration::from_hours(1)).await;
        });
        let client = HerdrSocketClient::new(&path);
        let short = SubscriptionConfig {
            max_silence: Duration::from_millis(50),
            ..config()
        };
        let (mut subscription, _) = EventSubscription::connect(&client, short).await.unwrap();
        let waiting = tokio::spawn(async move { subscription.next_event().await });
        tokio::task::yield_now().await;
        tokio::time::advance(Duration::from_millis(200)).await;
        let error = waiting.await.unwrap().unwrap_err();
        server.abort();
        assert!(matches!(error, SocketError::Timeout { .. }));
        assert_eq!(error.delivery(), DeliveryState::MayHaveReachedHost);
    }

    #[tokio::test]
    async fn reconnect_returns_last_error_after_bounded_grace() {
        tokio::time::pause();
        let temp = TempDir::new().unwrap();
        let path = temp.path().join("missing.sock");
        let client = HerdrSocketClient::new(&path);
        let started = Instant::now();
        let waiting = tokio::spawn(async move {
            EventSubscription::reconnect_with_grace(
                &client,
                config(),
                Duration::from_secs(10),
                Duration::from_secs(1),
            )
            .await
        });
        tokio::task::yield_now().await;
        tokio::time::advance(Duration::from_secs(11)).await;
        let error = waiting.await.unwrap().unwrap_err();
        assert!(started.elapsed() >= Duration::from_secs(10));
        assert_eq!(error.delivery(), DeliveryState::NotSent);
    }

    #[tokio::test]
    async fn reconnect_resubscribes_after_server_replacement() {
        tokio::time::pause();
        let temp = TempDir::new().unwrap();
        let path = temp.path().join("herdr.sock");
        let listener = UnixListener::bind(&path).unwrap();
        let server = tokio::spawn(async move {
            // First incarnation: accept the subscribe, then vanish without answering.
            let (stream, _) = listener.accept().await.unwrap();
            let (_reader, _id, _params) = read_subscribe_line(stream).await;
            // Second incarnation on a fresh connection: complete the retained request.
            let (stream, _) = listener.accept().await.unwrap();
            let (mut reader, id, params) = read_subscribe_line(stream).await;
            assert_eq!(params["subscriptions"][0]["type"], "tab.focused");
            reader
                .write_all(
                    format!("{{\"id\":\"{id}\",\"result\":{{\"subscribed\":true}}}}\n").as_bytes(),
                )
                .await
                .unwrap();
            tokio::time::sleep(Duration::from_hours(1)).await;
        });
        let client = HerdrSocketClient::new(&path);
        let mut retry_config = config();
        retry_config.subscribe_timeout = Duration::from_millis(200);
        let waiting = tokio::spawn(async move {
            EventSubscription::reconnect_with_grace(
                &client,
                retry_config,
                Duration::from_secs(5),
                Duration::from_millis(100),
            )
            .await
        });
        for _ in 0..100 {
            tokio::task::yield_now().await;
            tokio::time::advance(Duration::from_millis(100)).await;
            if waiting.is_finished() {
                break;
            }
        }
        let (subscription, result) = waiting.await.unwrap().unwrap();
        server.abort();
        assert_eq!(result, json!({"subscribed": true}));
        assert_eq!(
            subscription.params(),
            &json!({"subscriptions": [{"type": "tab.focused"}]})
        );
    }
}
