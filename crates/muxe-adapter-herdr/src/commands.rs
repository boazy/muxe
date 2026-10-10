//! Read-only inactive client endpoint discovery; mutations use the guarded JSON API.
use std::{future::Future, io, time::Duration};

use muxe_adapter_api::{AdapterError, AdapterErrorKind};
use muxe_core::OriginContext;
use serde::{Deserialize, Deserializer};
use serde_json::Value;
#[cfg(test)]
use serde_json::json;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::UnixStream,
    sync::watch,
    time::Instant,
};

use crate::{
    DeliveryState, SocketError,
    transport::{CommandEndpointContinuity, EndpointContinuityToken, HerdrSocketClient},
};

const MAX_FRAME_BYTES: usize = 2 * 1024 * 1024;
const MAX_SELECTOR_BYTES: usize = 1024;
const MAX_ID_BYTES: usize = 4096;
const LOOKUP_TIMEOUT: Duration = Duration::from_secs(10);
const CONTINUITY_INTERVAL: Duration = Duration::from_millis(25);
const HELLO: &str = r#"{"generation":1,"cell_width_px":0,"cell_height_px":0,"surface_size":{"cols":1,"rows":1},"pixel_mouse":false,"direct_graphics":false,"endpoint_keybindings":false,"mouse_capture":false,"surface_active":false,"surface_reuse":false,"surface_delta":false,"surface_scroll":false,"snapshot_codecs":["shell.snapshot.v1"],"surface_codecs":["shell.surface.v1"],"input_codecs":["shell.input.semantic.v1"],"blob_codecs":["shell.blob.v1"]}"#;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct CommandBinding(String);
impl CommandBinding {
    #[expect(clippy::result_large_err, reason = "shared adapter error boundary")]
    pub(crate) fn parse(value: &str) -> Result<Self, AdapterError> {
        if !valid_text(value, MAX_SELECTOR_BYTES) {
            return Err(AdapterError::new(
                AdapterErrorKind::InvalidRequest,
                "Herdr command binding must be nonempty, contain no control characters, and fit within 1024 bytes.",
            ));
        }
        Ok(Self(value.to_owned()))
    }
    pub(crate) fn as_str(&self) -> &str {
        &self.0
    }
}
impl<'de> Deserialize<'de> for CommandBinding {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let value = String::deserialize(deserializer)?;
        if valid_text(&value, MAX_SELECTOR_BYTES) {
            Ok(Self(value))
        } else {
            Err(serde::de::Error::custom("invalid command binding label"))
        }
    }
}
fn valid_text(value: &str, maximum: usize) -> bool {
    !value.is_empty() && value.len() <= maximum && !value.chars().any(char::is_control)
}

#[derive(Eq, PartialEq, Deserialize)]
#[serde(try_from = "String")]
struct CommandId(String);
impl TryFrom<String> for CommandId {
    type Error = &'static str;
    fn try_from(value: String) -> Result<Self, Self::Error> {
        if valid_text(&value, MAX_ID_BYTES) {
            Ok(Self(value))
        } else {
            Err("invalid command identifier")
        }
    }
}
#[derive(Deserialize)]
#[serde(try_from = "String")]
struct EndpointBootId {
    _value: String,
}
impl TryFrom<String> for EndpointBootId {
    type Error = &'static str;
    fn try_from(value: String) -> Result<Self, Self::Error> {
        if valid_text(&value, MAX_ID_BYTES) {
            Ok(Self { _value: value })
        } else {
            Err("invalid endpoint boot identifier")
        }
    }
}
#[derive(Deserialize)]
struct Welcome {
    generation: u32,
    snapshot_codec: String,
    surface_codec: String,
    input_codec: String,
    blob_codec: String,
    #[serde(default)]
    methods: Vec<String>,
    error: Option<RemoteError>,
}
#[derive(Deserialize)]
struct RemoteError {
    code: String,
    message: String,
}
#[derive(Deserialize)]
struct Snapshot {
    #[serde(rename = "boot_id")]
    _boot_id: EndpointBootId,
    #[serde(rename = "revision")]
    _revision: u64,
    commands: Vec<ManifestCommand>,
}
#[derive(Deserialize)]
struct ManifestCommand {
    command_id: CommandId,
    #[serde(default)]
    binding_label: Option<CommandBinding>,
    #[serde(default)]
    binding_labels: Option<Vec<CommandBinding>>,
    action: CommandAction,
}
#[derive(Clone, Copy, Deserialize, Eq, PartialEq)]
enum CommandAction {
    Shell,
    Pane,
    Popup,
    PluginAction,
    #[serde(other)]
    Unknown,
}

impl Snapshot {
    fn resolve(self, binding: &CommandBinding) -> Result<CommandId, SocketError> {
        // Both fields are validated at the boundary; the ID namespace belongs to
        // this boot/revision and is never cached across dispatches.
        let mut selected: Option<ManifestCommand> = None;
        for command in self.commands {
            let matches = match &command.binding_labels {
                Some(labels) => labels.iter().any(|label| label == binding),
                None => command.binding_label.as_ref() == Some(binding),
            };
            if !matches {
                continue;
            }
            if let Some(previous) = &selected
                && (previous.command_id != command.command_id || previous.action != command.action)
            {
                return Err(protocol(
                    "Herdr command binding is ambiguous in the current manifest",
                ));
            }
            selected = Some(command);
        }
        let command = selected.ok_or_else(|| {
            protocol(format!(
                "Herdr command binding {:?} is missing from the current manifest",
                binding.as_str()
            ))
        })?;
        if command.action == CommandAction::Unknown {
            return Err(protocol(
                "Herdr command binding has an unsupported manifest action",
            ));
        }
        Ok(command.command_id)
    }
}

#[expect(clippy::result_large_err, reason = "shared adapter error boundary")]
pub(crate) fn validate_origin(origin: &OriginContext) -> Result<(), AdapterError> {
    if origin
        .pane_id
        .as_ref()
        .is_none_or(|id| !valid_text(id.as_str(), MAX_ID_BYTES))
        || origin
            .tab_id
            .as_ref()
            .is_none_or(|id| !valid_text(id.as_str(), MAX_ID_BYTES))
        || origin
            .workspace_id
            .as_ref()
            .is_none_or(|id| !valid_text(id.as_str(), MAX_ID_BYTES))
    {
        return Err(AdapterError::new(
            AdapterErrorKind::ContextUnavailable,
            "Invoking a configured Herdr command requires the captured pane, tab, and workspace IDs.",
        ));
    }
    Ok(())
}

pub(crate) struct CommandManifestAuthority {
    continuity: CommandEndpointContinuity,
    // Keep the read-only inactive channel alive through the JSON operation.
    _stream: UnixStream,
}
impl CommandManifestAuthority {
    pub(crate) fn continuity(&self) -> &CommandEndpointContinuity {
        &self.continuity
    }
}
pub(crate) struct ResolvedCommand {
    command_id: CommandId,
    authority: CommandManifestAuthority,
}
impl ResolvedCommand {
    pub(crate) fn into_request(self, origin: &OriginContext) -> (Value, CommandManifestAuthority) {
        let mut params = serde_json::Map::new();
        params.insert("command_id".to_owned(), Value::String(self.command_id.0));
        for (field, id) in [
            (
                "workspace_id",
                origin
                    .workspace_id
                    .as_ref()
                    .map(muxe_core::WorkspaceId::as_str),
            ),
            (
                "tab_id",
                origin.tab_id.as_ref().map(muxe_core::TabId::as_str),
            ),
            (
                "pane_id",
                origin.pane_id.as_ref().map(muxe_core::PaneId::as_str),
            ),
        ] {
            params.insert(
                field.to_owned(),
                id.map_or(Value::Null, |id| Value::String(id.to_owned())),
            );
        }
        (Value::Object(params), self.authority)
    }
}

pub(crate) async fn resolve(
    client: &HerdrSocketClient,
    expected: &EndpointContinuityToken,
    binding: &CommandBinding,
    mut retirement: watch::Receiver<bool>,
) -> Result<ResolvedCommand, SocketError> {
    let deadline = Instant::now() + LOOKUP_TIMEOUT;
    let (mut stream, continuity) = guarded_wait(
        client.connect_command_endpoint(expected),
        &mut retirement,
        expected,
        client,
        None,
        deadline,
    )
    .await?;
    let mut buffer = Vec::new();
    encode_control(&mut buffer, "endpoint.hello.v1", HELLO)?;
    guarded_wait(
        async {
            stream
                .write_all(&buffer)
                .await
                .map_err(|source| SocketError::Write {
                    delivery: DeliveryState::NotSent,
                    source,
                })
        },
        &mut retirement,
        expected,
        client,
        Some(&continuity),
        deadline,
    )
    .await?;
    let mut welcome_received = false;
    let command_id = loop {
        guarded_wait(
            read_frame(&mut stream, &mut buffer),
            &mut retirement,
            expected,
            client,
            Some(&continuity),
            deadline,
        )
        .await?;
        match decode_frame(&buffer)? {
            Frame::Control {
                kind: "endpoint.welcome.v1",
                data,
            } => {
                if welcome_received {
                    return Err(protocol("duplicate Herdr endpoint welcome"));
                }
                let welcome: Welcome = decode_json(data.as_bytes())?;
                if let Some(error) = welcome.error {
                    return Err(protocol(format!(
                        "Herdr endpoint rejected discovery ({}): {}",
                        error.code, error.message
                    )));
                }
                if welcome.generation != 1
                    || welcome.snapshot_codec != "shell.snapshot.v1"
                    || welcome.surface_codec != "shell.surface.v1"
                    || welcome.input_codec != "shell.input.semantic.v1"
                    || welcome.blob_codec != "shell.blob.v1"
                    || !welcome
                        .methods
                        .iter()
                        .any(|method| method == "command.invoke")
                {
                    return Err(protocol(
                        "Herdr endpoint does not advertise generation-one command.invoke and the required codecs",
                    ));
                }
                welcome_received = true;
            }
            Frame::Control {
                kind: "shell.snapshot.v1",
                data,
            } => {
                if !welcome_received {
                    return Err(protocol("Herdr endpoint snapshot arrived before welcome"));
                }
                break decode_json::<Snapshot>(data.as_bytes())?.resolve(binding)?;
            }
            Frame::Shutdown { reason } => {
                return Err(protocol(format!(
                    "Herdr endpoint shut down: {}",
                    reason.unwrap_or("no reason supplied")
                )));
            }
            Frame::Control { .. } | Frame::Other => {}
        }
    };
    expected.verify_socket_file(client.socket())?;
    continuity.verify_socket_file()?;
    Ok(ResolvedCommand {
        command_id,
        authority: CommandManifestAuthority {
            continuity,
            _stream: stream,
        },
    })
}

async fn guarded_wait<T>(
    future: impl Future<Output = Result<T, SocketError>>,
    retirement: &mut watch::Receiver<bool>,
    expected: &EndpointContinuityToken,
    client: &HerdrSocketClient,
    binary: Option<&CommandEndpointContinuity>,
    deadline: Instant,
) -> Result<T, SocketError> {
    if *retirement.borrow() {
        return Err(SocketError::RuntimeRetired {
            delivery: DeliveryState::NotSent,
        });
    }
    verify_endpoint_files(expected, client, binary)?;
    let mut continuity = tokio::time::interval(CONTINUITY_INTERVAL);
    tokio::pin!(future);
    loop {
        tokio::select! {
            biased;
            _ = retirement.changed() => return Err(SocketError::RuntimeRetired { delivery: DeliveryState::NotSent }),
            () = tokio::time::sleep_until(deadline) => return Err(SocketError::Timeout { delivery: DeliveryState::NotSent }),
            _instant = continuity.tick() => verify_endpoint_files(expected, client, binary)?,
            result = &mut future => return result,
        }
    }
}
fn verify_endpoint_files(
    expected: &EndpointContinuityToken,
    client: &HerdrSocketClient,
    binary: Option<&CommandEndpointContinuity>,
) -> Result<(), SocketError> {
    expected.verify_socket_file(client.socket())?;
    if let Some(binary) = binary {
        binary.verify_socket_file()?;
    }
    Ok(())
}
async fn read_frame(stream: &mut UnixStream, buffer: &mut Vec<u8>) -> Result<(), SocketError> {
    let mut header = [0; 4];
    stream.read_exact(&mut header).await.map_err(read_error)?;
    let size = u32::from_le_bytes(header) as usize;
    if size == 0 {
        return Err(protocol("empty Herdr endpoint envelope"));
    }
    if size > MAX_FRAME_BYTES {
        return Err(SocketError::ResponseTooLarge {
            delivery: DeliveryState::NotSent,
        });
    }
    buffer.resize(size, 0);
    stream.read_exact(buffer).await.map_err(read_error)?;
    Ok(())
}
fn read_error(source: io::Error) -> SocketError {
    if source.kind() == io::ErrorKind::UnexpectedEof {
        SocketError::EarlyEof {
            delivery: DeliveryState::NotSent,
        }
    } else {
        SocketError::Read {
            delivery: DeliveryState::NotSent,
            source,
        }
    }
}
fn decode_json<T: serde::de::DeserializeOwned>(bytes: &[u8]) -> Result<T, SocketError> {
    serde_json::from_slice(bytes).map_err(|source| SocketError::InvalidJson {
        delivery: DeliveryState::NotSent,
        source,
    })
}
fn protocol(message: impl Into<String>) -> SocketError {
    SocketError::Protocol {
        delivery: DeliveryState::NotSent,
        message: message.into(),
    }
}
enum Frame<'a> {
    Control { kind: &'a str, data: &'a str },
    Shutdown { reason: Option<&'a str> },
    Other,
}
struct Cursor<'a> {
    bytes: &'a [u8],
}
impl<'a> Cursor<'a> {
    fn take(&mut self, length: usize) -> Result<&'a [u8], SocketError> {
        if length > self.bytes.len() {
            return Err(protocol("truncated Herdr endpoint envelope"));
        }
        let (value, rest) = self.bytes.split_at(length);
        self.bytes = rest;
        Ok(value)
    }
    fn integer(&mut self) -> Result<u64, SocketError> {
        let tag = self.take(1)?[0];
        let (width, minimum) = match tag {
            0..=250 => return Ok(u64::from(tag)),
            251 => (2, 251),
            252 => (4, 65_536),
            253 => (8, 4_294_967_296),
            _ => return Err(protocol("invalid Herdr endpoint varint tag")),
        };
        let bytes = self.take(width)?;
        let mut raw = [0; 8];
        raw[..width].copy_from_slice(bytes);
        let value = u64::from_le_bytes(raw);
        if value < minimum {
            return Err(protocol("noncanonical Herdr endpoint varint"));
        }
        Ok(value)
    }
    fn string(&mut self) -> Result<&'a str, SocketError> {
        let size = usize::try_from(self.integer()?)
            .map_err(|_| protocol("Herdr endpoint length overflow"))?;
        std::str::from_utf8(self.take(size)?)
            .map_err(|_| protocol("invalid UTF-8 in Herdr endpoint envelope"))
    }
    fn finish(&self) -> Result<(), SocketError> {
        if self.bytes.is_empty() {
            Ok(())
        } else {
            Err(protocol("trailing bytes in Herdr endpoint envelope"))
        }
    }
}
fn decode_frame(bytes: &[u8]) -> Result<Frame<'_>, SocketError> {
    if bytes.len() > MAX_FRAME_BYTES {
        return Err(protocol("Herdr endpoint envelope exceeds frame limit"));
    }
    let mut cursor = Cursor { bytes };
    let tag = u32::try_from(cursor.integer()?)
        .map_err(|_| protocol("Herdr endpoint variant tag overflow"))?;
    let frame = match tag {
        3 => {
            let reason = match cursor.take(1)?[0] {
                0 => None,
                1 => Some(cursor.string()?),
                _ => return Err(protocol("invalid Herdr endpoint shutdown option")),
            };
            Frame::Shutdown { reason }
        }
        20 => Frame::Control {
            kind: cursor.string()?,
            data: cursor.string()?,
        },
        _ => return Ok(Frame::Other),
    };
    cursor.finish()?;
    Ok(frame)
}
fn encode_integer(buffer: &mut Vec<u8>, value: u64) {
    if value <= 250 {
        buffer.push(u8::try_from(value).expect("a single-byte varint is at most 250"));
    } else if let Ok(value) = u16::try_from(value) {
        buffer.push(251);
        buffer.extend_from_slice(&value.to_le_bytes());
    } else if let Ok(value) = u32::try_from(value) {
        buffer.push(252);
        buffer.extend_from_slice(&value.to_le_bytes());
    } else {
        buffer.push(253);
        buffer.extend_from_slice(&value.to_le_bytes());
    }
}
fn encode_string(buffer: &mut Vec<u8>, value: &str) {
    encode_integer(buffer, value.len() as u64);
    buffer.extend_from_slice(value.as_bytes());
}
fn encode_control(buffer: &mut Vec<u8>, kind: &str, data: &str) -> Result<(), SocketError> {
    if kind
        .len()
        .checked_add(data.len())
        .is_none_or(|size| size > MAX_FRAME_BYTES - 19)
    {
        return Err(SocketError::RequestTooLarge);
    }
    buffer.clear();
    buffer.extend_from_slice(&[0; 4]);
    buffer.push(20);
    encode_string(buffer, kind);
    encode_string(buffer, data);
    let length = u32::try_from(buffer.len() - 4).map_err(|_| SocketError::RequestTooLarge)?;
    buffer[..4].copy_from_slice(&length.to_le_bytes());
    Ok(())
}

#[cfg(test)]
pub(crate) mod fixture {
    use super::*;
    pub(crate) fn schema_json() -> Value {
        let mut schema: Value = serde_json::from_str(include_str!(
            "../../../fixtures/herdr/herdr-api.schema.json"
        ))
        .unwrap();
        let request = &mut schema["schemas"]["request"];
        // Recorded installed 0.9.3 request validation surface; descriptions omitted.
        request["$defs"]["CommandInvokeParams"] = json!({
        "type":"object","required":["command_id"],"properties":{
            "command_id":{"type":"string"},"pane_id":{"type":["string","null"]},
            "tab_id":{"type":["string","null"]},"workspace_id":{"type":["string","null"]},
            "selection":{"anyOf":[{"$ref":"#/schemas/request/$defs/PaneSelectionReadParams"},{"type":"null"}]}
        }});
        request["$defs"]["PaneSelectionReadParams"] = json!({
        "type":"object","required":["pane_id","anchor","cursor"],"properties":{
            "pane_id":{"type":"string"},"anchor":{"$ref":"#/schemas/request/$defs/PaneTextPoint"},
            "cursor":{"$ref":"#/schemas/request/$defs/PaneTextPoint"},
            "content_revision":{"type":["integer","null"],"format":"uint64","minimum":0}
        }});
        request["$defs"]["PaneTextPoint"] = json!({
        "type":"object","required":["row","col"],"properties":{
            "row":{"type":"integer","format":"uint32","minimum":0},
            "col":{"type":"integer","format":"uint16","minimum":0,"maximum":65535}
        }});
        request["oneOf"].as_array_mut().unwrap().push(json!({
        "type":"object","required":["method","params"],"properties":{
            "method":{"type":"string","const":"command.invoke"},
            "params":{"$ref":"#/schemas/request/$defs/CommandInvokeParams"}
        }}));
        schema
    }
    pub(crate) fn schema() -> crate::ApiSchema {
        crate::ApiSchema::parse(schema_json()).unwrap()
    }
    pub(crate) async fn accept_inactive_hello(stream: &mut UnixStream) {
        let mut bytes = Vec::new();
        read_frame(stream, &mut bytes).await.unwrap();
        let Frame::Control { kind, data } = decode_frame(&bytes).unwrap() else {
            panic!("hello envelope");
        };
        assert_eq!(kind, "endpoint.hello.v1");
        let hello: Value = serde_json::from_str(data).unwrap();
        for key in [
            "surface_active",
            "mouse_capture",
            "endpoint_keybindings",
            "direct_graphics",
            "pixel_mouse",
        ] {
            assert_eq!(hello[key], false);
        }
        assert_eq!(hello["cell_width_px"], 0);
        assert_eq!(hello["cell_height_px"], 0);
        assert_eq!(hello["surface_size"], json!({"cols":1,"rows":1}));
    }
    pub(crate) async fn write_control(stream: &mut UnixStream, kind: &str, data: &Value) {
        let mut bytes = Vec::new();
        encode_control(&mut bytes, kind, &data.to_string()).unwrap();
        stream.write_all(&bytes).await.unwrap();
    }
    pub(crate) async fn read_request(stream: &mut UnixStream) -> Value {
        let mut bytes = Vec::new();
        loop {
            let byte = stream.read_u8().await.unwrap();
            if byte == b'\n' {
                break;
            }
            assert!(bytes.len() < MAX_FRAME_BYTES);
            bytes.push(byte);
        }
        serde_json::from_slice(&bytes).unwrap()
    }
    pub(crate) async fn write_response(stream: &mut UnixStream, response: &Value) {
        stream
            .write_all(format!("{response}\n").as_bytes())
            .await
            .unwrap();
        stream.shutdown().await.unwrap();
    }
    pub(crate) fn welcome() -> Value {
        json!({"generation":1,"snapshot_codec":"shell.snapshot.v1","surface_codec":"shell.surface.v1",
            "input_codec":"shell.input.semantic.v1","blob_codec":"shell.blob.v1","methods":["command.invoke"]})
    }
    pub(crate) fn snapshot(id: &str) -> Value {
        json!({"boot_id":"owned-boot","revision":1,"commands":[{"command_id":id,
            "binding_labels":["prefix+shift+u"],"binding_label":"legacy-label","action":"Popup",
            "description":"Usage","command":"must-not-be-forwarded-secret"}],"focused_pane_id":"wrong-focus"})
    }
    pub(crate) fn origin() -> OriginContext {
        OriginContext {
            host_kind: muxe_core::OriginHostKind::Herdr,
            server_id: muxe_core::ServerId::new("owned-server"),
            client_id: None,
            session_id: None,
            workspace_id: Some(muxe_core::WorkspaceId::new("captured-workspace")),
            tab_id: Some(muxe_core::TabId::new("captured-tab")),
            pane_id: Some(muxe_core::PaneId::new("captured-pane")),
            tab_index: None,
            pane_type: None,
            pane_cwd: None,
            selection_text: None,
            invocation_source: muxe_core::OriginInvocationSource::RootBinding,
            worktree_id: None,
            worktree_path: None,
            agent_id: None,
            link_url: None,
            link_handler_id: None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::net::UnixListener;
    #[test]
    fn hostile_readonly_envelopes_fail_closed() {
        for bytes in [
            vec![],
            vec![254],
            vec![255],
            vec![251, 20, 0],
            vec![20, 251, 1, 0, b'x', 0],
            vec![20, 1, 255, 0],
            vec![20, 0, 253, 255, 255, 255, 255, 255, 255, 255, 255],
            vec![20, 0, 0, 0],
            vec![3],
            vec![3, 2],
            vec![3, 0, 0],
        ] {
            assert!(decode_frame(&bytes).is_err());
        }
        assert!(matches!(
            decode_frame(&[3, 0]).unwrap(),
            Frame::Shutdown { reason: None }
        ));
        assert!(matches!(
            decode_frame(&[3, 1, 1, b'x']).unwrap(),
            Frame::Shutdown { reason: Some("x") }
        ));
        assert!(matches!(decode_frame(&[19, 255]).unwrap(), Frame::Other));
    }
    #[test]
    fn exact_selection_rejects_missing_ambiguous_unknown_and_invalid_ids() {
        let binding = CommandBinding::parse("prefix+shift+u").unwrap();
        let mut value = fixture::snapshot("current");
        let snapshot: Snapshot = serde_json::from_value(value.clone()).unwrap();
        assert!(snapshot.resolve(&binding).is_ok());
        value["commands"][0]["binding_labels"] = json!([]);
        value["commands"][0]["binding_label"] = json!("prefix+shift+u");
        assert!(
            decode_json::<Snapshot>(value.to_string().as_bytes())
                .unwrap()
                .resolve(&binding)
                .is_err()
        );
        value["commands"][0]
            .as_object_mut()
            .unwrap()
            .remove("binding_labels");
        assert!(
            decode_json::<Snapshot>(value.to_string().as_bytes())
                .unwrap()
                .resolve(&binding)
                .is_ok()
        );
        value["commands"][0]["action"] = json!("FutureAction");
        assert!(
            decode_json::<Snapshot>(value.to_string().as_bytes())
                .unwrap()
                .resolve(&binding)
                .is_err()
        );
        let mut value = fixture::snapshot("first");
        let mut other = value["commands"][0].clone();
        other["command_id"] = json!("second");
        value["commands"].as_array_mut().unwrap().push(other);
        assert!(
            decode_json::<Snapshot>(value.to_string().as_bytes())
                .unwrap()
                .resolve(&binding)
                .is_err()
        );
        value["boot_id"] = json!("");
        assert!(decode_json::<Snapshot>(value.to_string().as_bytes()).is_err());
        for value in ["", "prefix\nu"] {
            assert!(CommandBinding::parse(value).is_err());
        }
        assert!(CommandBinding::parse(&"x".repeat(MAX_SELECTOR_BYTES + 1)).is_err());
    }
    #[tokio::test]
    async fn either_socket_rebind_during_readonly_lookup_sends_no_rpc() {
        for binary_rebind in [false, true] {
            let temp = tempfile::TempDir::new().unwrap();
            let client = HerdrSocketClient::new(temp.path().join("herdr.sock"));
            let api = UnixListener::bind(client.socket()).unwrap();
            let binary = UnixListener::bind(client.command_socket().unwrap()).unwrap();
            let token = client.observed_token().await.unwrap();
            drop(api.accept().await.unwrap());
            let path = if binary_rebind {
                client.command_socket().unwrap()
            } else {
                client.socket().to_path_buf()
            };
            let server = tokio::spawn(async move {
                let (mut stream, _) = binary.accept().await.unwrap();
                fixture::accept_inactive_hello(&mut stream).await;
                fixture::write_control(&mut stream, "endpoint.welcome.v1", &fixture::welcome())
                    .await;
                std::fs::remove_file(&path).unwrap();
                let _replacement = UnixListener::bind(path).unwrap();
                let mut bytes = Vec::new();
                stream.read_to_end(&mut bytes).await.unwrap();
                assert!(bytes.is_empty());
            });
            let (retirement, _) = watch::channel(false);
            let failure = resolve(
                &client,
                &token,
                &CommandBinding::parse("prefix+shift+u").unwrap(),
                retirement.subscribe(),
            )
            .await
            .err()
            .unwrap();
            assert_eq!(failure.delivery(), DeliveryState::NotSent);
            assert!(matches!(failure, SocketError::EndpointReplaced { .. }));
            server.await.unwrap();
        }
    }

    #[tokio::test]
    async fn invalid_or_unsupported_manifests_never_open_a_mutating_api_connection() {
        for scenario in [
            "missing",
            "ambiguous",
            "unknown",
            "invalid-id",
            "oversize",
            "unsupported-welcome",
        ] {
            let temp = tempfile::TempDir::new().unwrap();
            let client = HerdrSocketClient::new(temp.path().join("herdr.sock"));
            let api = UnixListener::bind(client.socket()).unwrap();
            let binary = UnixListener::bind(client.command_socket().unwrap()).unwrap();
            let token = client.observed_token().await.unwrap();
            drop(api.accept().await.unwrap());
            let server = tokio::spawn(async move {
                let (mut stream, _) = binary.accept().await.unwrap();
                fixture::accept_inactive_hello(&mut stream).await;
                if scenario == "oversize" {
                    stream
                        .write_u32_le(u32::try_from(MAX_FRAME_BYTES + 1).unwrap())
                        .await
                        .unwrap();
                } else {
                    let mut welcome = fixture::welcome();
                    if scenario == "unsupported-welcome" {
                        welcome["methods"] = json!([]);
                    }
                    fixture::write_control(&mut stream, "endpoint.welcome.v1", &welcome).await;
                    if scenario != "unsupported-welcome" {
                        let mut snapshot = fixture::snapshot("opaque");
                        match scenario {
                            "missing" => snapshot["commands"] = json!([]),
                            "ambiguous" => {
                                let mut other = snapshot["commands"][0].clone();
                                other["command_id"] = json!("distinct");
                                snapshot["commands"].as_array_mut().unwrap().push(other);
                            }
                            "unknown" => snapshot["commands"][0]["action"] = json!("FutureAction"),
                            "invalid-id" => snapshot["commands"][0]["command_id"] = json!(""),
                            _ => {}
                        }
                        fixture::write_control(&mut stream, "shell.snapshot.v1", &snapshot).await;
                    }
                }
                let mut bytes = Vec::new();
                stream.read_to_end(&mut bytes).await.unwrap();
                assert!(bytes.is_empty());
            });
            let (retirement, _) = watch::channel(false);
            let binding = CommandBinding::parse("prefix+shift+u").unwrap();
            let failure = tokio::select! {
                biased;
                _ = api.accept() => panic!("invalid manifest opened a JSON API request"),
                result = resolve(&client, &token, &binding, retirement.subscribe()) =>
                    result.err().expect("invalid manifest must reject lookup"),
            };
            assert_eq!(failure.delivery(), DeliveryState::NotSent);
            server.await.unwrap();
        }
    }

    #[tokio::test]
    async fn read_only_deadline_does_not_claim_a_command_was_sent() {
        let temp = tempfile::TempDir::new().unwrap();
        let client = HerdrSocketClient::new(temp.path().join("herdr.sock"));
        let api = UnixListener::bind(client.socket()).unwrap();
        let expected = client.observed_token().await.unwrap();
        drop(api.accept().await.unwrap());
        let (_retirement, mut receiver) = watch::channel(false);
        let failure = guarded_wait(
            std::future::pending::<Result<(), SocketError>>(),
            &mut receiver,
            &expected,
            &client,
            None,
            Instant::now(),
        )
        .await
        .err()
        .unwrap();
        assert_eq!(failure.delivery(), DeliveryState::NotSent);
        assert!(matches!(failure, SocketError::Timeout { .. }));
    }
}
