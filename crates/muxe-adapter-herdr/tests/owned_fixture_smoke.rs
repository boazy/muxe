#[path = "support/fixture.rs"]
mod fixture;

use std::{io::Write, path::PathBuf};

use fixture::OwnedHerdrFixture;
use muxe_adapter_herdr::{HerdrSocketClient, probe_live_identity};

/// This intentionally runs only when an operator supplies the exact Herdr binary to spawn. The
/// fixture clears its environment, creates a fresh temporary socket, and retains the one Child it
/// starts; it does not inspect or contact any ambient Herdr server.
#[tokio::test]
#[ignore = "requires explicit MUXE_HERDR_TEST_BINARY approval for an owned live Herdr server"]
async fn ping_uses_only_the_owned_server() {
    let binary = PathBuf::from(
        std::env::var_os("MUXE_HERDR_TEST_BINARY")
            .expect("set MUXE_HERDR_TEST_BINARY to the absolute Herdr binary to run this smoke"),
    );
    let mut fixture = match OwnedHerdrFixture::start(binary).await {
        Ok(fixture) => fixture,
        Err(error) => panic!("could not start the owned Herdr fixture: {error}"),
    };
    let pid = fixture
        .owned_child_pid()
        .expect("the retained owned Herdr child must have a PID before cleanup");
    let socket = fixture.socket().to_owned();
    let initial = fixture.start_identity().clone();
    let _ = writeln!(
        std::io::stderr(),
        "owned Herdr start: pid={pid}; socket={}; identity={initial:?}",
        socket.display()
    );
    let observed = probe_live_identity(&HerdrSocketClient::new(&socket)).await;
    let cleanup = fixture.terminate_and_reap().await;

    match (observed, cleanup) {
        (Ok(observed), Ok(cleanup)) => {
            assert_eq!(observed, initial);
            assert!(
                !fixture.has_owned_child(),
                "the exact retained owned Herdr child must be reaped before fixture teardown"
            );
            let _ = writeln!(
                std::io::stderr(),
                "owned Herdr ping: observed={observed:?}; cleanup={:?}; retained_child_reaped=true",
                cleanup.disposition
            );
        }
        (Err(probe), Ok(cleanup)) => {
            panic!(
                "owned server stopped answering its fixture socket: {probe}; cleanup={:?}; {}",
                cleanup.disposition, cleanup.diagnostics
            );
        }
        (Ok(_), Err(cleanup)) => {
            panic!("owned Herdr child did not terminate and reap: {cleanup}");
        }
        (Err(probe), Err(cleanup)) => {
            panic!(
                "owned server stopped answering its fixture socket ({probe}) and cleanup failed: {cleanup}"
            );
        }
    }
}
