# Changelog

## Unreleased

- Preserve line-delimited Zellij event frames and keep subscribed CLI pipes held for their lifetime so event streams are not truncated or released prematurely.
- Subscribe to cached permission results and confirm the active bridge client from `PluginIds.client_id` census data before registration.
- Share the full 11-permission fixture grant contract between the protocol and live runner.
- Fix fixture activation to invoke the public `muxe activate` subcommand.
- Add two-client target-only read-only routing coverage and owned failure-log capture.
