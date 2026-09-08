# Changelog

## Unreleased

- Fix Zellij bridge permission replay and client census handling so grants are anchored to the receiving plugin client and foreign client records cannot authorize registration.
- Fix simultaneous Zellij client routing by requiring each origin response to match both its client ID and registration.
