# Security

Do not post passwords, connection strings containing secrets, private exports, captured
source data or unredacted incident bundles in public issues.

Use GitHub private vulnerability reporting if it is enabled for this repository. Otherwise,
ask a maintainer for a private reporting channel without including exploit or private data
in the public request. Include the application version, Windows/Python/driver versions and a
minimal synthetic reproduction once a private channel is available.

Use a dedicated SQL account with the permissions required for the selected workflow.
Keep TLS certificate validation enabled outside explicitly disposable test environments.
Local captures, journals and failure artifacts contain source data and require appropriate
filesystem permissions and retention. See [data locality](docs/data-locality.md).
