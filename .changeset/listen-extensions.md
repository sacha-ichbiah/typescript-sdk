---
'@modelcontextprotocol/server': minor
---

`createMcpHandler` and `serveStdio` take `listenExtensions`, so an extension can add its own fields to the `subscriptions/listen` filter, such as the Tasks extension's `taskIds`, and have its notifications delivered on listen streams. A `ListenExtension` names the notification methods it owns, the filter fields the server acknowledges, and which of its notifications reach a stream. `ServerEvent` gains an `extension` variant for publishing those notifications on the bus. Without `listenExtensions`, listen streams behave as before.
