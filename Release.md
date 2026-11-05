## Features

* UDP packet payloads for ordinary UDP proxies and SUDP now use a dedicated binary codec when frpc and frps successfully negotiate the capability under wire protocol v2, using a more compact wire representation. Wire protocol v1 remains JSON; wire protocol v2 falls back to JSON `UDPPacket` when the peer does not support or did not negotiate the capability.

## Fixes

* Client drain now preserves already assigned work until it closes or the drain deadline expires. New work after local withdrawal is rejected. Losing the control, explicit close, proxy removal, or changing a plugin proxy closes its work; this includes independent TCP connections with `tcpMux = false`.
* Closing one proxy's current work leaves sibling proxies running. An unrelated reload no longer clears that local cut. This operation does not change authorization or survive reconnect; remove the proxy configuration or drain to stop routing.

* Fixed a server panic and remote denial of service caused by a client sending a negative `pool_count`. Negative values are now rejected before work-connection pool resources are allocated.
* Fixed `frpc verify` ignoring configured `featureGates`, which caused VirtualNet configurations to be rejected even when the feature was enabled.
* Fixed a case-insensitive validation bypass that allowed `customDomains` under the configured `subDomainHost` to be registered using mixed-case domain names.
* Closing, replacing, or losing a server control now ends its active work streams as well as idle connections. This includes same-runID reconnects; other controls stay connected.
* Unix socket origin failures report the OS error code without logging the private socket pathname.

The fork module release is v1.0.6; the embedded upstream compatibility version remains 0.71.0.
