### Server Plugin

frp server plugin is aimed to extend frp's ability without modifying the Golang code.

An external server should run in a different process receiving RPC calls from frps.
Before frps is doing some operations, it will send RPC requests to notify the external RPC server and act according to its response.

### RPC request

RPC requests are based on JSON over HTTP.

When a server plugin accepts an operation request, it can respond with three different responses:

* Reject operation and return a reason.
* Allow operation and keep original content.
* Allow operation and return modified content.

### Interface

HTTP path can be configured for each manage plugin in frps. We'll assume for this example that it's `/handler`.

A request to the RPC server will look like:

```
POST /handler?version=0.1.0&op=Login
{
    "version": "0.1.0",
    "op": "Login",
    "content": {
        ... // Operation info
    }
}

Request Header:
X-Frp-Reqid: for tracing
```

The response can look like any of the following:

* Non-200 HTTP response status code (this will automatically tell frps that the request should fail)

* Reject operation:

```
{
    "reject": true,
    "reject_reason": "invalid user"
}
```

* Allow operation and keep original content:

```
{
    "reject": false,
    "unchange": true
}
```

* Allow operation and modify content

```
{
    "unchange": "false",
    "content": {
        ... // Replaced content
    }
}
```

### Operation

Currently `Login`, `NewProxy`, `NewProxyResult`, `CloseProxy`, `Ping`, `NewWorkConn` and `NewUserConn` operations are supported.

#### Login

Client login operation

```
{
    "content": {
        "version": <string>,
        "hostname": <string>,
        "os": <string>,
        "arch": <string>,
        "user": <string>,
        "timestamp": <int64>,
        "privilege_key": <string>,
        "run_id": <string>,
        "pool_count": <int>,
        "metas": map<string>string,
        "client_address": <string>
    }
}
```

#### NewProxy

Create new proxy

```
{
    "content": {
        "user": {
            "user": <string>,
            "metas": map<string>string,
            "run_id": <string>
        },
        "attempt_id": <string>,
        "proxy_name": <string>,
        "proxy_type": <string>,
        "use_encryption": <bool>,
        "use_compression": <bool>,
        "bandwidth_limit": <string>,
        "bandwidth_limit_mode": <string>,
        "group": <string>,
        "group_key": <string>,

        // tcp and udp only
        "remote_port": <int>,

        // http and https only
        "custom_domains": []<string>,
        "subdomain": <string>,
        "locations": []<string>,
        "http_user": <string>,
        "http_pwd": <string>,
        "host_header_rewrite": <string>,
        "headers": map<string>string,

        // stcp only
        "sk": <string>,

        // tcpmux only
        "multiplexer": <string>

        "metas": map<string>string
    }
}
```

#### NewProxyResult

Reports the final FRPS admission result for a `NewProxy` attempt. The request is
sent synchronously after the `NewProxy` plugin chain and, when that chain
accepts, after FRPS attempts to register the proxy. `admitted` is true only when
registration completed successfully.

This operation is a notification. Its response cannot reject or modify the
already-decided result. A plugin that prepares external state during `NewProxy`
can use `attempt_id` to correlate that preparation with this final result, then
commit the exact, unnormalized `user.run_id` and `proxy_name` state when
`admitted` is true or roll it back when false. FRPS generates a fresh
cryptographically random 128-bit lowercase-hex `attempt_id` before invoking the
`NewProxy` plugin chain and does not allow plugin responses to change it. A
failed admission is not a `CloseProxy`: FRPS only sends `CloseProxy` for a proxy
that was actually registered and later closed.

Every server-plugin HTTP request, including both `NewProxy` and
`NewProxyResult`, has a 25-second end-to-end timeout. `RegisterProxy` runs
synchronously after `NewProxy` and performs local FRPS registration. FRPS then
accepts `NewProxyResult` into a fixed 256-entry queue serviced by four workers
before returning the `NewProxy` response to the client. Result-plugin HTTP I/O
does not run on the control dispatcher, so a slow result endpoint cannot delay
the admission response or subsequent heartbeat and control messages. Queue
saturation fails the notification immediately and is logged; it never creates
unbounded goroutines or rewrites the already-decided admission outcome.

```
{
    "content": {
        "user": {
            "user": <string>,
            "metas": map<string>string,
            "run_id": <string>
        },
        "attempt_id": <string>,
        "proxy_name": <string>,
        "admitted": <bool>
    }
}
```

#### CloseProxy

A previously created proxy is closed.

`attempt_id` is the same immutable FRPS-generated value that accompanied the
successful `NewProxy` and `NewProxyResult` callbacks for this proxy. Consumers
must use it to distinguish a delayed close from a replacement that reuses the
same `user.run_id` and `proxy_name`.

FRPS enqueues one notification for every interested plugin and proxy into a
fixed 256-entry FIFO serviced by one worker. A full queue fails the enqueue
immediately and is logged, rather than blocking the control dispatcher,
teardown, or same-run-ID re-login. This bounds memory and concurrency without
creating an unbounded number of goroutines.

If an HTTP attempt fails before FRPS receives any response, the worker retries
the notification within a 30-second delivery budget, using the exact same
content, `attempt_id`, and request ID. Retry delay starts at 100 ms and doubles
to a maximum of five seconds. When that budget expires, the worker advances to
the next queued notification; one unreachable endpoint cannot wedge the FIFO
for the process lifetime. A received HTTP response is terminal and is never
retried, including a non-2xx status, an unreadable or malformed body, or a
successful plugin response. `CloseProxy` redirects are not followed; the first
redirect response is itself a terminal non-2xx response. Consumers must make
transport retries idempotent by `attempt_id`, return a response only after
recording the close outcome in the state that governs their contract, and use
TTL or reconciliation for a notification that exhausts its bounded delivery.

FRPS cancels the in-flight request and pending queue when it shuts down instead
of attempting an unbounded drain. A plugin that maintains a live registration
snapshot must reconcile or unregister that snapshot in its own shutdown path.

```
{
    "content": {
        "user": {
            "user": <string>,
            "metas": map<string>string,
            "run_id": <string>
        },
        "attempt_id": <string>,
        "proxy_name": <string>
    }
}
```

#### Ping

Heartbeat from frpc

```
{
    "content": {
        "user": {
            "user": <string>,
            "metas": map<string>string
            "run_id": <string>
        },
        "timestamp": <int64>,
        "privilege_key": <string>
    }
}
```

#### NewWorkConn

New work connection received from frpc (RPC sent after `run_id` is matched with an existing frp connection)

```
{
    "content": {
        "user": {
            "user": <string>,
            "metas": map<string>string
            "run_id": <string>
        },
        "run_id": <string>
        "timestamp": <int64>,
        "privilege_key": <string>
    }
}
```

#### NewUserConn

New user connection received from proxy (support `tcp`, `stcp`, `https` and `tcpmux`) .

```
{
    "content": {
        "user": {
            "user": <string>,
            "metas": map<string>string
            "run_id": <string>
        },
        "proxy_name": <string>,
        "proxy_type": <string>,
        "remote_addr": <string>
    }
}
```

### Server Plugin Configuration

```toml
# frps.toml
bindPort = 7000

[[httpPlugins]]
name = "user-manager"
addr = "127.0.0.1:9000"
path = "/handler"
ops = ["Login"]

[[httpPlugins]]
name = "port-manager"
addr = "127.0.0.1:9001"
path = "/handler"
ops = ["NewProxy"]
```

- addr: the address where the external RPC service listens. Defaults to http. For https, specify the schema: `addr = "https://127.0.0.1:9001"`.
- path: http request url path for the POST request.
- ops: operations plugin needs to handle (e.g. "Login", "NewProxy", ...).
- tlsVerify: When the schema is https, we verify by default. Set this value to false if you want to skip verification.

### Metadata

Metadata will be sent to the server plugin in each RPC request.

There are 2 types of metadata entries - global one and the other under each proxy configuration.
Global metadata entries will be sent in `Login` under the key `metas`, and in any other RPC request under `user.metas`.
Metadata entries under each proxy configuration will be sent in `NewProxy` op only, under `metas`.

This is an example of metadata entries:

```toml
# frpc.toml
serverAddr = "127.0.0.1"
serverPort = 7000
user = "fake"
metadatas.token = "fake"
metadatas.version = "1.0.0"

[[proxies]]
name = "ssh"
type = "tcp"
localPort = 22
remotePort = 6000
metadatas.id = "123"
```
