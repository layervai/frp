## Fixes

* Closing a client control now closes its active HTTP and other work streams as well as idle work connections. Streams owned by other controls remain connected.
* Unix socket origin failures report the OS error code without logging the private socket pathname.
