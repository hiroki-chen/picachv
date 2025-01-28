# Integration

> [NOTE]
>
> This documentation contains intensive technical details and may not be suitable for all audience.

## Picachv FFI APIs

We first introduce important Picachv APIs that must be called by the analytical frameworks for proper policy enforcement.

### Context Initialization
```c
ErrorCode open_new(uint8_t *uuid, size_t uuid_len);
```

The first step is to initialize the policy enforcement context `context` managed by a what we call `PicachvMonitor`, a global singleton protected by thread-safe `OnceLock`. This function tries to open a new enforcement context. The caller should prepare a buffer for receiving the UUID of the new context.

**Example:**

```c
#define UUID_LEN 16

/* ... */

uint8_t *ctx_uuid = (uint8_t *)(malloc(UUID_LEN));
if (open_new(ctx_uuid, UUID_LEN) != ErrorCode::Success) {
  char error[128];
  last_error(error, 128);

  printf("cannot open a new context: %s\n", error);
}
```

