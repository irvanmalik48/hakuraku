# TODO

## Refactoring

### Extract `pulse-server/src/main.rs` into Modules

The server `main.rs` is 540+ lines handling config parsing, DB setup, worker pool,
gRPC setup, Axum setup, graceful shutdown, and VM serialization in a single file.

Split into:

| Module | Responsibility |
|---|---|
| `config.rs` | Env parsing into a typed `ServerConfig` struct |
| `worker.rs` | Ingestion worker pool setup + VM push logic |
| `shutdown.rs` | `CancellationToken` orchestration |
| `vm.rs` | `serialize_to_vm_jsonl()` + VM client |

**Effort**: ~3 hours · **Impact**: Testability, readability
