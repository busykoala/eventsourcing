# eventsourcing

A lightweight, extensible Python library for building event-sourced applications.  
Provides **pluggable** interfaces (via `Protocol`) for Pub/Sub, Event Store, Outbox, and Read-Model stores—swap out any component by implementing the right interface. Includes built-in in-memory implementations, middleware (logging, retry, dedupe, metrics), a router, and an outbox processor. Integrates with FastAPI.

## Single Working Example

```zsh
docker compose up -d
```

```zsh
xh --form POST http://127.0.0.1:8000/upload_prescription/ file@image.png
```

## Development

- **Run tests**  
  ```bash
  uv run poe test
  ```

- **Format & lint**  
  ```bash
  uv run poe format
  ```

- **Type-check**  
  ```bash
  uv run poe types
  ```

- **Run example app**  
  ```bash
  uv run poe dev
  ```

## Contributing

1. Fork & clone
2. Create a feature branch
3. Implement your own `Protocol`-compliant component in `pubsub/`, `store/`, or `middleware/`
4. Add tests under `tests/`
5. Open a pull request  
