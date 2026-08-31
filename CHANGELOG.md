# Changelog

All notable changes to pystardog are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.21.0] - 2026-08-31

### Added

- **Published via PyPI trusted publishing.** This release is built and uploaded
  by GitHub Actions using OIDC, so no long-lived PyPI API token is stored
  anywhere in the project. Releases now ship both a wheel and a source
  distribution, and the uploaded artifacts carry
  [PEP 740](https://peps.python.org/pep-0740/) provenance attestations
  ([#207](https://github.com/stardog-union/pystardog/pull/207)).

- **Process management on `Admin`.** Three new methods for inspecting and
  cancelling server-side processes, backed by the `/admin/processes` endpoints
  ([#198](https://github.com/stardog-union/pystardog/pull/198)):
  - `Admin.process(id)` — information about a single process, as a `ProcessInfo`
  - `Admin.processes()` — information about all running processes
  - `Admin.kill_process(id)` — cancel a running process

  A `ProcessInfo` `TypedDict` is now exported from `stardog.admin`, with the
  fields `type`, `db`, `id`, `user`, `status`, `startTime`, and `endTime`.

- **User-defined query IDs.** All query methods on `Connection` now accept an
  optional `query_id` parameter, passed to the server as the query's `id`. This
  makes a query traceable in the Stardog query log and cancellable through the
  admin API without first having to discover the server-assigned ID
  ([#197](https://github.com/stardog-union/pystardog/pull/197)):
  `select()`, `graph()`, `paths()`, `ask()`, and `update()`.

  ```python
  conn.select("select * where { ?s ?p ?o }", query_id="nightly-audit-001")
  ```

### Fixed

- **`VirtualGraph.update()` now sends `mappings.syntax`.** When updating a
  virtual graph with mappings that carry a syntax (such as `MappingFile` or
  `MappingRaw`), the syntax is now sent alongside the mappings. Previously it
  was omitted, so the server fell back to auto-detection and stored a re-parsed
  form of the mapping that did not round-trip. `new_virtual_graph()` already
  sent this; `update()` now matches it. An explicit `mappings.syntax` in
  `options` still wins (PLAT-9239,
  [#202](https://github.com/stardog-union/pystardog/pull/202)).

- **Mutable default argument in `VirtualGraph.update()`.** The `options`
  parameter defaulted to a shared `{}` instance, which persisted across calls.
  It now defaults to `None` and the caller's dict is copied rather than mutated
  ([#202](https://github.com/stardog-union/pystardog/pull/202)).

- **Incorrect return type annotation on `Admin.namespaces()`.** Annotated as
  `Dict`, but pystardog unwraps the server's JSON object and returns a list. Now
  annotated as `List`
  ([#200](https://github.com/stardog-union/pystardog/pull/200)).

### Changed

- **Modernized type annotations in `stardog/admin.py`.** `typing.Dict` and
  `typing.List` in return annotations were replaced with the builtin `dict` and
  `list` generics. This is an annotation-only change with no runtime effect, and
  the minimum supported Python version remains 3.9
  ([#200](https://github.com/stardog-union/pystardog/pull/200)).

## [0.20.0] - 2026-03-18

Released before this changelog was introduced. See the
[commit history](https://github.com/stardog-union/pystardog/compare/0.19.0...0.20.0).

## [0.19.0] - 2025-08-08

Released before this changelog was introduced. See the
[commit history](https://github.com/stardog-union/pystardog/compare/0.18.1...0.19.0).

[Unreleased]: https://github.com/stardog-union/pystardog/compare/0.21.0...HEAD
[0.21.0]: https://github.com/stardog-union/pystardog/compare/0.20.0...0.21.0
[0.20.0]: https://github.com/stardog-union/pystardog/compare/0.19.0...0.20.0
[0.19.0]: https://github.com/stardog-union/pystardog/compare/0.18.1...0.19.0
