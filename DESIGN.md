# Engineering notes: WeatherFlow — resumable data pipelines

## Problem and flow

Versioned DAG + immutable run ID → topological validation → execute ready tasks → JSON outputs in SQLite → downstream transformations. A resumed run reuses successful outputs and retries failed tasks. Each run binds to a graph/version fingerprint; task code changes require a version bump.

## Current boundaries

Sequential runner with local SQLite state, not an Airflow replacement. No parallel scheduling or cross-process run coordination. Tasks with external side effects must be idempotent because a crash can occur after the side effect and before output persistence.

## Interview walkthrough

1. Run the demo and explain each output in terms of the code.
2. Show a test that exercises a failure rather than only a successful call.
3. Trace one input through the core implementation and its stored state.
4. Explain the tradeoff made by the current storage or algorithm choice.
5. Describe what would change with 100× the data or concurrent users.
6. Make a small extension and add a regression test before using this in a resume.

## Validation

See `test_engine.py` for executable assertions and `docs/demo-output.txt` for
captured results. CI is configured but remote CI results are not assumed.
