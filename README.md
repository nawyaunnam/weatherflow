# WeatherFlow — resumable data pipelines

A DAG pipeline runner with dependency validation, retries, persisted task outputs, resumable runs, and a live weather ingestion workflow.

**Focus:** Data engineering / orchestration / pipeline reliability · Python 3.11+ · Standard library · Offline demo

## Run in two commands

From this project directory:

```sh
python3 demo.py
python3 -m unittest discover -v
```

No API keys, paid services, or package downloads are required. The offline demo uses
synthetic or hand-authored examples and temporary storage; it does not access
personal data. See [demo output](demo-output.txt) for a captured local run.

## Design

Versioned DAG + immutable run ID → topological validation → execute ready tasks → JSON outputs in SQLite → downstream transformations. A resumed run reuses successful outputs and retries failed tasks. Each run binds to a graph/version fingerprint; task code changes require a version bump.

## Review the implementation

- [Core implementation](engine.py): domain logic and persistence/algorithms.
- [Executable demo](demo.py): a complete sample workflow.
- [Tests](test_engine.py): expected behavior and edge/failure cases.
- [Architecture and interview notes](DESIGN.md).
- GitHub Actions runs the tests and demo on Python 3.11–3.13 after publishing.

## Scope and limits

Sequential runner with local SQLite state, not an Airflow replacement. No parallel scheduling or cross-process run coordination. Tasks with external side effects must be idempotent because a crash can occur after the side effect and before output persistence.

This is a portfolio implementation, not evidence of production use or business
impact. Any reported metrics describe only the included demonstration data.
Built with AI assistance; review, customize, and understand the implementation
before presenting it as a personal project in an interview.

## Live public-data workflow

Fetches current and hourly weather-model data for fixed New York coordinates from [Open-Meteo](https://open-meteo.com/en/docs), then validates and aggregates it through the resumable DAG. Includes past seven days and the current forecast day. Data attribution: Open-Meteo, CC BY 4.0; source/provider details are linked in the API documentation.

```sh
python3 live.py                         # Fetch real public data, save snapshot.json and report.json
python3 live.py --replay snapshot.json   # Reproduce analysis from the captured data
python3 dashboard.py                    # Open http://127.0.0.1:8090
# In a separate terminal, to keep fetching while viewing the dashboard:
python3 live.py --watch 120
```

The report includes acquisition timestamps and source URLs. HTTP requests use
verified TLS, timeouts, bounded retries, ETags, and a minimum polling interval.
Network failures are explicit; synthetic data is never substituted for live data.
`demo.py` remains an offline synthetic example for tests and onboarding.

The dashboard is a local demonstration, not a publicly deployed service.
Stop the dashboard and live collector with Ctrl+C. Automated CI runs offline tests;
it does not repeatedly call third-party APIs.

## Verified run

- **9 automated tests passed** locally on Python 3.14.
- Live data was fetched successfully; [captured report](live-report.json).
- [Snapshot](snapshot.json) records real data and source acquisition timestamps.
- Offline replay was verified from a fresh temporary working directory.
- [Test log](test-output.txt) and [demo log](demo-output.txt) are included.

To inspect the bundled report without fetching data:

```sh
python3 dashboard.py --report live-report.json
```

## Next engineering milestone

Add task fingerprints for code and configuration, worker isolation, and parallel scheduling with run-level locks.

## License and provenance

Original code: [MIT](LICENSE). [Source attribution](DATA-SOURCES.md).
