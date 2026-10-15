# Employee Attendance & Analytics API

FastAPI + MongoDB API for employee records, race-safe punch-in/out, audited manual corrections, monthly analytics, department trends, late-comer ranking, and MongoDB execution-plan inspection.

## Run locally

Requirements: Python 3.11+, MongoDB 6.0+.

```bash
python -m venv .venv
# Windows: .venv\\Scripts\\activate
# macOS/Linux: source .venv/bin/activate
pip install -r requirements.txt
# Set MONGO_URI and MONGO_DB in your environment if not using localhost defaults.
uvicorn app.main:app --port 8000
```

Swagger UI: `http://localhost:8000/docs`; readiness check: `GET /health`.

Indexes are created automatically at startup. Configuration is read from `MONGO_URI` and `MONGO_DB`; no secrets or `.env` file are included. `REVIEW.md` records starter defects and `DECISIONS.md` explains the main design choices.

**Note:** I could compile-check the application source in this environment, but could not install dependencies or run integration tests because package-network access was unavailable. Run against MongoDB 6.0+ and exercise the OpenAPI endpoints before treating this as verified.
