# Local runbook

## Standard run

1. Create and activate a Python 3.11 virtual environment.
2. Install the project with the `dev` extra.
3. Run `python -m supply_chain_ops generate`.
4. Run `python -m supply_chain_ops run`.
5. Run `pytest` and inspect `data/evidence/latest_run.json`.

Use a smaller `--shipments` value during development. The acceptance run uses the default
250,000.

## Rerun behavior

An unchanged successful source file is skipped by path plus SHA-256 hash. Silver and gold are
then reconstructed deterministically from the current bronze snapshot. This deliberately
separates incremental file ingestion from downstream optimization.

To regenerate the source intentionally, use `generate --force`. The generator refuses casual
replacement. Generated data and evidence are ignored by Git.

## Failure triage

- `source file set differs from manifest`: a file is missing or unexpected.
- `hash mismatch`: source content changed after generation; regenerate intentionally or restore
  the source.
- `schema mismatch` / `type mismatch`: upstream contract changed; do not auto-evolve silently.
- SQL assertion error: inspect the named table and rule before rerunning.
- Quarantine spike: group `quarantine_records` by rule and source file, then inspect redacted
  samples from `raw_record_json`.

