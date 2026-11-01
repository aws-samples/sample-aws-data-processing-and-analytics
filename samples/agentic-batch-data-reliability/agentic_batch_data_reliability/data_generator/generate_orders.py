"""Generate a deterministic synthetic order batch and independent manifest."""

from __future__ import annotations

import argparse
import json
import random
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any

from .failure_injector import Scenario, inject_failure

STATUSES = ("pending", "paid", "fulfilled", "cancelled")
DEFAULT_START = datetime(2026, 1, 1, tzinfo=UTC)


def generate_orders(
    *,
    batch_id: str,
    row_count: int = 100,
    seed: int = 42,
    start_time: datetime = DEFAULT_START,
) -> list[dict[str, Any]]:
    """Return the same business records for the same inputs."""

    if row_count < 0:
        raise ValueError("row_count must be non-negative")
    if not batch_id or any(ch.isspace() for ch in batch_id):
        raise ValueError("batch_id must be non-empty and contain no whitespace")
    rng = random.Random(seed)
    records = []
    for index in range(row_count):
        order_timestamp = start_time + timedelta(minutes=index)
        amount = Decimal(rng.randint(100, 250_000)) / Decimal(100)
        records.append(
            {
                "order_id": f"ord-{batch_id}-{index + 1:08d}",
                "customer_id": f"cus-{rng.randint(1, max(row_count // 4, 10)):08d}",
                "order_timestamp": order_timestamp.isoformat().replace("+00:00", "Z"),
                "order_amount": f"{amount:.2f}",
                "status": STATUSES[rng.randrange(len(STATUSES))],
                "batch_id": batch_id,
            }
        )
    return records


def build_manifest(
    records: list[dict[str, Any]],
    *,
    batch_id: str,
    seed: int,
    scenario: Scenario | str,
    input_file: str,
    generated_at: datetime,
) -> dict[str, Any]:
    """Build expected results before any defect is injected."""

    return {
        "manifest_version": "1.0",
        "batch_id": batch_id,
        "expected_row_count": len(records),
        "expected_distinct_business_keys": len({r["order_id"] for r in records}),
        "input_files": [{"name": input_file, "format": "jsonl"}],
        "generation_timestamp": generated_at.astimezone(UTC).isoformat().replace("+00:00", "Z"),
        "scenario_id": Scenario(scenario).value,
        "random_seed": seed,
        "required_fields": [
            "order_id",
            "customer_id",
            "order_timestamp",
            "order_amount",
            "status",
            "batch_id",
        ],
    }


def write_batch(
    output_directory: Path,
    *,
    records: list[dict[str, Any]],
    manifest: dict[str, Any],
    injection: dict[str, Any],
) -> tuple[Path, Path, Path]:
    output_directory.mkdir(parents=True, exist_ok=True)
    data_path = output_directory / "orders.jsonl"
    manifest_path = output_directory / "manifest.json"
    injection_path = output_directory / "injection.json"
    data_path.write_text(
        "".join(json.dumps(record, sort_keys=True) + "\n" for record in records),
        encoding="utf-8",
    )
    manifest_path.write_text(
        json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    injection_path.write_text(
        json.dumps(injection, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    return data_path, manifest_path, injection_path


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--batch-id", required=True)
    parser.add_argument("--rows", type=int, default=100)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--scenario", choices=[item.value for item in Scenario], default="healthy")
    parser.add_argument(
        "--defer-injection",
        action="store_true",
        help="Record the scenario in the manifest but leave raw data healthy for ETL injection.",
    )
    parser.add_argument("--output", type=Path, required=True)
    return parser.parse_args()


def main() -> None:
    args = _parse_args()
    generated_at = datetime.now(UTC)
    expected = generate_orders(batch_id=args.batch_id, row_count=args.rows, seed=args.seed)
    if args.defer_injection:
        actual = expected
        injection = {
            "scenario_id": args.scenario,
            "injection_stage": "etl",
            "input_record_count": len(expected),
            "output_record_count": len(expected),
            "affected_order_ids": [],
        }
    else:
        actual, injection = inject_failure(expected, args.scenario, seed=args.seed)
    manifest = build_manifest(
        expected,
        batch_id=args.batch_id,
        seed=args.seed,
        scenario=args.scenario,
        input_file="orders.jsonl",
        generated_at=generated_at,
    )
    paths = write_batch(args.output, records=actual, manifest=manifest, injection=injection)
    print(json.dumps({"files": [str(path) for path in paths], "manifest": manifest}, indent=2))


if __name__ == "__main__":
    main()
