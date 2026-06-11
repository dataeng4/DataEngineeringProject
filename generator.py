# generator.py
# ============================================================
# Configuration-Driven Data Pipeline — Record Generator
# ============================================================

import yaml
import sys
import time
from datetime import datetime
from faker import Faker

from database import insert_profiles_batch, log_pipeline_status

# ── Constants ────────────────────────────────────────────────
CONFIG_PATH = "config.yaml"

# ── Helpers ──────────────────────────────────────────────────

def load_config(path: str) -> dict:
    """Load and return the master YAML configuration."""
    with open(path, "r") as fh:
        return yaml.safe_load(fh)


def resolve_faker_value(fake: Faker, provider: str, faker_args: dict | None) -> str | int:
    """
    Resolve a single Faker call from a provider string.

    Supports:
      • Chained accessors  : "unique.email"  → fake.unique.email()
      • Plain providers    : "first_name"    → fake.first_name()
      • Providers with args: "random_int"    → fake.random_int(min=18, max=75)
    """
    parts = provider.split(".")
    obj = fake
    for part in parts:
        obj = getattr(obj, part)
    return obj(**(faker_args or {})) if callable(obj) else obj


def build_record(fake: Faker, fields: list[dict]) -> dict:
    """
    Build a single record dictionary from the field definitions.
    Skips primary-key / autoincrement fields (handled by the DB).
    """
    record = {}
    for field in fields:
        if field.get("primary_key") and field.get("autoincrement"):
            continue
        provider = field.get("faker_provider")
        if provider is None:
            continue
        faker_args = field.get("faker_args")
        record[field["name"]] = resolve_faker_value(fake, provider, faker_args)
    return record


def generate_unique_records(
    fake: Faker,
    fields: list[dict],
    target: int,
) -> list[dict]:
    """
    Generate `target` records while honouring every unique constraint
    declared in the field definitions.

    Strategy
    --------
    Maintain per-field seen-sets for columns marked ``unique: true``.
    Re-draw the entire record whenever any unique field collides rather
    than patching individual fields — keeps the logic simple and avoids
    partial-state bugs.  Faker's own ``unique`` proxy (e.g. unique.email)
    handles its own de-duplication internally, so we only need the
    seen-sets for values that Faker itself does not deduplicate.
    """
    unique_fields = [f["name"] for f in fields if f.get("unique")]
    seen: dict[str, set] = {name: set() for name in unique_fields}

    records: list[dict] = []
    attempts = 0
    max_attempts = target * 20  # guard against infinite loops on tiny domains

    print(f"\n  {'─' * 58}")
    print(f"  Generating {target:,} records …")
    print(f"  {'─' * 58}")

    while len(records) < target:
        attempts += 1
        if attempts > max_attempts:
            print(
                f"\n  [WARN] Exceeded {max_attempts:,} attempts after "
                f"{len(records):,} records — stopping early."
            )
            break

        record = build_record(fake, fields)

        # Check application-level uniqueness (non-Faker-managed columns)
        collision = False
        for field_name in unique_fields:
            # Faker's unique proxy already guarantees global uniqueness for
            # providers called through it; skip those to avoid double-tracking.
            provider = next(
                (f.get("faker_provider", "") for f in fields if f["name"] == field_name),
                "",
            )
            if provider.startswith("unique."):
                continue
            val = record.get(field_name)
            if val in seen[field_name]:
                collision = True
                break
            seen[field_name].add(val)

        if collision:
            continue

        records.append(record)

    return records


def chunked(lst: list, size: int):
    """Yield successive chunks of `size` from `lst`."""
    for i in range(0, len(lst), size):
        yield lst[i : i + size]


def print_progress(
    batch_id: int,
    total_batches: int,
    records_this_batch: int,
    cumulative: int,
    target: int,
    elapsed: float,
    status: str,
) -> None:
    """Print a formatted single-line progress update."""
    pct = cumulative / target * 100
    bar_len = 30
    filled = int(bar_len * pct / 100)
    bar = "█" * filled + "░" * (bar_len - filled)
    ts = datetime.now().strftime("%H:%M:%S")
    print(
        f"  [{ts}]  Batch {batch_id:>3}/{total_batches}"
        f"  |{bar}| {pct:6.2f}%"
        f"  +{records_this_batch:,} rows"
        f"  ({cumulative:,}/{target:,})"
        f"  {elapsed:5.2f}s"
        f"  [{status}]"
    )


# ── Main Orchestration ────────────────────────────────────────

def run_pipeline(config_path: str = CONFIG_PATH) -> None:
    # ── 1. Load configuration ──────────────────────────────────
    config = load_config(config_path)
    pipeline_cfg = config["pipeline"]
    seed: int = pipeline_cfg.get("seed", 42)
    batch_size: int = pipeline_cfg.get("batch_size", 5000)

    # Pick the first table definition (user_profiles)
    table_cfg = config["tables"][0]
    target_records: int = table_cfg["target_records"]
    fields: list[dict] = table_cfg["fields"]

    print("\n" + "=" * 62)
    print("  Configuration-Driven SQLite Data Pipeline  —  generator.py")
    print("=" * 62)
    print(f"  Table          : {table_cfg['name']}")
    print(f"  Target records : {target_records:,}")
    print(f"  Batch size     : {batch_size:,}")
    print(f"  Random seed    : {seed}")
    print("=" * 62)

    # ── 2. Initialise Faker ────────────────────────────────────
    fake = Faker()
    Faker.seed(seed)

    # ── 3. Pre-generate all records ────────────────────────────
    t0 = time.perf_counter()
    all_records = generate_unique_records(fake, fields, target_records)
    gen_time = time.perf_counter() - t0
    print(f"\n  ✔  {len(all_records):,} records generated in {gen_time:.2f}s")

    # ── 4. Batch insertion ─────────────────────────────────────
    batches = list(chunked(all_records, batch_size))
    total_batches = len(batches)
    cumulative = 0

    print(f"\n  Starting insertion — {total_batches} batch(es)\n")
    print(f"  {'─' * 58}")

    pipeline_start = time.perf_counter()

    for batch_id, batch in enumerate(batches, start=1):
        # ── Log RUNNING ──────────────────────────────────────
        log_pipeline_status(
            batch_id=str(batch_id),
            records_processed=0,
            status="RUNNING",
            error_message=None,
        )

        batch_start = time.perf_counter()

        try:
            insert_profiles_batch(batch)
            elapsed = time.perf_counter() - batch_start
            cumulative += len(batch)

            # ── Log SUCCESS ──────────────────────────────────
            log_pipeline_status(
                batch_id=str(batch_id),
                records_processed=len(batch),
                status="SUCCESS",
                error_message=None,
            )

            print_progress(
                batch_id=batch_id,
                total_batches=total_batches,
                records_this_batch=len(batch),
                cumulative=cumulative,
                target=target_records,
                elapsed=elapsed,
                status="SUCCESS",
            )

        except Exception as exc:  # noqa: BLE001
            elapsed = time.perf_counter() - batch_start
            error_msg = str(exc)

            # ── Log FAILED ───────────────────────────────────
            log_pipeline_status(
                batch_id=str(batch_id),
                records_processed=0,
                status="FAILED",
                error_message=error_msg,
            )

            print_progress(
                batch_id=batch_id,
                total_batches=total_batches,
                records_this_batch=0,
                cumulative=cumulative,
                target=target_records,
                elapsed=elapsed,
                status="FAILED",
            )
            print(f"\n  [ERROR] Batch {batch_id} failed: {error_msg}")
            print("  Pipeline aborted — check pipeline_logs for details.\n")
            sys.exit(1)

    total_time = time.perf_counter() - pipeline_start
    rps = cumulative / total_time if total_time > 0 else 0

    print(f"  {'─' * 58}")
    print(f"\n  ✔  Pipeline complete.")
    print(f"     Records inserted : {cumulative:,}")
    print(f"     Total time       : {total_time:.2f}s")
    print(f"     Throughput       : {rps:,.0f} records/sec")
    print("\n" + "=" * 62 + "\n")


# ── Entry point ───────────────────────────────────────────────
if __name__ == "__main__":
    run_pipeline()