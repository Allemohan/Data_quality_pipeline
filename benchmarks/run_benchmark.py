"""Performance benchmark measuring actual execution duration and throughput across 1K, 10K, and 100K records."""

from __future__ import annotations

import time
from pathlib import Path
from sqlalchemy import create_engine

from generators.generate_data import generate_dataset
from src.database.connection import init_db
from src.pipeline.runner import PipelineRunner


def benchmark_scale(sizes=(1000, 10000, 100000), seed: int = 42) -> None:
    print("=" * 70)
    print("DATA QUALITY PIPELINE PERFORMANCE BENCHMARK")
    print("=" * 70)
    print(f"{'Dataset Size':<15} | {'Duration (s)':<15} | {'Throughput (rec/s)':<20} | {'Status'}")
    print("-" * 70)

    benchmark_dir = Path("data/benchmarks")
    benchmark_dir.mkdir(parents=True, exist_ok=True)

    for rows in sizes:
        csv_file = benchmark_dir / f"customers_{rows}.csv"
        # 1. Generate dataset
        generate_dataset(rows=rows, seed=seed, output_path=csv_file)

        # 2. Setup fresh in-memory SQLite or test DB
        db_path = benchmark_dir / f"bench_{rows}.db"
        if db_path.exists():
            db_path.unlink()
        engine = create_engine(f"sqlite:///{db_path}")
        init_db(engine)

        runner = PipelineRunner(engine=engine, batch_size=2500)

        # 3. Measure pipeline run
        start_t = time.perf_counter()
        tracker = runner.run(csv_file)
        elapsed = time.perf_counter() - start_t

        rate = rows / elapsed if elapsed > 0 else 0
        print(f"{rows:<15,d} | {elapsed:<15.4f} | {rate:<20.1f} | {tracker.status}")

        engine.dispose()
        if db_path.exists():
            db_path.unlink()
        if csv_file.exists():
            csv_file.unlink()

    print("=" * 70)


if __name__ == "__main__":
    benchmark_scale()

