import argparse
import sys
from typing import Dict, Callable

from sportsflow.mlb.jobs import (
    update_schedule,
    ingest_games,
    calculate_odds,
)

JOBS: Dict[str, Callable] = {
    "update_schedule": update_schedule.main,
    "ingest_games": ingest_games.main,
    "calculate_odds": calculate_odds.main,
}


def main():
    parser = argparse.ArgumentParser(description="MLB ETL Jobs")
    parser.add_argument(
        "--script", required=True, choices=JOBS.keys(), help="Script to run"
    )
    args = parser.parse_args()

    try:
        job_func = JOBS[args.script]
        job_func(date=args.date, config=args.config)
    except Exception as e:
        print(f"Job {args.script} failed: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
