"""Entry point for ingestion pipelines. Run via `python -m ingestion.run`."""

import argparse
from ingestion.sources import REGISTRY


def main() -> None:
    parser = argparse.ArgumentParser(description="Healthcare DW ingestion runner")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--all", action="store_true", help="Run every registered source")
    group.add_argument("--source", choices=list(REGISTRY), help="Run a single source")
    args = parser.parse_args()

    sources = list(REGISTRY) if args.all else [args.source]
    for name in sources:
        pipeline = REGISTRY[name]()
        pipeline.run()


if __name__ == "__main__":
    main()
