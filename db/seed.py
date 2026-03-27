import asyncio
import csv
import os
from pathlib import Path

from sqlalchemy.ext.asyncio import create_async_engine

from db.schema import history_table, metadata

DEFAULT_SEED_CSV = Path(__file__).with_name("dev-seed.csv")
MAX_RETRIES = 5


def read_seed_rows(csv_path: Path) -> list[dict[str, int | float]]:
    with csv_path.open(newline="") as handle:
        reader = csv.DictReader(handle)
        return [
            {
                "itemid": int(row["itemid"]),
                "clock": int(row["clock"]),
                "value": float(row["value"]),
                "ns": int(row["ns"]),
            }
            for row in reader
        ]


async def seed() -> None:
    db_url = os.environ.get("DB_URL")
    if not db_url:
        msg = "A database URL is required. Set DB_URL in the environment or load it from .env."
        raise ValueError(msg)

    rows = read_seed_rows(DEFAULT_SEED_CSV)
    engine = create_async_engine(db_url)

    async with engine.begin() as connection:
        await connection.run_sync(metadata.drop_all)
        await connection.run_sync(metadata.create_all)
        await connection.execute(history_table.insert(), rows)

    print(f"Seeded {len(rows)} rows into history from {DEFAULT_SEED_CSV}.")


async def seed_with_retries() -> None:
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            await seed()
            break  # Success, exit the retry loop
        except Exception as e:
            print(f"Attempt {attempt} failed with error: {e}")
            if attempt == MAX_RETRIES:
                print("Max retries reached. Failed to seed the database.")
                raise
            else:
                await asyncio.sleep(2**attempt)  # Exponential backoff before retrying
                print("Retrying...")


if __name__ == "__main__":
    asyncio.run(seed_with_retries())
