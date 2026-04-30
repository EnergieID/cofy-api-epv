import asyncio
import csv
import os
from pathlib import Path

from sqlalchemy.ext.asyncio import create_async_engine

from db.schema import history_table, metadata, seuils_api_meteo_table

DEFAULT_SEED_CSV = Path(__file__).with_name("dev-seed.csv")
MAX_RETRIES = 5

# Dummy boundary rows for local development.
# id matches the community names in FORECASTS; timestamp_debut_validite values
# are Unix epoch seconds representing when each set of boundaries became valid.
SEED_BOUNDARIES = [
    # ACC_1 – initial boundaries (Nov 2023)
    {
        "id": "ACC_1",
        "timestamp_debut_validite": 1700000000,
        "seuil_neg2": -1_000_000.0,
        "seuil_neg1": 0.0,
        "seuil_pos1": 5_000_000.0,
        "seuil_pos2": 10_000_000.0,
    },
    # ACC_1 – updated boundaries (Feb 2025)
    {
        "id": "ACC_1",
        "timestamp_debut_validite": 1740000000,
        "seuil_neg2": -500_000.0,
        "seuil_neg1": 0.0,
        "seuil_pos1": 8_000_000.0,
        "seuil_pos2": 12_000_000.0,
    },
]


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
        await connection.execute(seuils_api_meteo_table.insert(), SEED_BOUNDARIES)

    print(f"Seeded {len(rows)} rows into history from {DEFAULT_SEED_CSV}.")
    print(f"Seeded {len(SEED_BOUNDARIES)} rows into seuils_api_meteo.")


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
