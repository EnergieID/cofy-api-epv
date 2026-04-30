import asyncio
import datetime as dt
import math
import os

from sqlalchemy.ext.asyncio import create_async_engine

from db.schema import history_table, metadata, seuils_api_meteo_table

MAX_RETRIES = 5
RESOLUTION = dt.timedelta(minutes=15)
WINDOW_DAYS = 10
ITEMID = 42923
COHORT_ID = "ACC_1"


def _duck_curve(hour: float, day_of_year: int) -> float:
    """Net community power (W) at a given hour-of-day using a simplified duck curve.

    Positive = net import from grid, negative = net export (solar surplus).
    """
    # Seasonal solar factor: April–May has decent sun (~0.85–1.0)
    seasonal = 0.85 + 0.15 * math.sin(2 * math.pi * (day_of_year - 80) / 365)

    base = 4_000_000.0
    morning = 3_000_000.0 * math.exp(-0.5 * ((hour - 8.0) / 1.5) ** 2)
    solar = -9_500_000.0 * math.exp(-0.5 * ((hour - 13.0) / 2.5) ** 2) * seasonal
    evening = 8_000_000.0 * math.exp(-0.5 * ((hour - 18.0) / 2.0) ** 2)
    return base + morning + solar + evening


def generate_history_rows(start: dt.datetime, end: dt.datetime) -> list[dict]:
    rows = []
    t = start
    while t < end:
        hour = t.hour + t.minute / 60
        doy = t.timetuple().tm_yday
        # Small deterministic ripple so adjacent 15-min slots differ
        ripple = 150_000 * math.sin(t.timestamp() / 900 * 1.3)
        value = _duck_curve(hour, doy) + ripple
        rows.append(
            {
                "itemid": ITEMID,
                "clock": int(t.timestamp()),
                "value": round(value, 3),
                "ns": 0,
            }
        )
        t += RESOLUTION
    return rows


def generate_boundary_rows(start: dt.datetime) -> list[dict]:
    """A few boundary snapshots spread across the seeding window (W)."""
    # seuil_neg2 < seuil_neg1 < 0 < seuil_pos1 < seuil_pos2
    snapshots = [
        # day 0: initial config at window open
        (start, -7_000_000.0, -2_000_000.0, 5_000_000.0, 9_000_000.0),
        # day +5: revised up after a sunny stretch
        (start + dt.timedelta(days=5), -8_000_000.0, -2_500_000.0, 6_000_000.0, 10_000_000.0),
        # day +14: tighter neg threshold after grid update
        (start + dt.timedelta(days=14), -6_000_000.0, -1_500_000.0, 6_000_000.0, 10_000_000.0),
    ]
    return [
        {
            "id": COHORT_ID,
            "timestamp_debut_validite": int(ts.timestamp()),
            "seuil_neg2": n2,
            "seuil_neg1": n1,
            "seuil_pos1": p1,
            "seuil_pos2": p2,
        }
        for ts, n2, n1, p1, p2 in snapshots
    ]


async def seed() -> None:
    db_url = os.environ.get("DB_URL")
    if not db_url:
        msg = "A database URL is required. Set DB_URL in the environment or load it from .env."
        raise ValueError(msg)

    now = dt.datetime.now(tz=dt.UTC).replace(hour=0, minute=0, second=0, microsecond=0)
    start = now - dt.timedelta(days=WINDOW_DAYS)
    end = now + dt.timedelta(days=WINDOW_DAYS)

    history_rows = generate_history_rows(start, end)
    boundary_rows = generate_boundary_rows(start)

    engine = create_async_engine(db_url)
    async with engine.begin() as connection:
        await connection.run_sync(metadata.drop_all)
        await connection.run_sync(metadata.create_all)
        await connection.execute(history_table.insert(), history_rows)
        await connection.execute(seuils_api_meteo_table.insert(), boundary_rows)

    print(f"Seeded {len(history_rows)} rows into history ({start.date()} – {end.date()}).")
    print(f"Seeded {len(boundary_rows)} rows into seuils_api_meteo.")


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
