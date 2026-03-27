import datetime as dt

import polars as pl
from cofy.modules.timeseries import ISODuration, Timeseries, TimeseriesSource
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine

from db.schema import history_table


class DBSource(TimeseriesSource):
    def __init__(self, db_url: str | None, itemid: int = 42923):
        super().__init__()
        self.db_url = db_url
        self.itemid = itemid
        self._engine: AsyncEngine | None = create_async_engine(db_url) if db_url else None

    async def fetch_timeseries(
        self,
        start: dt.datetime,
        end: dt.datetime,
        resolution: ISODuration,
        **kwargs,
    ) -> Timeseries:
        """Fetch timeseries data between start and end datetimes with the given resolution."""
        if self._engine is None:
            msg = "DB_URL must be configured before fetching timeseries data."
            raise ValueError(msg)

        start_ts = self._to_epoch_seconds(start)
        end_ts = self._to_epoch_seconds(end)
        statement = (
            select(history_table.c.clock, history_table.c.value)
            .where(history_table.c.itemid == self.itemid)
            .where(history_table.c.clock >= start_ts)
            .where(history_table.c.clock < end_ts)
            .order_by(history_table.c.clock.asc())
        )

        async with self._engine.connect() as connection:
            result = await connection.execute(statement)
            rows = [(int(clock), float(value)) for clock, value in result.all()]

        frame = self._build_frame(rows)
        return Timeseries(frame=frame)

    @staticmethod
    def _to_epoch_seconds(value: dt.datetime) -> int:
        if value.tzinfo is None:
            value = value.replace(tzinfo=dt.UTC)
        return int(value.astimezone(dt.UTC).timestamp())

    @staticmethod
    def _build_frame(rows: list[tuple[int, float]]) -> pl.DataFrame:
        schema = {
            "timestamp": pl.Datetime(time_zone="UTC"),
            "value": pl.Float64,
        }

        if not rows:
            return pl.DataFrame(schema=schema)

        timestamps = [dt.datetime.fromtimestamp(clock, tz=dt.UTC) for clock, _ in rows]
        values = [float(value) for _, value in rows]
        return pl.DataFrame(
            {
                "timestamp": timestamps,
                "value": values,
            },
            schema=schema,
        )
