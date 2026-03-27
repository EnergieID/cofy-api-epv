import datetime as dt

import polars as pl
from cofy.modules.timeseries import ISODuration, Timeseries, TimeseriesSource
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine

from db.schema import history_table

DEFAULT_RESOLUTION = dt.timedelta(minutes=15)


class DBSource(TimeseriesSource):
    def __init__(self, db_url: str, itemid: int = 42923):
        super().__init__()
        self.itemid = itemid
        self._engine: AsyncEngine = create_async_engine(db_url)

    async def fetch_timeseries(
        self,
        start: dt.datetime,
        end: dt.datetime,
        resolution: ISODuration,
        **kwargs,
    ) -> Timeseries:
        """Fetch timeseries data between start and end datetimes with the given resolution."""

        # To ensure we have enough data points for resampling, we fetch data starting from start - resolution.
        start_ts = self._to_epoch_seconds(start - DEFAULT_RESOLUTION)
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
        frame = self._resample_frame(frame, resolution)

        # We filter the frame again to ensure we only return data points within the requested start and end range after resampling.
        frame = frame.filter((pl.col("timestamp") >= start) & (pl.col("timestamp") < end))

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

    @staticmethod
    def _resample_frame(frame: pl.DataFrame, resolution: ISODuration) -> pl.DataFrame:
        if frame.is_empty() or resolution == DEFAULT_RESOLUTION:
            return frame
        if not isinstance(resolution, dt.timedelta):
            raise ValueError(f"Resolution {resolution} is not supported.")

        if resolution < DEFAULT_RESOLUTION:
            return frame.upsample("timestamp", every=resolution).fill_null(strategy="forward")
        else:
            return frame.group_by_dynamic("timestamp", every=resolution).agg(pl.col("value").mean())

    @property
    def supported_resolutions(self) -> list:
        return ["PT1M", "PT5M", "PT15M", "PT30M", "PT1H", "PT6H", "P1D"]
