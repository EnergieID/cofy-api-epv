import datetime as dt

import polars as pl
from cofy.modules.timeseries import ISODuration, Timeseries, TimeseriesSource
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine

from db.schema import history_table, seuils_api_meteo_table

DEFAULT_RESOLUTION = dt.timedelta(minutes=15)


class BaseDBSource(TimeseriesSource):
    def __init__(self, db_url: str):
        super().__init__()
        self._engine: AsyncEngine = create_async_engine(db_url)

    @staticmethod
    def _to_epoch_seconds(value: dt.datetime) -> int:
        if value.tzinfo is None:
            value = value.replace(tzinfo=dt.UTC)
        return int(value.astimezone(dt.UTC).timestamp())

    @property
    def supported_resolutions(self) -> list:
        return ["PT1M", "PT5M", "PT15M", "PT30M", "PT1H", "PT6H", "P1D"]


class SignalDBSource(BaseDBSource):
    def __init__(self, db_url: str, itemid: int = 42923):
        super().__init__(db_url)
        self.itemid = itemid

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


class BoundaryDBSource(BaseDBSource):
    """A TimeseriesSource that fetches per-cohort boundary thresholds from the
    seuils_api_meteo table and expands them into a timeseries at the requested
    resolution by repeating (forward-filling) each set of boundaries until the
    next change point."""

    def __init__(self, db_url: str, cohort_id: str):
        super().__init__(db_url)
        self.cohort_id = cohort_id

    async def fetch_timeseries(
        self,
        start: dt.datetime,
        end: dt.datetime,
        resolution: ISODuration,
        **kwargs,
    ) -> Timeseries:
        """Fetch boundary timeseries for the cohort between start and end at the given resolution."""
        if start.tzinfo is None:
            start = start.replace(tzinfo=dt.UTC)
        if end.tzinfo is None:
            end = end.replace(tzinfo=dt.UTC)

        end_ts = self._to_epoch_seconds(end)
        statement = (
            select(
                seuils_api_meteo_table.c.timestamp_debut_validite,
                seuils_api_meteo_table.c.seuil_neg2,
                seuils_api_meteo_table.c.seuil_neg1,
                seuils_api_meteo_table.c.seuil_pos1,
                seuils_api_meteo_table.c.seuil_pos2,
            )
            .where(seuils_api_meteo_table.c.id == self.cohort_id)
            .where(seuils_api_meteo_table.c.timestamp_debut_validite <= end_ts)
            .order_by(seuils_api_meteo_table.c.timestamp_debut_validite.asc())
        )

        async with self._engine.connect() as connection:
            result = await connection.execute(statement)
            rows = [(int(ts), float(n2), float(n1), float(p1), float(p2)) for ts, n2, n1, p1, p2 in result.all()]

        frame = self._build_boundary_frame(rows, start, end, resolution)
        return Timeseries(frame=frame)

    @staticmethod
    def _build_boundary_frame(
        rows: list[tuple[int, float, float, float, float]],
        start: dt.datetime,
        end: dt.datetime,
        resolution: ISODuration,
    ) -> pl.DataFrame:
        schema = {
            "timestamp": pl.Datetime(time_zone="UTC"),
            "b0": pl.Float64,
            "b1": pl.Float64,
            "b2": pl.Float64,
            "b3": pl.Float64,
        }

        if not rows:
            return pl.DataFrame(schema=schema)

        if not isinstance(resolution, dt.timedelta):
            raise ValueError(f"Resolution {resolution} is not supported.")

        # Build a sparse frame of boundary change points (b0–b3 map to seuil_neg2/neg1/pos1/pos2).
        change_points = pl.DataFrame(
            {
                "timestamp": [dt.datetime.fromtimestamp(row[0], tz=dt.UTC) for row in rows],
                "b0": [row[1] for row in rows],
                "b1": [row[2] for row in rows],
                "b2": [row[3] for row in rows],
                "b3": [row[4] for row in rows],
            },
            schema=schema,
        ).sort("timestamp")

        # Build a dense target timestamp series from start to end (exclusive).
        # Floor start to the nearest resolution boundary (aligned to midnight UTC)
        midnight = start.replace(hour=0, minute=0, second=0, microsecond=0)
        aligned_start = midnight + int((start - midnight) / resolution) * resolution

        n_steps = int((end - aligned_start) / resolution) + 1
        if n_steps <= 0:
            return pl.DataFrame(schema=schema)

        timestamps = [aligned_start + resolution * i for i in range(n_steps)]
        full_frame = pl.DataFrame(
            {"timestamp": timestamps},
            schema={"timestamp": pl.Datetime(time_zone="UTC")},
        )

        # Expand change points onto the dense grid via a backward as-of join
        # (each timestamp picks the most recently valid set of boundaries).
        return full_frame.join_asof(change_points, on="timestamp", strategy="backward").drop_nulls()
