import datetime as dt

from cofy.modules.timeseries import ISODuration, Timeseries, TimeSeriesSource


class DBSource(TimeSeriesSource):
    def __init__(self, db_url: str, forcast_id: int = 42923):
        super().__init__()
        self.db_url = db_url
        self.forcast_id = forcast_id

    async def fetch_timeseries(
        self,
        start: dt.datetime,
        end: dt.datetime,
        resolution: ISODuration,
        **kwargs,
    ) -> Timeseries:
        """Fetch timeseries data between start and end datetimes with the given resolution."""