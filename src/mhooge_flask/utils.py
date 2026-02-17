from datetime import datetime, timedelta, tzinfo

def _last_sunday_in_month(dt: datetime) -> bool:
    days_offset = list(range(6, -1, -1))
    return 31 - dt.day <= days_offset[dt.weekday()]

class CPHTimeZone(tzinfo):
    def tzname(self, dt: datetime | None) -> str | None:
        return "Copenhagen"

    def utcoffset(self, dt: datetime | None) -> timedelta | None:
        dst = self.dst(dt)
        return dst if dst.total_seconds() > 0 else timedelta(hours=1)
    
    def dst(self, dt: datetime | None) -> timedelta:
        if dt.month > 3 and dt.month < 10:
            return timedelta(hours=2)

        if dt.month < 3 or dt.month > 10:
            return timedelta(hours=0)

        if dt.month == 3 and dt.day > 24 and _last_sunday_in_month(dt):
            return timedelta(hours=2)

        if dt.month == 10 and (dt.day < 25 or not _last_sunday_in_month(dt)):
            return timedelta(hours=2)

        return timedelta(hours=0)
