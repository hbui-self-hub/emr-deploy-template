from datetime import datetime, timedelta, tzinfo

class JST(tzinfo):
    def utcoffset(self, dt):
        return timedelta(hours=9)
    def dst(self, dt):
        return timedelta(0)
    def tzname(self, dt):
        return 'JST'

def time_now():
    return datetime.now(JST())
