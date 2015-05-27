import datetime
import pytz
@outputSchema("requested_at_date_in_et:chararray")
def toEST(unix_timestamp):
    try:
        est_time = datetime.datetime.fromtimestamp(int(unix_timestamp),pytz.timezone('America/New_York')).strftime('%Y-%m-%d')
        return est_time
    except ValueError:
        return None
