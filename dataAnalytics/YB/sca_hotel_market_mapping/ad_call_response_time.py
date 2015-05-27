import datetime
import pytz
@outputSchema("requested_at_date_in_et:chararray")
def toEST(unix_timestamp):
    try:
        est_time = datetime.datetime.fromtimestamp(int(str(unix_timestamp)[:10]),pytz.timezone('America/New_York')).strftime('%Y-%m-%d')
        return est_time
    except ValueError:
        return None

import re
@outputSchema("mvt_variant:chararray")
def extractMVTVariant(multivariate_test_attributes_variable):
    try:
        variant = re.search(r'"HOTEL_MARKET_MATCHING_TYPE":"(.*?)"',multivariate_test_attributes_variable).group(1)
        return variant
    except ValueError:
        return None
