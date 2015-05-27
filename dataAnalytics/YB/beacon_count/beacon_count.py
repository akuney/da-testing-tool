__author__ = 'yoojong.bang'

import csv
import pandas as pd

input = []

# Beacon count
# with open('/Users/yoojong.bang/Desktop/beacons/part-r-all', 'rb') as input_file:
#     reader = csv.reader(input_file, delimiter='\t')
#
#     for i in range(804):
#         input.append(reader.next())
#
# input_df = pd.DataFrame(input, columns=['aggregation_level_date_in_et', 'site_type', 'site_country', 'product_category_type', 'browser', 'member_id', 'referrer_url', 'beacon_count'])
#
# input_df['aggregation_level_date_in_et'] = [input_df['aggregation_level_date_in_et'][i][:10] for i in range(len(input_df))]
#
# path = ('/Users/yoojong.bang/Desktop/beacon_count.csv')
# input_df.to_csv(path, index=False)

# Site type and referrer url
with open('/Users/yoojong.bang/Desktop/beacons/part-m-all', 'rb') as input_file:
    reader = csv.reader(input_file, delimiter='\t')

    for i in range(109205):
        input.append(reader.next())

input_df = pd.DataFrame(input, columns=['site_type', 'browser', 'member_id', 'referrer_url'])

path = ('/Users/yoojong.bang/Desktop/beacon_referrer_url.csv')
input_df.to_csv(path, index=False)