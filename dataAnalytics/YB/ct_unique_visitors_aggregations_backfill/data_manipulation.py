__author__ = 'yoojong.bang'

import csv
import pandas as pd

input = []

with open('/Users/yoojong.bang/Desktop/uv/part-m-all', 'rb') as input_file:
    reader = csv.reader(input_file, delimiter='\t')

    for i in range(47526):
        input.append(reader.next())

input_df = pd.DataFrame(input, columns=['aggregation_level_date_in_et', 'site_id', 'product_category_type', 'ad_unit_id', 'unique_visitors'])

input_df['aggregation_level_date_in_et'] = [input_df['aggregation_level_date_in_et'][i][:10] for i in range(len(input_df))]

input_df['created_at'] = ['2014-07-23 00:00:00' for i in range(len(input_df))]
input_df['updated_at'] = ['2014-07-23 00:00:00' for i in range(len(input_df))]

cols = ['aggregation_level_date_in_et', 'product_category_type', 'site_id', 'ad_unit_id', 'unique_visitors', 'created_at', 'updated_at']

input_final = input_df[cols]

path = ('/Users/yoojong.bang/Desktop/ct_unique_visitors_aggregations.csv')
input_final.to_csv(path, index=False)
