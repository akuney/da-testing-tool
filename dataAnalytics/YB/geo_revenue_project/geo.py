__author__ = 'yoojong.bang'

import sys
import csv
import time
import pandas as pd
import bisect as bi

# Input Parameter: length of each input source: clicks, blocks, and locations


def map_click_to_geo(arg1, arg2, arg3):
    # Load pig output for click numeric ip_address, blocks, and locations
    clicks_raw = []
    with open('/Users/yoojong.bang/Desktop/click_value_per_geo/part-r-all', 'rb') as tsvfile:
        reader = csv.reader(tsvfile, delimiter='\t')
        for line in range(int(arg1)):
            clicks_raw.append(reader.next())

    clicks = pd.DataFrame(clicks_raw, columns=['ip_address', 'revenue'])
    clicks['locId'] = 'No Match'
    clicks['country'] = 'NA'
    clicks['region'] = 'NA'

    print "File read: 'clicks' completed"

    blocks_raw = []
    with open('/Users/yoojong.bang/Documents/IM_Documents/GeoLiteCity_20140701/GeoLiteCity-Blocks.csv', 'rb') as blocks_csvfile:
        reader = csv.reader(blocks_csvfile, delimiter=',')
        reader.next()
        header = reader.next()
        for i in range(int(arg2)):
            blocks_raw.append(reader.next())

    blocks = pd.DataFrame(blocks_raw, columns=header)

    print "File read: 'blocks' completed"

    location_raw = []
    with open('/Users/yoojong.bang/Documents/IM_Documents/GeoLiteCity_20140701/GeoLiteCity-Location.csv', 'rb') as location_csvfile:
        reader = csv.reader(location_csvfile, delimiter=',')
        reader.next()
        header = reader.next()
        for i in range(int(arg3)):
            location_raw.append(reader.next())

    locations = pd.DataFrame(location_raw, columns=header)

    print "File read: 'locations' completed"

    # Join clicks and blocks with ip_address
    bst_array_input = blocks['startIpNum'].astype(int)
    for i in range(len(clicks)):
        if clicks['ip_address'][i] is not None:
            pos = bi.bisect_left(bst_array_input, int(clicks['ip_address'][i]))
            if pos != 0:
                if (int(clicks['ip_address'][i]) >= int(blocks['startIpNum'][pos-1])) & (int(clicks['ip_address'][i]) <= int(blocks['endIpNum'][pos-1])):
                    clicks['locId'][i] = blocks['locId'][pos-1]
                    print "pos-1 " + str(i)
                elif (int(clicks['ip_address'][i]) >= int(blocks['startIpNum'][pos])) & (int(clicks['ip_address'][i]) <= int(blocks['endIpNum'][pos])):
                    clicks['locId'][i] = blocks['locId'][pos]
                    print "pos " + str(i)
                else:
                    print "else " + str(i)
                    continue
            else:
                clicks['locId'][i] = blocks['locId'][pos]
                print "exception " + str(i)
        else:
            print "None " + str(i)
            continue

    print "Join clicks and blocks completed"

    # Filter out clicks that cannot be mapped to location
    valid_clicks = clicks[clicks['locId'] != 'No Match']
    valid_clicks.is_copy = False

    # Join valid_clicks with locations
    bst_array_input = locations['locId'].astype(int)
    for i in range(len(valid_clicks)):
        if valid_clicks['locId'][i] is not None:
            pos = bi.bisect_left(bst_array_input, int(valid_clicks['locId'][i]))
            if int(valid_clicks['locId'][i]) == int(locations['locId'][pos]):
                valid_clicks['country'][i] = locations['country'][pos]
                valid_clicks['region'][i] = locations['region'][pos]
            else:
                continue
        else:
            print "None " + str(i)
            continue

    print "Join valid_clicks and locations completed"

    # Group by country and region (collapsing non-US countries to one category and print count and sum of revenue)
    for i in range(len(valid_clicks)):
        if valid_clicks['country'][i] != 'US':
            valid_clicks['country'][i] = 'Non-US'
            valid_clicks['region'][i] = 'Non-US'

    valid_clicks['revenue'] = valid_clicks['revenue'].astype(float)
    per_country_region = valid_clicks.groupby(['country', 'region'])
    summary_value = pd.DataFrame(per_country_region.revenue.sum(), columns=['revenue'])
    summary_count = pd.DataFrame(per_country_region.revenue.count(), columns=['count'])

    summary = pd.concat([summary_count, summary_value], join='inner', axis=1)

    # Export output to csv
    path = ('/Users/yoojong.bang/Desktop/output.csv')
    summary.to_csv(path)


def main():
    """
    This function will take in three arguments, one is the length of clicks (from pig), and tow GeoLite files.
    Then, it returns the csv file which is the summary of click revenue per each state.

    """
    map_click_to_geo(sys.argv[1], sys.argv[2], sys.argv[3])

if __name__ == '__main__':
    start_time = time.time()
    main()
    print("--- %s seconds ---" % time.time() - start_time)