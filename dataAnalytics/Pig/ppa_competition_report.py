@outputSchema("newbag:bag{t:tuple(other_win_or_tie:int,  advertiser_id_4:int, advertiser_id_3:int, advertiser_id_2:int, advertiser_id_1:int, is_publisher_4:int, is_publisher_2:int, is_publisher_3:int, is_publisher_1:int, clicks_2:int, clicks_3:int, clicks_1:int, clicks_4:int, win_or_tie:int, revenue_4:double, loss:int, revenue_1:double, revenue_2:double, revenue_3:double, advertiser_price_4:double, advertiser_price_2:double, advertiser_price_3:double, advertiser_price_1:double)}")
def processBag(bag):
    try:

        publisher_id = input
        publisher_price = input

        inbag = [{'external_id':'example1','request_id':'req1','advertiser_price':100,'advertiser_id':1258,'revenue':1.75,'clicks':1},
        {'external_id':'example2','request_id':'req1','advertiser_price':99,'advertiser_id':5648,'revenue':1.75,'clicks':1},
        {'external_id':'example3','request_id':'req1','advertiser_price':99.5,'advertiser_id':8864,'revenue':0,'clicks':0},
        {'external_id':'example4','request_id':'req1','advertiser_price':100.3,'advertiser_id':6548}]

        # Create a publisher row
        for dic in inbag:
            dic['is_publisher'] = 0

        pubdic = {}
        pubdic['advertiser_id'] = publisher_id
        pubdic['advertiser_price'] = publisher_price
        pubdic['is_publisher'] = 1
        pubdic['revenue'] = 0
        pubdic['clicks'] = 0

        inbag.append(pubdic)

        # Determine winning price, runner up and losses, by sorting then looping to find the first looser
        price_sorted = sorted(inbag, key=lambda x: x['advertiser_price'])

        winning_price = price_sorted[0]['advertiser_price']
        runner_up = 0
        losses = 0

        for dic in price_sorted:
            if dic['advertiser_price'] - winning_price < 1:
                dic['win_or_tie'] = 1
                dic['loss'] = 0

            if dic['advertiser_price'] - winning_price >= 1:
                dic['win_or_tie'] = 0
                dic['loss'] = 1
                losses = losses + 1
                if runner_up == 0:
                    runner_up = dic['advertiser_price'];
                

        if runner_up == 0:
            runner_up = winning_price

        for dic in price_sorted:
            dic['other_win_or_tie'] = len(price_sorted) - 1 - losses

        id_sorted = sorted(price_sorted, key=lambda x: x['advertiser_id'])


        # Create final output as a single row for each record with data for each other record.  Inner loop and outer loop

        final_bag = []

        i = 0
        while i < len(id_sorted):

            out_dict = {}
            out_dict['win_or_tie'] = id_sorted[i]['win_or_tie']
            out_dict['loss'] = id_sorted[i]['loss']
            out_dict['other_win_or_tie'] = id_sorted[i]['other_win_or_tie']
            
            counter = 1
            place = i
            while counter < len(id_sorted):

                if place == len(id_sorted):
                    place = 0

                out_dict['is_publisher_' + str(counter)] = id_sorted[place]['is_publisher']
                out_dict['advertiser_id_' + str(counter)] = id_sorted[place]['advertiser_id']
                out_dict['advertiser_price_' + str(counter)] = id_sorted[place]['advertiser_price'] 
                if 'clicks' in id_sorted[place]:
                    out_dict['clicks_' + str(counter)] = id_sorted[place]['clicks']
                else: 
                    out_dict['clicks_' + str(counter)] = 0
                if 'revenue' in id_sorted[place]:
                    out_dict['revenue_' + str(counter)] = id_sorted[place]['revenue'] 
                else:
                     out_dict['revenue_' + str(counter)] = 0   
                counter = counter + 1
                place = place + 1
            
            final_bag.append(out_dict)
            i = i + 1

        return final_bag

    except ValueError:
        return None


