import datasci.auction.auction_simulator as auction
import mock
import numpy as np
import pandas as pd
import unittest


def ad_call_field_value(field_name):
    if field_name == 'SITE_ID':
        return 2
    elif field_name == 'HOTEL_AVERAGE_NIGHTLY_RATE':
        return 64.95
    else:
        raise Exception('unexpected field call')


def ad_call_unnormalized_field_value(field_name):
    if field_name == 'PUBLISHER_HOTEL_PRICE':
        return 64.95


class TestSimpleAuctionFunctions(unittest.TestCase):
    def setUp(self):
        self.ad_call_mock = mock.Mock(spec=auction.AdCall, get_field_value=ad_call_field_value,
                                      get_unnormalized_field_value=ad_call_unnormalized_field_value)
        self.ad_call_mock.advertiser_ids = [165443, 148708, 155752, 150060, 157259, 148684, 148241, 163284, 165435, 163325]
        advertiser_values_per_click = {165443: 1.1704, 148708: 1.1399, 155752: 1.1340, 150060: 1.1701, 157259: 1.1363,
                                       148684: 1.1476, 148241: 1.1773, 163284: 1.1442, 165435: 1.1462, 163325: 1.3417}
        advertiser_effects = {165443: 7.31e-05, 148708: 4.83e-05, 155752: 3.58e-05, 150060: 0.0001341,
                              157259: 0.0001123, 148684: 9.95e-05, 148241: 5.26e-05, 163284: 4.95e-05,
                              165435: 4.95e-05, 163325: 0.0001385}
        advertiser_prices = {165443: 64.95, 148708: 64.95, 155752: 64.95, 150060: 65.95, 157259: 65.95, 148684: 64.95,
                             148241: 64.95, 163284: 64.95, 165435: 64.95, 163325: 63.95}
        advertiser_minimum_bids = {165443: 0.00, 148708: 0.00, 155752: 0.00, 150060: 0.00, 157259: 0.00, 148684: 0.00,
                             148241: 0.00, 163284: 0.00, 165435: 0.00, 163325: 0.00}
        self.advertiser_information = pd.DataFrame({'advertiser_prices': advertiser_prices,
                                               'advertiser_effects': advertiser_effects,
                                               'advertiser_values_per_click': advertiser_values_per_click,
                                               'advertiser_minimum_bids': advertiser_minimum_bids})
        self.position_effects_mock = mock.Mock(spec=auction.PositionEffects,
                                          get_position_effects=lambda x: [1.1, 1.0, 0.9, 0.8],
                                          get_position_effects_dict=lambda: {2: [1.1, 1.0, 0.9, 0.8]},
                                          get_default_site=lambda: 2)

    def test__is_competitive_auction(self):
        ad_call_mock = mock.Mock(spec=auction.AdCall)
        ad_call_mock.get_field_value.return_value = 200.05
        # ad_call_mock.side_effect = lambda *args, **kw: ad_call_mock.get_field_value(*args, **kw)
        advertiser_prices_1 = [200.0, 200.0, 201.0, 200.0]
        advertiser_prices_2 = [200.0, 200.0, 200.0, 200.0]
        advertiser_prices_3 = [200.0, 100.0, 63.95, 200.0]
        assert auction.SimpleAuction._is_competitive_auction(ad_call_mock, advertiser_prices_1) is False
        assert auction.SimpleAuction._is_competitive_auction(ad_call_mock, advertiser_prices_2) is False
        assert auction.SimpleAuction._is_competitive_auction(ad_call_mock, advertiser_prices_3) is True

    def test__get_auction_results(self):
        simple_auction = auction.SimpleAuction(self.position_effects_mock)
        results = simple_auction.get_auction_results(self.ad_call_mock, self.advertiser_information)
        sorted_results = sorted(results.values(), key=lambda x: x.auction_position)
        # (w_t_plus_one / w_s) * e_s * (x_t - x_t_plus_one) * v_t_plus_one
        # (7.31e-05 / 9.95e-05) * 9.95e-05 * (0.8 - 0.0) * 1.1704
        np.testing.assert_almost_equal(sorted_results[3].payment, 6.8444992e-05)
        # (9.95e-05 / 0.0001123) * 0.000123 * (0.9 - 0.8) * 1.1476 + 6.8444992e-05
        np.testing.assert_almost_equal(sorted_results[2].payment, 7.9863612e-05)
        # (0.0001123 / 0.0001341) * 0.0001341 * (1.0 - 0.9) * 1.1363 + 7.9863612e-05
        np.testing.assert_almost_equal(sorted_results[1].payment, 9.2624261e-05)
        # (0.0001341 / 0.0001385) * 0.0001385 * (1.1 - 1.0) * 1.1701 + 9.2624261e-05
        np.testing.assert_almost_equal(sorted_results[0].payment, 0.000108315302)
        total_realized_value = np.sum([result.realized_utility for result in results.values()])
        np.testing.assert_almost_equal(np.sum(total_realized_value), 0.000567513206)

    def test__get_auction_results_min_bid_1_dot_15(self):
        self.advertiser_information['advertiser_minimum_bids'] = 1.15
        simple_auction = auction.SimpleAuction(self.position_effects_mock)
        results = simple_auction.get_auction_results(self.ad_call_mock, self.advertiser_information)
        sorted_results = sorted(results.values(), key=lambda x: x.auction_position)
        # (w_t_plus_one / w_s) * e_s * (x_t - x_t_plus_one) * v_t_plus_one
        # (7.31e-05 / 9.95e-05) * 9.95e-05 * (0.8 - 0.0) * 1.1704
        np.testing.assert_almost_equal(sorted_results[3].payment, 6.8444992e-05)
        # (9.95e-05 / 0.0001123) * 0.0001123 * (0.9 - 0.8) * 1.15 + 6.8444992e-05
        np.testing.assert_almost_equal(sorted_results[2].payment, 7.9887492e-05)
        # (0.0001123 / 0.0001341) * 0.0001341 * (1.0 - 0.9) * 1.15 + 7.9887492e-05
        np.testing.assert_almost_equal(sorted_results[1].payment, 9.2801992e-05)
        # (0.0001341 / 0.0001385) * 0.0001385 * (1.1 - 1.0) * 1.1701 + 9.2801992e-05
        np.testing.assert_almost_equal(sorted_results[0].payment, 0.000108493033)
        total_realized_value = np.sum([result.realized_utility for result in results.values()])
        np.testing.assert_almost_equal(np.sum(total_realized_value), 0.000567513206)

    def test__get_auction_results_squashed(self):
        simple_auction = auction.SimpleAuction(self.position_effects_mock, qs_exponent=0.5)
        results = simple_auction.get_auction_results(self.ad_call_mock, self.advertiser_information)
        sorted_results = sorted(results.values(), key=lambda x: x.auction_position)
        # (w_t_plus_one / w_s)**q * e_s * (x_t - x_t_plus_one) * v_t_plus_one
        # (7.31e-05 / 9.95e-05)**0.5 * 9.95e-05 * (0.8 - 0.0) * 1.17047
        np.testing.assert_almost_equal(sorted_results[3].payment, 7.985838125888646e-05)
        # (9.95e-05 / 0.0001123)**0.5 * 0.0001123 * (0.9 - 0.8) * 1.1476 +\
        # (7.31e-05 / 0.0001123)**0.5 * 0.0001123 * (0.8 - 0.0) * 1.17047
        np.testing.assert_almost_equal(sorted_results[2].payment, 9.6965441013209083e-05)
        # (0.0001123 / 0.0001341)**0.5 * 0.0001341 * (1.0 - 0.9) * 1.1363 +\
        # (9.95e-05 / 0.0001341)**0.5 * 0.0001341 * (0.9 - 0.8) * 1.1476 +\
        # (7.31e-05 / 0.0001341)**0.5 * 0.0001341 * (0.8 - 0.0) * 1.17047
        np.testing.assert_almost_equal(sorted_results[1].payment, 0.00011990420518366569)
        # (0.0001341 / 0.0001385)**0.5 * 0.0001385 * (1.1 - 1.0) * 1.17 +\
        # (0.0001123 / 0.0001385)**0.5 * 0.0001385 * (1.0 - 0.9) * 1.1363 +\
        # (9.95e-05 / 0.0001385)**0.5 * 0.0001385 * (0.9 - 0.8) * 1.1476 +\
        # (7.31e-05 / 0.0001385)**0.5 * 0.0001385 * (0.8 - 0.0) * 1.17047
        np.testing.assert_almost_equal(sorted_results[0].payment, 0.00013780609448962473)

    def test__get_auction_results_reserve_qwr(self):
        simple_auction = auction.SimpleAuction(
            self.position_effects_mock, reserve_price=0.00012, reserve_price_type='qwr')
        results = simple_auction.get_auction_results(self.ad_call_mock, self.advertiser_information)
        sorted_results = sorted(results.values(), key=lambda x: x.auction_position)
        np.testing.assert_almost_equal(sorted_results[3].payment, 0.0)
        # w_s * (x_t - x_t_plus_one)
        # 0.00012 * 0.9
        np.testing.assert_almost_equal(sorted_results[2].payment, 0.000108)
        # (0.0001123 / 0.0001341) * 0.0001341 * (1.0 - 0.9) * 1.1363 + 0.000108
        np.testing.assert_almost_equal(sorted_results[1].payment, 0.000120760649)
        # (0.0001341 / 0.0001385) * 0.0001385 * (1.1 - 1.0) * 1.17 + 0.000120760649
        np.testing.assert_almost_equal(sorted_results[0].payment, 0.000136450349)

    def test__get_auction_results_reserve_qwr_squashed(self):
        simple_auction = auction.SimpleAuction(
            self.position_effects_mock, reserve_price=0.00012, reserve_price_type='qwr', qs_exponent=0.5)
        results = simple_auction.get_auction_results(self.ad_call_mock, self.advertiser_information)
        sorted_results = sorted(results.values(), key=lambda x: x.auction_position)
        np.testing.assert_almost_equal(sorted_results[3].payment, 0.0)
        # w_s * (x_t - x_t_plus_one)
        # 0.00012 * 0.9
        np.testing.assert_almost_equal(sorted_results[2].payment, 0.000108)
        # (0.0001123 / 0.0001341)**0.5 * 0.0001341 * (1.0 - 0.9) * 1.1363 + 0.000108
        np.testing.assert_almost_equal(sorted_results[1].payment, 0.00012194431785356198)
        # (0.0001341 / 0.0001385)**0.5 * 0.0001385 * (1.1 - 1.0) * 1.1701 +\
        # (0.0001123 / 0.0001385)**0.5 * 0.0001385 * (1.0 - 0.9) * 1.1363 + 0.000108
        np.testing.assert_almost_equal(sorted_results[0].payment, 0.00013811762269263337)

    def test__get_auction_results_reserve_uwr(self):
        simple_auction = auction.SimpleAuction(
            self.position_effects_mock, reserve_price=1.1704, reserve_price_type='uwr')
        results = simple_auction.get_auction_results(self.ad_call_mock, self.advertiser_information)
        sorted_results = sorted(results.values(), key=lambda x: x.auction_position)
        np.testing.assert_almost_equal(sorted_results[3].payment, 0.0)
        # e_s * w_s * (x_t - x_t_plus_one)
        # 5.26e-05 * 1.1704 * 0.9
        np.testing.assert_almost_equal(sorted_results[2].payment, 5.5406736e-05)
        # 5.26e-05 / 7.31e-05 * 7.31e-05 * (1.0 - 0.9) * 1.1773 +\
        # 7.31e-05 * 1.1704 * 0.9
        np.testing.assert_almost_equal(sorted_results[1].payment, 8.3193214e-05)
        # 7.31e-05 / 0.0001385 * 0.0001385 * (1.1 - 1.0) * 1.1704
        # 5.26e-05 / 0.0001385 * 0.0001385 * (1.0 - 0.9) * 1.1773 +\
        # 0.0001385 * 1.1704 * 0.9
        np.testing.assert_almost_equal(sorted_results[0].payment, 0.000160638582)

    def test__get_auction_results_hpa(self):
        simple_auction_lower = auction.SimpleAuction(self.position_effects_mock, min_ads_shown=1)
        simple_auction_equal_or_higher = auction.SimpleAuction(self.position_effects_mock)
        hpaAuction = auction.HpaAuction(
            self.position_effects_mock, simple_auction_lower, simple_auction_equal_or_higher)
        results = hpaAuction.get_auction_results(self.ad_call_mock, self.advertiser_information)
        sorted_results = sorted(results.values(), key=lambda x: x.auction_position)
        self.advertiser_information['vpc_by_e'] = self.advertiser_information['advertiser_effects'] *\
                   self.advertiser_information['advertiser_values_per_click']
        # (w_t_plus_one / w_s)**q * e_s * (x_t - x_t_plus_one) * v_t_plus_one
        # (7.31e-05 / 9.95e-05) * 9.95e-05 * (0.8 - 0.0) * 1.1704
        np.testing.assert_almost_equal(sorted_results[3].payment, 6.8444992e-5)
        # (9.95e-05 / 0.0001123) * 0.0001123 * (0.9 - 0.8) * 1.1476 + 6.8444992e-5
        np.testing.assert_almost_equal(sorted_results[2].payment, 7.9863612e-5)
        # (0.0001123 / 0.0001341) * 0.0001341 * (1.0 - 0.9) * 1.1363 + 7.9863612e-5
        np.testing.assert_almost_equal(sorted_results[1].payment, 9.2624261e-5)
        # single advertiser in 'lower' auction, so second price is 0.0
        np.testing.assert_almost_equal(sorted_results[0].payment, 0.0)

    def test__get_auction_results_hpa_reserve_qwr_low_reserve_price(self):
        simple_auction_lower = auction.SimpleAuction(self.position_effects_mock, min_ads_shown=1,
                                                     reserve_price_type='qwr', reserve_price=0.00012)
        simple_auction_equal_or_higher = auction.SimpleAuction(self.position_effects_mock)
        hpaAuction = auction.HpaAuction(
            self.position_effects_mock, simple_auction_lower, simple_auction_equal_or_higher)
        results = hpaAuction.get_auction_results(self.ad_call_mock, self.advertiser_information)
        sorted_results = sorted(results.values(), key=lambda x: x.auction_position)
        self.advertiser_information['vpc_by_e'] = self.advertiser_information['advertiser_effects'] *\
                   self.advertiser_information['advertiser_values_per_click']
        # (w_t_plus_one / w_s)**q * e_s * (x_t - x_t_plus_one) * v_t_plus_one
        # (7.31e-05 / 9.95e-05) * 9.95e-05 * (0.8 - 0.0) * 1.1704
        np.testing.assert_almost_equal(sorted_results[3].payment, 6.8444992e-5)
        # (9.95e-05 / 0.0001123) * 0.0001123 * (0.9 - 0.8) * 1.1476 + 6.8444992e-5
        np.testing.assert_almost_equal(sorted_results[2].payment, 7.9863612e-5)
        # (0.0001123 / 0.0001341) * 0.0001341 * (1.0 - 0.9) * 1.1363 + 7.9863612e-5
        np.testing.assert_almost_equal(sorted_results[1].payment, 9.2624261e-5)
        # single advertiser in 'lower' auction, so second price is 0.00012
        # 0.00012 * 1.1
        np.testing.assert_almost_equal(sorted_results[0].payment, 0.000132)

    def test__get_auction_results_hpa_reserve_qwr_low_reserve_price(self):
        simple_auction_lower = auction.SimpleAuction(self.position_effects_mock, min_ads_shown=1,
                                                     reserve_price_type='qwr', reserve_price=0.0002)
        simple_auction_equal_or_higher = auction.SimpleAuction(self.position_effects_mock)
        hpaAuction = auction.HpaAuction(
            self.position_effects_mock, simple_auction_lower, simple_auction_equal_or_higher)
        results = hpaAuction.get_auction_results(self.ad_call_mock, self.advertiser_information)
        sorted_results = sorted(results.values(), key=lambda x: x.auction_position)
        self.advertiser_information['vpc_by_e'] = self.advertiser_information['advertiser_effects'] *\
                   self.advertiser_information['advertiser_values_per_click']
        # (w_t_plus_one / w_s)**q * e_s * (x_t - x_t_plus_one) * v_t_plus_one
        # (5.26e-05 / 7.31e-05) * 7.31e-05 * (0.8 - 0.0) * 1.1773
        np.testing.assert_almost_equal(sorted_results[3].payment, 4.9540784e-5)
        # (7.31e-05 / 9.95e-05) * 9.95e-05 * (0.9 - 0.8) * 1.1704 + 4.9540784e-5
        np.testing.assert_almost_equal(sorted_results[2].payment, 5.8096408e-5)
        # (9.95e-05 / 0.0001123) * 0.0001123 * (1.0 - 0.9) * 1.1476 + 5.8096408e-5
        np.testing.assert_almost_equal(sorted_results[1].payment, 6.9515028e-5)
        # (0.0001123 / 0.0001341) * 0.0001341 * (1.1 - 1.0) * 1.1363 + 6.9515028e-5
        np.testing.assert_almost_equal(sorted_results[0].payment, 8.2275677e-5)

if __name__ == '__main__':
    unittest.main()