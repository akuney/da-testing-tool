select
	d.from_currency as "Local Currency",
	max(d.exchange_rate) as "Exchange Rate to USD"
from
(
	select
		from_currency,
		max(quote_time) as quote_time
	from daily_currency_exchange_rates
	group by
	  from_currency
) latest
left join daily_currency_exchange_rates d
  on d.from_currency = latest.from_currency
  and d.quote_time = latest.quote_time
group by
  d.from_currency

union

select
    'USD' as "Local Currency",
    1.0 as "Exchange Rate to USD"