import dxfeed as dx
from datetime import datetime
from dateutil.relativedelta import relativedelta

symbols = ['AAPL', 'MSFT']
date_time = datetime.now() - relativedelta(days=3)
endpoint = dx.Endpoint('demo.dxfeed.com:7300')

trade_sub = endpoint.create_subscription('Trade').add_symbols(symbols)
trade_sub.get_event_handler().get_dataframe().head(3)

quote_sub = endpoint.create_subscription('Quote').add_symbols(symbols)
quote_sub.get_event_handler().get_dataframe().head(3)

summary_sub = endpoint.create_subscription('Summary').add_symbols(symbols)
summary_sub.get_event_handler().get_dataframe().head(3)

profile_sub = endpoint.create_subscription('Profile').add_symbols(symbols)
profile_sub.get_event_handler().get_dataframe().head(3)

tns_sub = endpoint.create_subscription('TimeAndSale').add_symbols(symbols)
tns_sub.get_event_handler().get_dataframe().head(3)

aggregated_symbols = [symbol + '{=d}' for symbol in symbols]
print(aggregated_symbols)
candle_sub = endpoint.create_subscription('Candle', date_time=date_time).add_symbols(aggregated_symbols)
candle_sub.get_event_handler().get_dataframe().head(3)

order_sub = endpoint.create_subscription('Order').add_symbols(symbols)
order_sub.get_event_handler().get_dataframe().head(3)

underlying_sub = endpoint.create_subscription('Underlying').add_symbols(symbols)
underlying_sub.get_event_handler().get_dataframe().head(3)

series_sub = endpoint.create_subscription('Series').add_symbols(symbols)
series_sub.get_event_handler().get_dataframe().head(3)


import requests
url = 'https://tools.dxfeed.com/ipf?TYPE=OPTION&UNDERLYING=AAPL&limit=1'
r = requests.get(url, auth=('demo', 'demo'))
option_symbol = r.text.split('\n')[1].split(',')[1]
print(option_symbol)


greek_sub = endpoint.create_subscription('Greeks').add_symbols([option_symbol])
greek_sub.get_event_handler().get_dataframe().head(3)

theo_sub = endpoint.create_subscription('TheoPrice').add_symbols([option_symbol])
theo_sub.get_event_handler().get_dataframe().head(3)

endpoint.close_connection()
print(f'Connection status: {endpoint.connection_status}')