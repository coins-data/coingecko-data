from api import CoinGeckoAPI
from dotenv import load_dotenv
import os
from utils import test_function
import random
from datetime import datetime, timedelta

# Load environment variables from .pip env file
load_dotenv()

# Initialize the CoinGeckoAPI class
cg = CoinGeckoAPI(os.getenv('COINGECK_API_KEY', None), os.getenv('COINGECK_API_PLAN', 'public'))

# Track test results
test_results = []

# Run tests
test_results.append(('cg.ping', test_function(cg.ping)))
test_results.append(('cg.api_is_up', test_function(cg.api_is_up)))

test_results.append(('cg.get_price', 
    test_function(cg.get_price, 
        ids = 'bitcoin, ethereum, arbitrum', 
        vs_currencies = 'usd, btc', 
        include_market_cap = True, 
        include_24hr_vol = True, 
        include_24hr_change = True, 
        include_last_updated_at = True, 
        precision = 12
    )
))

test_results.append(('cg.get_token_price_by_address', 
    test_function(
        cg.get_token_price_by_address, 
        asset_platform = 'ethereum', 
        contract_addresses = '0x2260fac5e5542a773aa44fbcfedf7c193bc2c599', 
        vs_currencies = 'usd, btc',
        include_market_cap = True,
        include_24hr_vol = True,
        include_24hr_change = True,
        include_last_updated_at = True,
        precision = 12
    )
))

test_results.append(('cg.get_supported_vs_currencies', test_function(cg.get_supported_vs_currencies)))

test_results.append(('cg.get_coins_list', test_function(cg.get_coins_list)))

test_results.append(('cg.get_coins_with_market_data', 
    test_function(
        cg.get_coins_with_market_data, 
        ids = 'bitcoin, ethereum, arbitrum'
    )
))

test_results.append(('cg.get_coin_by_id', test_function(cg.get_coin_by_id, id = 'bitcoin')))

test_results.append(('cg.get_coin_ticker_by_id', 
    test_function(
        cg.get_coin_ticker_by_id, 
        id = 'ethereum', exchange_ids = 'binance', 
        include_exchange_logo = True, depth = True
    )
))

random_date = (datetime.now() - timedelta(days=random.randint(1, 365))).strftime('%d-%m-%Y')
test_results.append(('cg.get_coin_history_by_id', 
    test_function(
        cg.get_coin_history_by_id, 
        id = 'ethereum',
        date = random_date
    )
))

test_results.append(('cg.get_coin_chart_by_id', 
    test_function(
        cg.get_coin_chart_by_id, 
        id = 'ethereum',
        days = 3,
        interval = 'daily',
        precision = 12
    )
))

to_timestamp = int((datetime.now() - timedelta(days=random.randint(0, 182))).timestamp())
from_timestamp = to_timestamp - int(timedelta(days=random.randint(1, 182)).total_seconds())
test_results.append(('cg.get_coin_chart_in_range', 
    test_function(
        cg.get_coin_chart_in_range, 
        id = 'solana',
        from_timestamp = from_timestamp,
        to_timestamp = to_timestamp,
        precision = 12
    )
))

test_results.append(('cg.get_coin_ohlc_by_id', 
    test_function(
        cg.get_coin_ohlc_by_id, 
        id = 'bitcoin',
        days = 7,
        precision = 12
    )
))

# Print test results
passed_tests = sum(1 for _, result in test_results if result)
failed_tests = [test_name for test_name, result in test_results if not result]

print(f"\nTest Results:\nPassed: {passed_tests}\nFailed: {len(failed_tests)}")
if failed_tests:
    print("Failed Tests:")
    for test_name in failed_tests:
        print(f"- {test_name}")