import os
from supabase import create_client, Client
from supabase.client import ClientOptions
from coingecko_api.api_to_db_mappings import coins_market_data_to_coins, coins_market_data_to_continuous_prices
from coingecko_api.api import CoinGeckoAPI
from utils.script_logger import ScriptLogger
from dotenv import load_dotenv
import datetime
import random

# TODO: Randomize calls between updating USD prices and BTC to avoid skipping BTC prices more often

# Initialize the script logger
log = ScriptLogger("store_real_time_prices")

# Set the maximum times in seconds
max_coin_update_time = 25
max_run_time = 50

# Initialize counters
total_coins_updated = 0
total_usd_prices_added = 0  
total_btc_prices_added = 0
api_page_calls = 0

# Load environment variables from .pip env file
load_dotenv()

# Initialize and Test the CoinGeckoAPI class
cg = CoinGeckoAPI(os.getenv('COINGECKO_API_KEY'), os.getenv('COINGECKO_API_PLAN', 'public'))
if not cg.api_is_up:
    log.error("CoinGecko API is not responding")
    raise Exception("CoinGecko API is not responding")

# Initialize Supabase client
url: str = os.getenv("SUPABASE_URL")
key: str = os.getenv("SUPABASE_KEY")
supabase: Client = create_client(url, key,
  options=ClientOptions(
    postgrest_client_timeout=30,
    schema="coingecko",
  ))

# Choose max page randomly to update lower volume
max_page = random.randint(3, 30)

# Get coin update priorities
coin_update_limit = 500 + (max_page * 100)
response = supabase.rpc("coins_to_update", {"p_limit": coin_update_limit}).execute()
coins_to_update = [coin['id'] for coin in response.data]

price_priority_limit = 30 + max_page
response = supabase.rpc("usd_price_priority", {"p_limit": price_priority_limit}).execute()
usd_price_priority = [coin['coin_id'] for coin in response.data]
response = supabase.rpc("btc_price_priority", {"p_limit": price_priority_limit}).execute()
btc_price_priority = [coin['coin_id'] for coin in response.data]

def update_coins_and_usd_prices():
    global log, max_coin_update_time, max_run_time, total_coins_updated, total_usd_prices_added, api_page_calls, max_page, cg, supabase, coins_to_update, usd_price_priority

    # Get active coins from CoinGecko, iterate over max_page pages of API calls (250 coins per page)
    for page_number in range(1,max_page+1):

        # Break if max_total_run_time is reached
        if log.current_run_time_seconds() > max_run_time:
            break
        
        # Random skip to reduce API calls and run time
        if random.random() < (page_number / ( max_page + 2 ))**0.4:
            continue

        coins_list = cg.get_coins_with_market_data(page=page_number)
        api_page_calls += 1

        for coin in coins_list:

            # Update general data in coins table
            if (coin['id'] in coins_to_update) and (log.current_run_time_seconds() < max_coin_update_time):
                try:
                    # Modify coin data to match coins table
                    general_data = {value: coin.get(key) for key, value in coins_market_data_to_coins.items()}
                    for key, value in general_data.items():
                        if isinstance(value, float):
                            general_data[key] = int(round(value))

                    # Add current utc timestamp
                    general_data['updated_at'] = datetime.datetime.now(datetime.timezone.utc).isoformat()

                    # Upsert coin data, print success message
                    response = supabase.table("coins") \
                        .upsert(general_data) \
                        .execute()
                    
                    if response.data:
                        # print(f"Successfully updated {coin['id']} general data")
                        total_coins_updated += 1
                    else:
                        log.error(f"Unknown Response when updating {coin['id']} general data")
                        print("Unknown Response:")
                        print(response)
            
                except Exception as exception:
                    log.error(f"Error updating {coin['id']} general data", exception)
                    print(exception)
                    print(coin['id'])

            # Update price data in continuous_usd_prices table
            if (coin['id'] in usd_price_priority):
                try:
                    # Modify coin data to match continuous_usd_prices table
                    price_data = {value: coin.get(key) for key, value in coins_market_data_to_continuous_prices.items()}
                    price_data['vol_24h'] = int(round(price_data['vol_24h']))

                    # Add current utc timestamp
                    price_data['created_at'] = datetime.datetime.now(datetime.timezone.utc).isoformat()

                    # Insert price_data in to continuous_usd_prices table
                    response = supabase.table("continuous_usd_prices") \
                        .insert(price_data) \
                        .execute()
                    
                    if response.data:   
                        # print(f"Successfully added {coin['id']} USD price data")
                        total_usd_prices_added += 1
                    else:
                        log.error(response)
                        log.error(f"Unknown Response when adding {coin['id']} USD price data")
                        print("Unknown Response:")
                        print(response)

                except Exception as exception:
                    log.error(f"Error adding {coin['id']} USD price data", exception)
                    print(exception)
                    print(coin)   

def update_btc_prices():
    global log, max_coin_update_time, max_run_time, total_coins_updated, total_btc_prices_added, api_page_calls, max_page, cg, supabase, btc_price_priority

    for page_number in range(1,max_page+1):

        # Break if max_total_run_time is reached
        if log.current_run_time_seconds() > max_run_time:
            break

        # Random skip to reduce API calls and run time
        if random.random() < (page_number / ( max_page + 2 ))**0.4:
            continue

        coins_list = cg.get_coins_with_market_data(page=page_number, vs_currency='btc')
        api_page_calls += 1

        for coin in coins_list:
            # Update price data in continuous_btc_prices table
            if (coin['id'] in btc_price_priority):
                try:
                    # Modify coin data to match continuous_usd_prices table
                    price_data = {value: coin.get(key) for key, value in coins_market_data_to_continuous_prices.items()}

                    # Add current utc timestamp
                    price_data['created_at'] = datetime.datetime.now(datetime.timezone.utc).isoformat()
                    price_data['vol_24h'] = int(round(price_data['vol_24h']))
                    
                    # Insert price_data in to continuous_btc_prices table, print success message
                    response = supabase.table("continuous_btc_prices") \
                        .insert(price_data) \
                        .execute()
                    
                    if response.data:   
                        # print(f"Successfully added {coin['id']} BTC price data")
                        total_btc_prices_added += 1
                    else:
                        log.error(f"Unknown Response when adding {coin['id']} BTC price data")
                        print("Unknown Response:")
                        print(response)

                except Exception as exception:
                    log.error(f"Error adding {coin['id']} BTC price data", exception)
                    print(exception)
                    print(coin)        

# Run Function in random order
functions = [update_coins_and_usd_prices, update_btc_prices]
random.shuffle(functions)
for func in functions:
    func()
    if log.current_run_time_seconds() > max_run_time:
        break
    
log.end(f"{max_page} max API page, {api_page_calls * 250} coins from API, {len(coins_to_update)} coins queued for update, {total_coins_updated} coins updated, {total_usd_prices_added} USD prices added, {total_btc_prices_added} BTC prices added")      
