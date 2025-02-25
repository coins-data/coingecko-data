import os
from supabase import create_client, Client
from supabase.client import ClientOptions
from coingecko_api.api_to_db_mappings import coins_market_data_to_coins, coins_market_data_to_continuous_prices
from coingecko_api.api import CoinGeckoAPI
from utils.script_logger import ScriptLogger
from dotenv import load_dotenv
import datetime
import random

# Initialize the script logger
log = ScriptLogger("store_real_time_prices")

# Load environment variables from .pip env file
load_dotenv()

# Initialize and Test the CoinGeckoAPI class
cg = CoinGeckoAPI(os.getenv('COINGECKO_API_KEY'), os.getenv('COINGECKO_API_PLAN', 'public'))
if not cg.api_is_up:
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
max_page = random.randint(1, 15)
print(f"Total API pages: {max_page}")

# Get coins that need general data updated (coins table), limit based on max_page
limit = max_page * 350
print(f"Coins Data Update Limit: {limit}")
response = supabase.rpc("coins_to_update", {"p_limit": limit}).execute()
coins_to_update_general = [coin['id'] for coin in response.data]

# From coins table return id where track_prices is true
response = supabase.table("coins").select("id").eq("track_prices", True).execute()
coins_to_add_prices = [coin['id'] for coin in response.data]
total_coins_updated = 0
total_usd_prices_added = 0  
total_btc_prices_added = 0

# Get active coins from CoinGecko, iterate over max_page pages of API calls (250 coins per page)
for page in range(1,max_page+1):
    coins_list = cg.get_coins_with_market_data(page=page)

    for coin in coins_list:

        # Update general data in coins table
        if (coin['id'] in coins_to_update_general):
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
                    print(f"Successfully updated {coin['id']} general data")
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
        if (coin['id'] in coins_to_add_prices):
            try:
                # Modify coin data to match continuous_usd_prices table
                price_data = {value: coin.get(key) for key, value in coins_market_data_to_continuous_prices.items()}
                price_data['vol_24h'] = int(round(price_data['vol_24h']))

                # Add current utc timestamp
                price_data['created_at'] = datetime.datetime.now(datetime.timezone.utc).isoformat()

                # Insert price_data in to continuous_usd_prices table, print success message
                response = supabase.table("continuous_usd_prices") \
                    .insert(price_data) \
                    .execute()
                
                if response.data:   
                    print(f"Successfully added {coin['id']} USD price data")
                    total_usd_prices_added += 1
                else:
                    log.error(f"Unknown Response when adding {coin['id']} USD price data")
                    print("Unknown Response:")
                    print(response)

            except Exception as exception:
                log.error(f"Error adding {coin['id']} USD price data", exception)
                print(exception)
                print(coin)   

for page in range(1,max_page+1):
    coins_list = cg.get_coins_with_market_data(page=page, vs_currency='btc')

    for coin in coins_list:
        # Update price data in continuous_btc_prices table
        if (coin['id'] in coins_to_add_prices):
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
                    print(f"Successfully added {coin['id']} BTC price data")
                    total_btc_prices_added += 1
                else:
                    log.error(f"Unknown Response when adding {coin['id']} BTC price data")
                    print("Unknown Response:")
                    print(response)

            except Exception as exception:
                log.error(f"Error adding {coin['id']} BTC price data", exception)
                print(exception)
                print(coin)                  

log.end(f"{max_page * 250} coins from API, {len(coins_to_update_general)} coins queued for update, {total_coins_updated} coins updated, {total_usd_prices_added} USD prices added, {total_btc_prices_added} BTC prices added")      
