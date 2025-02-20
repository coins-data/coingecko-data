import os
from supabase import create_client, Client
from supabase.client import ClientOptions
from coingecko_api.api_to_db_mappings import coins_market_data_to_coins, coins_market_data_to_continuous_prices
from coingecko_api.api import CoinGeckoAPI
from dotenv import load_dotenv
import datetime
import random
from pprint import pprint

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

# Get coins that need general data updated (coins table)
response = supabase.rpc("coins_to_update").execute()
coins_to_update_general = [coin['id'] for coin in response.data]
print("Coins to update general data:")
print(coins_to_update_general)
print()

# From coins table return id where track_prices is true
response = supabase.table("coins").select("id").eq("track_prices", True).execute()
coins_to_add_prices = [coin['id'] for coin in response.data]

# Get active coins from CoinGecko, iterate over max_page pages of API calls (250 coins per page)
# Choose max page randomly to update lower volume coins less often
max_page = random.randint(2, 15)
print(f"Total API pages: {max_page}")
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
                    print(f"Successfully updated {coin['id']}")
                else:
                    print("Unknown Response:")
                    print(response)

            except Exception as exception:
                print(exception)
                print(coin)

        # Update price data in continuous_usd_prices table
        if (coin['id'] in coins_to_add_prices):
            try:
                # Modify coin data to match continuous_usd_prices table
                price_data = {value: coin.get(key) for key, value in coins_market_data_to_continuous_prices.items()}

                # Add current utc timestamp
                price_data['created_at'] = datetime.datetime.now(datetime.timezone.utc).isoformat()

                # Insert price_data in to continuous_usd_prices table, print success message
                response = supabase.table("continuous_usd_prices") \
                    .insert(price_data) \
                    .execute()
                
                if response.data:   
                    print(f"Successfully added {coin['id']} price data")
                else:
                    print("Unknown Response:")
                    print(response)

            except Exception as exception:
                print(exception)
                print(coin)              

# Get price data from coingecko
# price_data = cg.get_price(
#     ids = 'bitcoin', 
#     vs_currencies = 'usd', 
#     include_market_cap = True, 
#     include_24hr_vol = True, 
#     include_24hr_change = True, 
#     include_last_updated_at = True, 
#     precision = 12
# )



# try:
#   response = supabase.table("continuous_btc_prices")
#     .insert([
#       { "id": 1, "name": "Frodo" },
#       { "id": 1, "name": "Sam" },
#     ])
#     .execute()
#   return response
# except Exception as exception:
#   return exception
