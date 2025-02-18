import os
from supabase import create_client, Client
from supabase.client import ClientOptions
from coingecko_api.api_to_db_mappings import coins_market_data_to_coins, coins_market_data_to_btc_prices
from coingecko_api.api import CoinGeckoAPI
from dotenv import load_dotenv
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
coins_to_update = [coin['id'] for coin in response.data]
print("Coins to update:")
print(coins_to_update)
print()

# Get all active coins from CoinGecko, iterate over 5 pages of API calls (250 coins per page)
for page in range(1,6):
    coins_list = cg.get_coins_with_market_data(page=page)
    for coin in coins_list:
        if (coin['id'] in coins_to_update):
            try:
                # Modify coin data to match db schema
                coin = {value: coin.get(key) for key, value in coins_market_data_to_coins.items()}
                for key, value in coin.items():
                    if isinstance(value, float):
                        coin[key] = int(round(value))

                # Upsert coin data
                response = supabase.table("coins") \
                    .upsert(coin) \
                    .execute()
                
                # Print the response
                if response.data:
                    print(f"Successfully updated {coin['id']}")
            except Exception as exception:
                print(exception)

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
