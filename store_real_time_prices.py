import os
from supabase import create_client, Client
from supabase.client import ClientOptions
from api import CoinGeckoAPI
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

# Get all active coins from CoinGecko, iterate over 5 pages of API calls (250 coins per page)
for page in range(1,6):
    coins_list = cg.get_coins_with_market_data(page=page)
    for coin in coins_list:
        pprint(coin)
        print("\n")
        if (coin['id'] in coins_to_update):
          try:
              response = supabase.table("coins").insert([coin]).execute()
              print(response)
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
