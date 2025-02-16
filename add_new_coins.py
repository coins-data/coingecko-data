import os
from supabase import create_client, Client
from supabase.client import ClientOptions
from itertools import islice
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

# Get all supported coins on CoinGecko
coins_list = cg.get_coins_list()
max_market_cap_rank = len(coins_list)

# Batch insert the data into the database
def batch_insert(data, batch_size=1000):
    it = iter(data)
    for i in range(0, len(data), batch_size):
        yield ({**record, 'market_cap_rank': max_market_cap_rank} for record in islice(it, batch_size))

try:
    for batch in batch_insert(coins_list):
        batch_list = list(batch)
        response = supabase.table("coins") \
            .insert(batch_list) \
            .execute()
        print(response)
except Exception as exception:
    print(exception)
