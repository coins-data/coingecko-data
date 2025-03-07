import os
from supabase import create_client, Client
from supabase.client import ClientOptions
from itertools import islice
from coingecko_api.api import CoinGeckoAPI
from utils.script_logger import ScriptLogger
from dotenv import load_dotenv

# Initialize the script logger
log = ScriptLogger("add_new_coins")

# Load environment variables from .pip env file
load_dotenv()

# Initialize and Test the CoinGeckoAPI class
cg = CoinGeckoAPI(os.getenv('COINGECKO_API_KEY'), os.getenv('COINGECKO_API_PLAN', 'public'))
if not cg.api_is_up:
    log.error("CoinGecko API not responding")
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
total_coins = len(coins_list)

# Batch insert the data into the database
def batch_insert(data, batch_size=500):
    it = iter(data)
    for i in range(0, len(data), batch_size):
        yield ({**record, 'market_cap_rank': max_market_cap_rank} for record in islice(it, batch_size))

coins_added = []
for batch in batch_insert(coins_list):
    batch_list = list(batch)
    try:  
        response = supabase.table("coins") \
            .upsert(batch_list, ignore_duplicates=True) \
            .execute()
        if response.data:
            coins_added.extend(response.data)
            # print(f"Successfully added {len(response.data)} of {len(batch_list)} coins.")
    except Exception as exception:
        log.error("Error adding coins", exception)
        print(exception)

print()
print(f"Total coins in CoinGecko: {total_coins}")
print(f"Total coins skipped: {total_coins - len(coins_added)}")
print(f"Total coins added: {len(coins_added)}")
if coins_added:
    print()
    print("Coins added:")
    for coin in coins_added:
        print(coin['id'])

log.end(f"Total coins in CoinGecko: {total_coins}, Total coins added: {len(coins_added)}, Total coins skipped: {total_coins - len(coins_added)}")
