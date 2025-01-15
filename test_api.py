from api import CoinGeckoAPI
from dotenv import load_dotenv
import os
from utils import test_function

# Load environment variables from .pip env file
load_dotenv()

# Initialize the CoinGeckoAPI class
cg = CoinGeckoAPI(os.getenv('API_KEY'), os.getenv('API_PLAN', 'public'))

# Run tests
test_function(cg.ping)
test_function(cg.api_is_up)

test_function(cg.get_price, 
              ids='bitcoin, ethereum, arbitrum', 
              vs_currencies='usd, btc', 
              include_market_cap = True, 
              include_24hr_vol=True, 
              include_24hr_change=True, 
              include_last_updated_at=True, 
              precision=8)

test_function(cg.get_token_price_by_address, 
              asset_platform = 'ethereum', 
              contract_addresses = '0x2260fac5e5542a773aa44fbcfedf7c193bc2c599', 
              vs_currencies='usd, btc',
              include_market_cap = True,
              include_24hr_vol=True,
              include_24hr_change=True,
              include_last_updated_at=True,
              precision=8)

test_function(cg.get_supported_vs_currencies)