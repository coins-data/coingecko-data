import json
import requests
import time
from urllib.parse import urlencode
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from datetime import timedelta

# Docs for Public API users (Demo plan)
# https://docs.coingecko.com/v3.0.1/reference/introduction

# Docs for Paid Plan Subscribers (users with Pro-API key)
# https://docs.coingecko.com/reference/introduction

class CoinGeckoAPI: 
    __API_URL_BASE = 'https://api.coingecko.com/api/v3/'
    __PRO_API_URL_BASE = 'https://pro-api.coingecko.com/api/v3/'
    __ANALYST_PAUSE_TIME = 120 # 500 requests per minute   
    __DEMO_PAUSE_TIME = 2000 # 30 requests per minute 
    __PUBLIC_PAUSE_TIME = 5000 # 12 request per minute

    def __init__(self, api_key, plan = 'public', retries = 5,):
        self.api_key = api_key
        self.last_request_time = time.time()
        self.request_timeout = 30
        self.plan = plan

        # set pause time based on plan
        if plan == 'demo':
            self.pause_time = self.__DEMO_PAUSE_TIME           
        elif plan == 'analyst':
            self.pause_time = self.__ANALYST_PAUSE_TIME   
        else:
            self.pause_time = self.__PUBLIC_PAUSE_TIME  

        # set base url based on api_key and plan
        if api_key and plan != 'demo':
            self.api_base_url = self.__PRO_API_URL_BASE
        else:
            self.api_base_url = self.__API_URL_BASE

        self.session = requests.Session()
        retries = Retry(total = retries, backoff_factor = 0.5, status_forcelist = [502, 503, 504])
        self.session.mount('https://', HTTPAdapter(max_retries = retries))

    def __request(self, url):
        """Make a request to the CoinGecko API"""
        # print("Request URL: " + url)

        # Pause between requests
        current_time = time.time()
        elapsed_time = (current_time - self.last_request_time) * 1000 
        if elapsed_time < self.pause_time:
            pause_time = round((self.pause_time - elapsed_time) / 1000)
            # print(f"Sleeping for {pause_time} seconds")
            time.sleep(pause_time)
        self.last_request_time = current_time

        # Make request, on error increase pause time
        try:
            response = self.session.get(url, timeout = self.request_timeout)
        except requests.exceptions.RequestException:
            self.pause_time *= 1.1
            raise

        # Check if request was successful
        try:
            response.raise_for_status()
            content = json.loads(response.content.decode('utf-8'))
            return content
        except Exception as e:
            self.pause_time *= 1.1
            # check if json (with error message) is returned
            try:
                content = json.loads(response.content.decode('utf-8'))
                raise ValueError(content)
            # if no json
            except json.decoder.JSONDecodeError as e:
                # TODO: better error handling
                print(f"JSON decode error: {e}")

            raise

    def __append_params(self, api_url, params):
        """Append parameters to the API URL"""

        # If using pro version of CoinGecko, inject key in every call
        if self.api_key:
            if self.plan == 'demo':
                params['x_cg_demo_api_key'] = self.api_key
            else:
                params['x_cg_pro_api_key'] = self.api_key

        # URL encode parameters, and return full URL
        query_string = urlencode({key: str(value).lower() if isinstance(value, bool) else value for key, value in params.items()})
        separator = '&' if '?' in api_url else '?'

        return api_url + separator + query_string

    # ---------- PING ----------#
    # Check API server status
    def ping(self):
        """Check if the API server is up and running"""
        api_url = '{0}ping'.format(self.api_base_url)
        return self.__request(api_url)
    
    def api_is_up(self):
        """Return True if the API server is up and running, False otherwise"""

        try:
            self.ping()
            return True
        except Exception as e:
            return False

    # ---------- SIMPLE ----------#
    # API calls under {API_BASE_URL}/simple/* 

    # Coin Price by IDs
    def get_price(self, ids, vs_currencies, **kwargs):
        """Get the current price of a list of cryptocurrencies in any other supported currencies"""

        ids = ids.replace(' ', '')
        kwargs['ids'] = ids
        vs_currencies = vs_currencies.replace(' ', '')
        kwargs['vs_currencies'] = vs_currencies

        api_url = '{0}simple/price'.format(self.api_base_url)
        api_url = self.__append_params(api_url, kwargs)

        return self.__request(api_url)
    
    # Coin Price by Token Addresses
    def get_token_price_by_address(self, asset_platform, contract_addresses, vs_currencies, **kwargs):
        """Get the current price of any tokens by using token contract address"""

        contract_addresses = contract_addresses.replace(' ', '')
        kwargs['contract_addresses'] = contract_addresses
        vs_currencies = vs_currencies.replace(' ', '')
        kwargs['vs_currencies'] = vs_currencies

        api_url = '{0}simple/token_price/{1}'.format(self.api_base_url, asset_platform)
        api_url = self.__append_params(api_url, kwargs)
        return self.__request(api_url)

    # Supported Currencies List
    def get_supported_vs_currencies(self):
        """Returns a list of supported_vs_currencies (base currencies)"""
        api_url = '{0}simple/supported_vs_currencies'.format(self.api_base_url)
        return self.__request(api_url)
    

    # ---------- COINS ----------#
    # API calls under {API_BASE_URL}/coins/* 

    # Coins List (ID Map)
    def get_coins_list(self):
        """Returns all coins with id, name, symbol, and platforms"""
        api_url = '{0}coins/list'.format(self.api_base_url)
        return self.__request(api_url)
    
    # Coins List with Market Data
    def get_coins_with_market_data(self, vs_currency = 'usd', order = 'volume_desc', per_page = 250, sparkline = True, **kwargs):
        """Returns all coins with price, market cap, volume, and market related data"""

        kwargs['vs_currency'] = vs_currency
        kwargs['order'] = order
        kwargs['per_page'] = per_page
        kwargs['sparkline'] = sparkline

        api_url = '{0}coins/markets'.format(self.api_base_url)
        api_url = self.__append_params(api_url, kwargs)

        return self.__request(api_url)
    
    # Coin Data by ID
    def get_coin_by_id(self, id, localization = False, sparkline = True, **kwargs):
        """Returns all current data (name, price, market, etc.) for a given coin"""

        kwargs['localization'] = localization
        kwargs['sparkline'] = sparkline

        api_url = '{0}coins/{1}/'.format(self.api_base_url, id)
        api_url = self.__append_params(api_url, kwargs)

        return self.__request(api_url)

    # Coin Tickers by ID
    def get_coin_ticker_by_id(self, id, depth = True, **kwargs):
        """Returns coin tickers for a given coin"""

        kwargs['depth'] = depth
        
        api_url = '{0}coins/{1}/tickers'.format(self.api_base_url, id)
        api_url = self.__append_params(api_url, kwargs)

        return self.__request(api_url)

    # Coin Historical Data by ID    
    def get_coin_history_by_id(self, id, date, localization = False, **kwargs):
        """Returns historical data (price, market cap, volume, etc) for a given date and coin"""
  
        # Check if date string is in dd-mm-yyyy format
        try:
            time.strptime(date, '%d-%m-%Y')
        except ValueError:
            raise ValueError("Date must be in dd-mm-yyyy format")
        
        # Check if the plan is not paid and the date is within the last 365 days
        if self.plan in ['demo', 'public']:
            date_obj = time.strptime(date, '%d-%m-%Y')
            if (time.time() - time.mktime(date_obj)) > timedelta(days=365).total_seconds():
                raise ValueError("Date must be within the last 365 days for free plans")
        
        # Append date and localization to kwargs
        kwargs['date'] = date
        kwargs['localization'] = localization

        api_url = '{0}coins/{1}/history'.format(self.api_base_url, id)
        api_url = self.__append_params(api_url, kwargs)

        return self.__request(api_url)
    
    # Coin Historical Chart Data by ID
    def get_coin_chart_by_id(self, id, vs_currency = 'usd', days = 90, **kwargs):
        """Returns historical chart data (price, market cap, and 24h volume) for a given coin"""
        
        kwargs['vs_currency'] = vs_currency
        kwargs['days'] = days
    
        api_url = '{0}coins/{1}/market_chart'.format(self.api_base_url, id)
        api_url = self.__append_params(api_url, kwargs)

        return self.__request(api_url)   
    
    # Coin Historical Chart Data within Time Range by ID
    def get_coin_chart_in_range(self, id, from_timestamp, to_timestamp, vs_currency = 'usd', **kwargs):
        """Returns historical chart data within a time range (price, market cap, and 24h volume) for a given coin"""

        kwargs['vs_currency'] = vs_currency
        kwargs['from'] = from_timestamp
        kwargs['to'] = to_timestamp 

        api_url = '{0}coins/{1}/market_chart/range'.format(self.api_base_url, id)
        api_url = self.__append_params(api_url, kwargs)

        return self.__request(api_url)
    
    # Coin OHLC Chart by ID
    def get_coin_ohlc_by_id(self, id, days, vs_currency = 'usd', **kwargs):
        """Returns Open, High, Low, Close for given coin"""

        kwargs['vs_currency'] = vs_currency
        kwargs['days'] = days
        
        api_url = '{0}coins/{1}/ohlc'.format(self.api_base_url, id)
        api_url = self.__append_params(api_url, kwargs)

        return self.__request(api_url)