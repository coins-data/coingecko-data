# get_coins_with_market_data() to coins table
# https://docs.coingecko.com/v3.0.1/reference/coins-markets
coins_market_data_to_coins = {
    "id": "id",
    "symbol": "symbol",
    "name": "name",
    "image": "image_url",
    "market_cap_rank": "market_cap_rank",
    "market_cap" : "market_cap_usd",
    "fully_diluted_valuation": "fully_diluted_valuation",
    "total_supply": "total_supply",
    "max_supply": "max_supply",
    "circulating_supply": "circulating_supply"
}

# get_coins_with_market_data() to continuous_btc_prices table
# https://docs.coingecko.com/v3.0.1/reference/coins-markets
coins_market_data_to_btc_prices = {
    "id": "coin_id",
    "last_updated": "timestamp",
    "current_price": "price",
    "total_volume": "vol_24h",
    "high_24h": "high_24h",
    "low_24h": "low_24h",
    "price_change_percentage_24h": "price_change_percentage_24h"
}