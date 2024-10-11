from models.APIFetch import APIFetch

class APIMarkets(APIFetch):
    def __init__(self, url=None):
        if url is None:
            url = "https://api.coinlore.net/api/coin/markets/?id=90"
        super().__init__(url)

    
    def getKeyValue(self, dictionary):
        return {
            "name": dictionary.get("name", 0),
            "base": dictionary.get("base", 0),
            "quote": dictionary.get("quote", 0),
            "price": dictionary.get("price", 0),
            "price_usd": dictionary.get("price_usd", "0"),
            "volume": dictionary.get("volume", "0"),
            "volume_usd": dictionary.get("volume_usd", "0"),
            "time": dictionary.get("time", "0")
        }