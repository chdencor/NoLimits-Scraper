from models.APIFetch import APIFetch

class APIGlobalMetrics(APIFetch):
    def __init__(self, url=None):
        if url is None:
            url = "https://api.coinlore.net/api/global/"
        super().__init__(url)

    
    def getKeyValue(self, dictionary):
        return {
            "coins_count": dictionary.get("coins_count", 0),
            "active_markets": dictionary.get("active_markets", 0),
            "total_mcap": dictionary.get("total_mcap", 0),
            "total_volume": dictionary.get("total_volume", 0),
            "btc_dominance": dictionary.get("btc_d", "0"),
            "eth_dominance": dictionary.get("eth_d", "0"),
            "mcap_change": dictionary.get("mcap_change", "0"),
            "volume_change": dictionary.get("volume_change", "0"),
            "avg_change_percent": dictionary.get("avg_change_percent", "0"),
            "volume_ath": dictionary.get("volume_ath", 0),
            "mcap_ath": dictionary.get("mcap_ath", 0)
        }