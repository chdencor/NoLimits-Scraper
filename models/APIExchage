from models.APIFetch import APIFetch

class APIExchage(APIFetch):
    def __init__(self, url=None):
        if url is None:
            url = "https://api.coinlore.net/api/exchanges/"
        super().__init__(url)

    
    def getKeyValue(self, dictionary):
        return {
            "id": dictionary.get("id", "0"),
            "name": dictionary.get("name", "0"),
            "name_id": dictionary.get("name_id", "0"),
            "volume_usd": dictionary.get("volume_usd", 0),
            "active_pairs": dictionary.get("active_pairs", 0),
            "url": dictionary.get("url", "0"),
            "country": dictionary.get("country", "0")
        }