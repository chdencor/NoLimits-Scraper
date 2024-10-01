from models.APIFetch import APIFetch

"""
Clase hija para la API de criptomonedas.
"""
class APICripto(APIFetch):
    def __init__(self, url=None):
        if url is None:
            url = "https://api.coinlore.net/api/tickers/"
        super().__init__(url)  # Llama al constructor de la clase padre con el URL determinado
        self.url = url
    """
    Crea un diccionario con los datos de la API.
    """
    def getKeyValue(self, jsonDictionary):
        dataList = jsonDictionary.get("data")
        return dataList  # Retornamos la lista completa
