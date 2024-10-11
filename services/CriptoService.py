from models.APICriptoTicker import APICripto

class CriptoService:
    """
    Clase Cripto que contiene todos los métodos para recuperar la información de las criptomonedas.
    """
    def __init__(self):
        self.data = APICripto()
        self._respuesta = self.data.APICall(self.data.url)  # Llamada a la API
        self._parsed_data = None 

    @property
    def parsed_data(self):
        """
        Convierte la respuesta de la API a JSON y la parsea para obtener los datos.

        :return: Lista de diccionarios con los datos de las criptomonedas.
        :rtype: list
        """
        if self._parsed_data is None:  # Si no se ha parseado, realiza el parseo
            parsed_response = self.data.APIParsing(self._respuesta)
            self._parsed_data = self.data.getKeyValue(parsed_response)
        return self._parsed_data

    def getCriptoData(self, criptoId):
        """
        Obtiene los datos de una criptomoneda específica por su ID.

        :param criptoId: ID de la criptomoneda.
        :type criptoId: str
        :return: Diccionario con los datos de la criptomoneda.
        :rtype: dict
        """
        for item in self.parsed_data:
            if item.get("id") == criptoId:
                return item
        return None
    
    @property
    def all_ids(self):
        """
        Obtiene una lista de todos los IDs de las criptomonedas.
        
        :return: Lista de IDs.
        :rtype: list
        """
        return [item.get('id') for item in self.parsed_data if item.get('id') is not None]

    def getSymbol(self, criptoId):
        """
        Obtiene el símbolo de una criptomoneda específica.

        :param criptoId: ID de la criptomoneda.
        :type criptoId: str
        :return: Símbolo de la criptomoneda.
        :rtype: str
        """
        criptoData = self.getCriptoData(criptoId)
        return criptoData.get("symbol") if criptoData else None

    def getName(self, criptoId):
        """
        Obtiene el nombre de una criptomoneda específica.

        :param criptoId: ID de la criptomoneda.
        :type criptoId: str
        :return: Nombre de la criptomoneda.
        :rtype: str
        """
        criptoData = self.getCriptoData(criptoId)
        return criptoData.get("name") if criptoData else None

    def getNameId(self, criptoId):
        """
        Obtiene el 'nameid' de una criptomoneda específica.

        :param criptoId: ID de la criptomoneda.
        :type criptoId: str
        :return: 'Nameid' de la criptomoneda.
        :rtype: str
        """
        criptoData = self.getCriptoData(criptoId)
        return criptoData.get("nameid") if criptoData else None

    def getRank(self, criptoId):
        """
        Obtiene el ranking de una criptomoneda específica.

        :param criptoId: ID de la criptomoneda.
        :type criptoId: str
        :return: Ranking de la criptomoneda.
        :rtype: int
        """
        criptoData = self.getCriptoData(criptoId)
        return criptoData.get("rank") if criptoData else None

    def getPriceUsd(self, criptoId):
        """
        Obtiene el precio en USD de una criptomoneda específica.

        :param criptoId: ID de la criptomoneda.
        :type criptoId: str
        :return: Precio en USD de la criptomoneda.
        :rtype: str
        """
        criptoData = self.getCriptoData(criptoId)
        return criptoData.get("price_usd") if criptoData else None

    def getPercentChange24h(self, criptoId):
        """
        Obtiene el porcentaje de cambio en las últimas 24 horas de una criptomoneda específica.

        :param criptoId: ID de la criptomoneda.
        :type criptoId: str
        :return: Porcentaje de cambio en las últimas 24 horas.
        :rtype: str
        """
        criptoData = self.getCriptoData(criptoId)
        return criptoData.get("percent_change_24h") if criptoData else None

    def getPercentChange1h(self, criptoId):
        """
        Obtiene el porcentaje de cambio en la última hora de una criptomoneda específica.

        :param criptoId: ID de la criptomoneda.
        :type criptoId: str
        :return: Porcentaje de cambio en la última hora.
        :rtype: str
        """
        criptoData = self.getCriptoData(criptoId)
        return criptoData.get("percent_change_1h") if criptoData else None

    def getPercentChange7d(self, criptoId):
        """
        Obtiene el porcentaje de cambio en los últimos 7 días de una criptomoneda específica.

        :param criptoId: ID de la criptomoneda.
        :type criptoId: str
        :return: Porcentaje de cambio en los últimos 7 días.
        :rtype: str
        """
        criptoData = self.getCriptoData(criptoId)
        return criptoData.get("percent_change_7d") if criptoData else None

    def getPriceBtc(self, criptoId):
        """
        Obtiene el precio en BTC de una criptomoneda específica.

        :param criptoId: ID de la criptomoneda.
        :type criptoId: str
        :return: Precio en BTC de la criptomoneda.
        :rtype: str
        """
        criptoData = self.getCriptoData(criptoId)
        return criptoData.get("price_btc") if criptoData else None

    def getMarketCapUsd(self, criptoId):
        """
        Obtiene la capitalización de mercado en USD de una criptomoneda específica.

        :param criptoId: ID de la criptomoneda.
        :type criptoId: str
        :return: Capitalización de mercado en USD de la criptomoneda.
        :rtype: str
        """
        criptoData = self.getCriptoData(criptoId)
        return criptoData.get("market_cap_usd") if criptoData else None

    def getVolume24(self, criptoId):
        """
        Obtiene el volumen en las últimas 24 horas de una criptomoneda específica.

        :param criptoId: ID de la criptomoneda.
        :type criptoId: str
        :return: Volumen en las últimas 24 horas.
        :rtype: float
        """
        criptoData = self.getCriptoData(criptoId)
        return criptoData.get("volume24") if criptoData else None

    def getVolume24a(self, criptoId):
        """
        Obtiene el volumen ajustado en las últimas 24 horas de una criptomoneda específica.

        :param criptoId: ID de la criptomoneda.
        :type criptoId: str
        :return: Volumen ajustado en las últimas 24 horas.
        :rtype: float
        """
        criptoData = self.getCriptoData(criptoId)
        return criptoData.get("volume24a") if criptoData else None

    def getCsupply(self, criptoId):
        """
        Obtiene la oferta circulante de una criptomoneda específica.

        :param criptoId: ID de la criptomoneda.
        :type criptoId: str
        :return: Oferta circulante.
        :rtype: str
        """
        criptoData = self.getCriptoData(criptoId)
        return criptoData.get("csupply") if criptoData else None

    def getTsupply(self, criptoId):
        """
        Obtiene la oferta total de una criptomoneda específica.

        :param criptoId: ID de la criptomoneda.
        :type criptoId: str
        :return: Oferta total.
        :rtype: str
        """
        criptoData = self.getCriptoData(criptoId)
        return criptoData.get("tsupply") if criptoData else None

    def getMsupply(self, criptoId):
        """
        Obtiene la oferta máxima de una criptomoneda específica.

        :param criptoId: ID de la criptomoneda.
        :type criptoId: str
        :return: Oferta máxima.
        :rtype: str
        """
        criptoData = self.getCriptoData(criptoId)
        return criptoData.get("msupply") if criptoData else None
