from models.APIGlobalMetrics import APIGlobalMetrics

class GlobalMetricsService:
    """
    Clase GlobalMetricsService que contiene métodos para recuperar información de las métricas globales del mercado de criptomonedas.
    """
    def __init__(self):
        self.data = APIGlobalMetrics()
        self._respuesta = self.data.APICall(self.data.url)
        self._parsed_data = None

    @property
    def parsed_data(self):
        """Parsea la respuesta de la API solo cuando es necesario."""
        if self._parsed_data is None:
            parsed_response = self.data.APIParsing(self._respuesta)
            self._parsed_data = self.data.getKeyValue(parsed_response)
        return self._parsed_data

    @property
    def coins_count(self):
        """
        Obtiene el número total de criptomonedas.

        :return: Número de criptomonedas.
        :rtype: int
        """
        return self.parsed_data.get("coins_count")

    @property
    def active_markets(self):
        """
        Obtiene el número total de mercados activos.

        :return: Número de mercados activos.
        :rtype: int
        """
        return self.parsed_data.get("active_markets")

    @property
    def total_market_cap(self):
        """
        Obtiene la capitalización total del mercado.

        :return: Capitalización total del mercado.
        :rtype: float
        """
        return self.parsed_data.get("total_mcap")

    @property
    def total_volume(self):
        """
        Obtiene el volumen total del mercado.

        :return: Volumen total del mercado.
        :rtype: float
        """
        return self.parsed_data.get("total_volume")

    @property
    def btc_dominance(self):
        """
        Obtiene el porcentaje de dominancia de Bitcoin.

        :return: Porcentaje de dominancia de Bitcoin.
        :rtype: str
        """
        return self.parsed_data.get("btc_dominance")

    @property
    def eth_dominance(self):
        """
        Obtiene el porcentaje de dominancia de Ethereum.

        :return: Porcentaje de dominancia de Ethereum.
        :rtype: str
        """
        return self.parsed_data.get("eth_dominance")

    @property
    def market_cap_change(self):
        """
        Obtiene el cambio porcentual de la capitalización de mercado.

        :return: Cambio porcentual de la capitalización.
        :rtype: str
        """
        return self.parsed_data.get("mcap_change")

    @property
    def volume_change(self):
        """
        Obtiene el cambio porcentual del volumen de mercado.

        :return: Cambio porcentual del volumen.
        :rtype: str
        """
        return self.parsed_data.get("volume_change")

    @property
    def avg_change_percent(self):
        """
        Obtiene el cambio promedio porcentual del mercado.

        :return: Cambio promedio porcentual.
        :rtype: str
        """
        return self.parsed_data.get("avg_change_percent")

    @property
    def volume_ath(self):
        """
        Obtiene el volumen histórico máximo (ATH) del mercado.

        :return: Volumen ATH.
        :rtype: float
        """
        return self.parsed_data.get("volume_ath")

    @property
    def market_cap_ath(self):
        """
        Obtiene la capitalización de mercado histórica máxima (ATH).

        :return: Capitalización de mercado ATH.
        :rtype: float
        """
        return self.parsed_data.get("mcap_ath")
