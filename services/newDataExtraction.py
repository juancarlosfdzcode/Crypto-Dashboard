import requests
import pandas as pd
import os
from dotenv import load_dotenv
from typing import List, Dict, Optional
from dataclasses import dataclass
import logging
from funciones import processTokenData

# Configurar logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

@dataclass
class Token:
    """Clase para representar un token de criptomoneda"""
    coin: str
    id: str
    
    def __post_init__(self):
        if not self.coin or not self.id:
            raise ValueError("Coin y id son requeridos")

@dataclass
class APIConfig:
    """Configuración para la API de CoinGecko"""
    base_url: str = 'https://api.coingecko.com/api/v3'
    vs_currency: str = 'usd'
    days: int = 100
    interval: str = 'daily'
    timeout: int = 30

class CoinGeckoAPIError(Exception):
    """Excepción personalizada para errores de la API"""
    pass

class CoinGeckoClient:
    """Cliente para interactuar con la API de CoinGecko"""
    
    def __init__(self, api_token: Optional[str] = None, config: Optional[APIConfig] = None):
        load_dotenv()
        self.api_token = api_token or os.getenv('coinGeckoToken')
        self.config = config or APIConfig()
        
        if not self.api_token:
            raise ValueError("API token is required. Set coinGeckoToken in .env file or pass it directly.")
        
        self.headers = {
            'accept': 'application/json',
            'x-cg-api-key': self.api_token
        }
        self.session = requests.Session()
        self.session.headers.update(self.headers)
    
    def ping(self) -> bool:
        """Verificar la conectividad con la API"""
        try:
            url = f"{self.config.base_url}/ping"
            response = self.session.get(url, timeout=self.config.timeout)
            response.raise_for_status()
            logger.info("Conexión establecida con la API de CoinGecko")
            return True
        except requests.RequestException as e:
            logger.error(f"Error conectando con la API: {e}")
            raise CoinGeckoAPIError(f"No se puede establecer conexión con el servidor: {e}")
    
    def get_market_chart(self, token: Token) -> Dict:
        """Obtener datos históricos de mercado para un token"""
        url = (f"{self.config.base_url}/coins/{token.id}/market_chart"
               f"?vs_currency={self.config.vs_currency}"
               f"&days={self.config.days}"
               f"&interval={self.config.interval}")
        
        try:
            logger.info(f"Obteniendo información de: {token.coin}")
            response = self.session.get(url, timeout=self.config.timeout)
            response.raise_for_status()
            
            logger.info(f"Información obtenida correctamente para: {token.coin}")
            return response.json()
            
        except requests.RequestException as e:
            logger.error(f"Error fetching data for {token.coin}: {e}")
            raise CoinGeckoAPIError(f"Error fetching token data for {token.coin}: {e}")
    
    def close(self):
        """Cerrar la sesión"""
        self.session.close()

class CryptoDataExtractor:
    """Extractor principal de datos de criptomonedas"""
    
    def __init__(self, tokens: List[Dict[str, str]], api_config: Optional[APIConfig] = None):
        self.tokens = [Token(**token_dict) for token_dict in tokens]
        self.api_config = api_config or APIConfig()
        self.client: Optional[CoinGeckoClient] = None
        
    def __enter__(self):
        self.client = CoinGeckoClient(config=self.api_config)
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.client:
            self.client.close()
    
    def extract_single_token(self, token: Token) -> Optional[pd.DataFrame]:
        """Extraer y procesar datos de un solo token"""
        try:
            if not self.client:
                raise RuntimeError("Cliente no inicializado. Use el context manager.")
            
            token_data = self.client.get_market_chart(token)
            df_token = processTokenData(token_data, token.coin)
            return df_token
            
        except Exception as e:
            logger.error(f"Error procesando {token.coin}: {e}")
            return None
    
    def extract_all_tokens(self) -> pd.DataFrame:
        """Extraer datos de todos los tokens configurados"""
        if not self.client:
            raise RuntimeError("Cliente no inicializado. Use el context manager.")
        
        # Verificar conectividad
        self.client.ping()
        
        dataframes = []
        failed_tokens = []
        
        for token in self.tokens:
            try:
                df = self.extract_single_token(token)
                if df is not None:
                    dataframes.append(df)
                else:
                    failed_tokens.append(token.coin)
            except Exception as e:
                logger.error(f"Error procesando {token.coin}: {e}")
                failed_tokens.append(token.coin)
        
        if not dataframes:
            raise CoinGeckoAPIError("No se pudieron obtener datos de ningún token")
        
        if failed_tokens:
            logger.warning(f"Tokens que fallaron: {failed_tokens}")
        
        final_df = pd.concat(dataframes, ignore_index=True)
        logger.info(f"Datos extraídos exitosamente. Total de filas: {len(final_df)}")
        
        return final_df

# Función principal refactorizada
def dataExtraction() -> pd.DataFrame:
    """
    Extrae datos históricos de criptomonedas usando la API de CoinGecko.
    
    Returns:
        pd.DataFrame: DataFrame consolidado con datos de todos los tokens
        
    Raises:
        CoinGeckoAPIError: Si hay problemas con la API
        ValueError: Si la configuración es inválida
    """
    
    # Configuración de tokens
    tokens_config = [
        {'coin': 'aave', 'id': 'aave'},
        {'coin': 'cronos', 'id': 'crypto-com-chain'},
        {'coin': 'chainlink', 'id': 'chainlink'}    
    ]
    
    try:
        # Usar context manager para manejo automático de recursos
        with CryptoDataExtractor(tokens_config) as extractor:
            return extractor.extract_all_tokens()
            
    except Exception as e:
        logger.error(f'Error occurred: {str(e)}')
        raise Exception('No se ha podido ejecutar workflow.') from e

# Ejemplo de uso alternativo con configuración personalizada
def dataExtractionCustom(days: int = 365, interval: str = 'daily') -> pd.DataFrame:
    """
    Versión personalizable de la extracción de datos
    
    Args:
        days: Número de días de historia a obtener
        interval: Intervalo de datos ('daily', 'hourly', etc.)
    """
    tokens_config = [
        {'coin': 'bitcoin', 'id': 'bitcoin'},
        {'coin': 'ethereum', 'id': 'ethereum'},
    ]
    
    custom_config = APIConfig(days=days, interval=interval)
    
    with CryptoDataExtractor(tokens_config, custom_config) as extractor:
        return extractor.extract_all_tokens()

# Ejemplo de uso
if __name__ == "__main__":
    try:
        df = dataExtraction()
        print(f"Datos extraídos: {len(df)} filas")
        print(df.head())
        
    except Exception as e:
        logger.error(f"Error en la ejecución: {e}")