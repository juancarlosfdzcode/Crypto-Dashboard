import requests
import pandas as pd
import os
from dotenv import load_dotenv

def dataExtraction():

    """ 
    Escribir documentación
    """
    
    ## Importar funciones ##

    from funciones import processTokenData

    ## Cargar variables .env ##

    load_dotenv()
    coinGeckoToken = os.getenv('coinGeckoToken')

    urlPing = 'https://api.coingecko.com/api/v3/ping'
    baseUrlTokenPrice = 'https://api.coingecko.com/api/v3/coins/'

    headers = {'accept': 'application/json', 'x-cg-api-key': coinGeckoToken}

    vs_currency = 'usd'
    days = 100
    interval = 'daily'

    ## Diccionario de Tokens ##

    tokensDict = [
        {'coin': 'aave', 'id': 'aave'},
        {'coin': 'cronos', 'id': 'crypto-com-chain'},
        {'coin': 'chainlink', 'id': 'chainlink'}    
    ]

    ## Code ##

    allRows = []
    allDataFrames = []

    ## Check API server status ##

    try:

        response = requests.get(urlPing, headers=headers)

        if response.status_code == 200:

            print(f'Conexión establecida')
    
        else:

            raise Exception('No puede establecerse conexión con el servidor.')
        
        ## Fetch Token Price ##

        for token in tokensDict:

            coin = token['coin']
            id = token['id']

            UrlTokenPrice = baseUrlTokenPrice + f'{id}/market_chart?vs_currency={vs_currency}&days={days}&interval={interval}'

            response = requests.get(UrlTokenPrice, headers=headers)

            print(f'Obteniendo información de: {coin}')

            if response.status_code == 200:
                print(f'Información obtenida correctamente.')

                token_data = response.json()
                allRows.append({'coin': coin, 'data': token_data})
                df_token = processTokenData(token_data, coin)
                allDataFrames.append(df_token)

            else:
                print(f'Error fetching token data. Status code: {response.status_code}')
                print(f'Response: {response.text}')
        
        if allDataFrames:
            final_df = pd.concat(allDataFrames, ignore_index=True)

            print(len(final_df))

            return final_df

    except Exception as e:
        print(f'Error occurred: {str(e)}')
        raise Exception('No se ha podido ejecutar workflow.')

