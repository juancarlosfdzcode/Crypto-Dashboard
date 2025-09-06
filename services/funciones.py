import pandas as pd

def processTokenData(token_data, coin_name):
    """
    Convierte los datos de un token a DataFrame
    Args:
        token_data (dict): Datos del token desde la API
        coin_name (str): Nombre de la moneda
    Returns:
        pandas.DataFrame: DataFrame procesado
    """
    df = pd.DataFrame({
        'coin': coin_name,
        'timestamp': [x[0] for x in token_data['prices']],
        'price': [x[1] for x in token_data['prices']],
        'market_cap': [x[1] for x in token_data['market_caps']],
        'volume': [x[1] for x in token_data['total_volumes']]
    })
    
    df['datetime'] = pd.to_datetime(df['timestamp'], unit='ms')
    
    return df