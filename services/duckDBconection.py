import duckdb

from coinGeckoDataExtraction import dataExtraction

df_crypto = dataExtraction()

conn = duckdb.connect('crypto_database.db')

conn.register('crypto_temp', df_crypto)

conn.execute("""
    CREATE TABLE IF NOT EXISTS crypto_data AS
    SELECT * FROM crypto_temp
""")

conn.close()