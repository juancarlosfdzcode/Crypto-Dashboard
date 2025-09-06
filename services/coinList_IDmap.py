import requests
import pandas as pd

url = "https://api.coingecko.com/api/v3/coins/list/?include_platform=true"
headers = {
    "accept": "application/json",
    "x-cg-api-key": "CG-VuAVgyXDs56NP5n2baFoJkdW"
}

response = requests.get(url, headers=headers)
data = response.json()
df = pd.DataFrame(data)

coins = ['aave', 'cronos', 'chainlink']

filtered_df = df[df['name'].str.lower().isin([coin.lower() for coin in coins])]
for _, row in filtered_df.iterrows():
    print(f"{row['name']} : {row['id']}")