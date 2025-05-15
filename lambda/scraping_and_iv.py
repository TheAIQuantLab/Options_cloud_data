import requests
from bs4 import BeautifulSoup
import pandas as pd
import time
import math
from scipy.stats import norm
from scipy.optimize import brentq
import boto3
import os

ACCESS_KEY = os.environ['ACCESS_KEY']
SECRET_KEY = os.environ['SECRET_KEY']

def parse_tipo(tipo):
    if len(tipo) != 11 or not tipo.startswith("O"):
        raise ValueError("Invalid tipo format")
    type_CP = "Call" if tipo[1] == "C" else "Put" if tipo[1] == "P" else "Unknown"
    type_EA = "European" if tipo[2] == "E" else "American" if tipo[2] == "A" else "Unknown"
    raw_date = tipo[3:]
    formatted_date = f"{raw_date[6:8]}-{raw_date[4:6]}-{raw_date[0:4]}"
    return {
        "type_CP": type_CP,
        "type_EA": type_EA,
        "expiration_date": formatted_date
    }

def calculate_T(expiration_date, today_str):
    today = pd.to_datetime(today_str, format="%d-%m-%Y")
    expiration = pd.to_datetime(expiration_date, format="%d-%m-%Y")
    return (expiration - today).days / 365.0

def black_scholes_price(S, K, T, r, sigma, option_type="call"):
    if T <= 0 or sigma <= 0:
        return 0.0
    d1 = (math.log(S / K) + (r + 0.5 * sigma**2) * T) / (sigma * math.sqrt(T))
    d2 = d1 - sigma * math.sqrt(T)
    if option_type == "call":
        return S * norm.cdf(d1) - K * math.exp(-r * T) * norm.cdf(d2)
    elif option_type == "put":
        return K * math.exp(-r * T) * norm.cdf(-d2) - S * norm.cdf(-d1)

def implied_volatility(S, K, T, r, market_price, option_type="call"):
    if T == 0 or market_price == 0:
        print("Invalid parameters for IV calculation")
        return float('nan')
    try:
        return brentq(
            lambda sigma: black_scholes_price(S, K, T, r, sigma, option_type) - market_price, 0, 10
        )
    except ValueError:
        print("No solution found for IV")
        return float('nan')

def scrape_meff_data():
    url = "https://www.meff.es/esp/Derivados-Financieros/Ficha/FIEM_MiniIbex_35"
    headers = {
        "User-Agent": "Mozilla/5.0"
    }
    response = requests.get(url, headers=headers)
    soup = BeautifulSoup(response.content, "html.parser")

    # Extract today's price
    table_prices = soup.find(id="Contenido_Contenido_tblFuturos")
    row_todays_price = table_prices.find_all("tr")[2]
    price_today = row_todays_price.find_all("td")[13].get_text(strip=True)

    # Extract options table
    table = soup.find(id="tblOpciones")
    rows = table.find_all("tr")

    date_today = time.strftime("%d-%m-%Y")
    data = []

    for row in rows:
        tipo = row.get("data-tipo")
        if tipo:
            parsed = parse_tipo(tipo)
            cols = [date_today, price_today, parsed["type_CP"], parsed["type_EA"], parsed["expiration_date"]]
            cols.extend(col.get_text(strip=True) for col in row.find_all("td"))
            if cols[0]:
                T = calculate_T(parsed["expiration_date"], date_today)
                try:
                    K = float(cols[5].replace(".", "").replace(",", "."))
                    S = float(price_today.replace(".", "").replace(",", "."))
                    option_price = float(cols[17].replace(".", "").replace(",", "."))
                    option_type = "call" if parsed["type_CP"].lower() == "call" else "put"
                    r = 0.03
                    print(f"Calculating IV for K={K}, S={S}, T={T}, r={r}, option_price={option_price}, option_type={option_type}")
                    iv = implied_volatility(S, K, T, r, option_price, option_type)
                    print(f"IV: {iv}")
                except Exception:
                    iv = float('nan')
                cols.append(T)
                cols.append(iv)
                data.append(cols)

    df = pd.DataFrame(data, columns=[
        "execution_date", "price_today", "type_CP", "type_EA", "expiration_date",
        "strike_price", "colCompra1", "colCompra2", "colCompra3",
        "colVenta1", "colVenta2", "colVenta3",
        "colUltimo", "colVar", "x1", "x2", "x3", "last_option_price", "T", "IV"
    ])

    df = df.drop(columns=[
        "colCompra1", "colCompra2", "colCompra3",
        "colVenta1", "colVenta2", "colVenta3",
        "colUltimo", "colVar", "x1", "x2", "x3"
    ])

    df["id"] = df.apply(
        lambda row: f"{row['execution_date']}_{row['expiration_date']}_{row['type_CP']}_{row['type_EA']}_{row['strike_price']}",
        axis=1
    )

    return df

def save_df_to_dynamodb(df, table):
    for _, row in df.iterrows():
        item = {col: str(row[col]) for col in df.columns}
        try:
            table.put_item(Item=item)
        except Exception as e:
            print(f"Failed to insert row: {row.to_dict()}, Error: {str(e)}")

def lambda_handler(event, context):
    df = scrape_meff_data()

    dynamodb = boto3.resource(
        'dynamodb',
        region_name='eu-north-1',
        aws_access_key_id=ACCESS_KEY,
        aws_secret_access_key=SECRET_KEY
    )
    table = dynamodb.Table('meff_options')

    save_df_to_dynamodb(df, table)
    return {
        'statusCode': 200,
        'body': f'{len(df)} items saved to DynamoDB'
    }

if __name__ == "__main__":
    df = scrape_meff_data()
    print(df)

    dynamodb = boto3.resource(
        'dynamodb',
        region_name='eu-north-1',
        aws_access_key_id=ACCESS_KEY,
        aws_secret_access_key=SECRET_KEY
    )
    table = dynamodb.Table('meff_options')

    save_df_to_dynamodb(df, table)
    print("Data saved to DynamoDB")
