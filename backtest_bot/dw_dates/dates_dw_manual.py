import requests
import pandas as pd
import time
import datetime
import pytz

def descargar_historial_binance_futures(symbol, timeframe, start_date, end_date, filename="binance_futures_data.csv", timezone=None):
    """
    Descarga el historial de datos de futuros de Binance en formato CSV.

    Args:
        symbol (str): Símbolo del par (ej. 'BTCUSDT').
        timeframe (str): Temporalidad (ej. '1m', '5m', '1h', '1d').
        start_date (str): Fecha de inicio en formato 'YYYY-MM-DD'.
        end_date (str): Fecha de fin en formato 'YYYY-MM-DD'.
        filename (str, optional): Nombre del archivo CSV. Por defecto es "binance_futures_data.csv".
        timezone (str, optional): La zona horaria deseada para los datos (ej. 'America/New_York', 'Europe/Madrid'). Por defecto es None (UTC).
    """

    url = "https://fapi.binance.com/fapi/v1/klines"
    start_ts = int(datetime.datetime.strptime(start_date, "%Y-%m-%d %H:%M:%S").timestamp()) * 1000
    end_ts = int(datetime.datetime.strptime(end_date, "%Y-%m-%d %H:%M:%S").timestamp()) * 1000
    interval = timeframe

    all_data = []
    current_ts = start_ts
    retries = 5  # Número de reintentos

    while current_ts < end_ts:
        limit = 1500  # Máximo permitido por Binance
        params = {
            "symbol": symbol,
            "interval": interval,
            "startTime": current_ts,
            "limit": limit
        }
        if (current_ts + limit * milliseconds(interval)) > end_ts:
            params["endTime"] = end_ts

        for attempt in range(retries):
            try:
                response = requests.get(url, params=params)
                response.raise_for_status()  # Lanza una excepción para códigos de error HTTP
                data = response.json()
                if not data:  # Si data está vacío, se rompe el bucle.
                    break
                all_data.extend(data)
                current_ts = data[-1][0] + milliseconds(interval)  # Avanzamos al siguiente tiempo
                time.sleep(1.5)  # Espera 1.5 segundos para no saturar las solicitudes
                break  # Salimos del bucle de reintentos si la solicitud tuvo éxito
            except requests.exceptions.RequestException as e:
                print(f"Error en la solicitud (intento {attempt + 1}/{retries}): {e}")
                time.sleep(2 ** attempt)  # Espera exponencial antes de reintentar
                if attempt == retries - 1:
                    print("Máximo número de reintentos alcanzado. Abortando.")
                    return

    df = pd.DataFrame(all_data)
    df.columns = [
        "Date", "Open", "High", "Low", "Close", "Volume",
        "Close time", "Quote asset volume", "Number of trades",
        "Taker buy base asset volume", "Taker buy quote asset volume", "Ignore"
    ]

    df["Date"] = pd.to_datetime(df["Date"], unit="ms")
    df["Close time"] = pd.to_datetime(df["Close time"], unit="ms")

    if timezone:
        try:
            df["Date"] = df["Date"].dt.tz_localize('UTC').dt.tz_convert(timezone)
            df["Close time"] = df["Close time"].dt.tz_localize('UTC').dt.tz_convert(timezone)
        except pytz.exceptions.UnknownTimeZoneError:
            print(f"Zona horaria desconocida: {timezone}. Usando UTC.")
    
    numeric_cols = ["Open", "High", "Low", "Close", "Volume", "Quote asset volume", "Taker buy base asset volume", "Taker buy quote asset volume"]
    for col in numeric_cols:
        df[col] = pd.to_numeric(df[col])

    df.to_csv(filename, index=False)
    print(f"Datos guardados en {filename}")

def milliseconds(interval):
    units = {"m": 60000, "h": 3600000, "d": 86400000, "w": 604800000, "M": 2592000000}
    num = int(interval[:-1])
    unit = interval[-1]
    return num * units[unit]

if __name__ == "__main__":
    print('Descargas Iniciadas')    
    # Ejemplo de uso 1
    symbol = "ETHUSDT"  # Par de futuros
    timeframe = "5m"      # Temporalidad (5 minutos)
    start_date = "2025-02-21 00:00:00"  # Fecha de inicio
    end_date = "2025-04-22 13:00:00"    # Fecha de fin-
    filename = 'ETHUSDT_5_60dnew.csv'

    # Ejemplo con zona horaria de Buenos Aires
    descargar_historial_binance_futures(symbol, timeframe, start_date, end_date, filename, timezone='America/Argentina/Buenos_Aires')
    timeframe = "1m"      # Temporalidad (5 minutos)
    filename = 'ETHUSDT_1_60dnew.csv'
    # Ejemplo con zona horaria de Buenos Aires
    descargar_historial_binance_futures(symbol, timeframe, start_date, end_date, filename, timezone='America/Argentina/Buenos_Aires')

    # Ejemplo con zona horaria de Nueva York
    #descargar_historial_binance_futures(symbol, timeframe, start_date, end_date, filename, timezone='America/New_York')

    # Ejemplo sin especificar zona horaria (se usará UTC)
    #descargar_historial_binance_futures(symbol, timeframe, start_date, end_date, filename)
    print('Descargas finalizadas')