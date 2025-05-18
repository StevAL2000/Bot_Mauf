from binance.client import Client
import pandas as pd
import datetime
import time
from requests.exceptions import RequestException, ConnectTimeout
import pytz

'''
ESTE SCRIPT OBTIENE DATOS DE FORMA LIMITADA PARA CUALQUIER ACTIVO, PERMITIENDO REDUCIR EL TAMAÑO DE LAS SOLICITUDES A LA API
DE BINANCE, SE PUEDE EJECUTAR EN TIEMPO REAL EN UN MODULO APARTE PARA QUE TRABAJE EN PARALELO A LA LÓGICA DE DETECCIÓN DE UNA ESTRATEGIA
'''

def crear_cliente_con_reintentos(api_key, api_secret, max_reintentos=500, retraso_reintento=1):
    for intento in range(max_reintentos):
        try:
            client = Client(api_key, api_secret, {"timeout": 40})
            client.ping()  # Verificar la conexión
            return client
        except (RequestException, ConnectTimeout) as e:
            print(f"Intento {intento + 1}/{max_reintentos}: Error al inicializar el cliente: {e}")
            if intento < max_reintentos - 1:
                time.sleep(retraso_reintento)
            else:
                print(f"Fallo al inicializar el cliente después de {max_reintentos} intentos.")
                return None
        except Exception as e:
            print(f"Intento {intento + 1}/{max_reintentos}: Error inesperado al inicializar el cliente: {e}")
            if intento < max_reintentos - 1:
                time.sleep(retraso_reintento)
            else:
                print(f"Fallo al inicializar el cliente después de {max_reintentos} intentos.")
                return None
    return None

def descarga_datos_en_tiempo_real(API_KEY, API_SECRET, symbol, temporalidad_mayor, temporalidad_menor, timezone=None, archivo_mayor='data_mayor.csv', archivo_menor='data_menor.csv', limit_myr=400, limit_mnr=2000):
    def descargar_datos_con_reintentos(symbol, interval, limit, start_str, end_str):
        max_reintentos = 600
        retraso_reintento = 1
        for intento in range(max_reintentos):
            try:
                klines = client.futures_klines(symbol=symbol, interval=interval, start_str=start_str, end_str=end_str, limit=limit)
                #print(f"Consultadas {len(klines)} velas para {symbol} en el intervalo {interval}")
                return klines
            except RequestException as e:
                print(f"Intento {intento + 1}/{max_reintentos}: Error al obtener los datos de {symbol}: {e}")
                if intento < max_reintentos - 1:
                    time.sleep(retraso_reintento)
                else:
                    print(f"Fallo al obtener los datos de {symbol} después de {max_reintentos} intentos.")
                    return None
            except Exception as e:
                print(f"Intento {intento + 1}/{max_reintentos}: Error inesperado al obtener los datos de {symbol}: {e}")
                if intento < max_reintentos - 1:
                    time.sleep(retraso_reintento)
                else:
                    print(f"Fallo al obtener los datos de {symbol} después de {max_reintentos} intentos.")
                    return None
        return None

    if timezone is None:
        timezone = pytz.timezone('UTC')
    elif isinstance(timezone, str):
        timezone = pytz.timezone(timezone)
    elif not isinstance(timezone, datetime.tzinfo):
        raise TypeError("timezone debe ser una cadena o un objeto tzinfo")

    client = crear_cliente_con_reintentos(API_KEY, API_SECRET)
    if client is None:
        print("No se pudo inicializar el cliente de Binance.")
        return None, None

    interval_map = {
        '1m': Client.KLINE_INTERVAL_1MINUTE,
        '3m': Client.KLINE_INTERVAL_3MINUTE,
        '5m': Client.KLINE_INTERVAL_5MINUTE,
        '15m': Client.KLINE_INTERVAL_15MINUTE,
        '30m': Client.KLINE_INTERVAL_30MINUTE,
        '1h': Client.KLINE_INTERVAL_1HOUR,
        '4h': Client.KLINE_INTERVAL_4HOUR,
        '1d': Client.KLINE_INTERVAL_1DAY
    }

    interval_mayor = interval_map.get(temporalidad_mayor)
    interval_menor = interval_map.get(temporalidad_menor)

    if interval_mayor is None:
        raise ValueError(f"Temporalidad mayor no válida: {temporalidad_mayor}")
    if interval_menor is None:
        raise ValueError(f"Temporalidad menor no válida: {temporalidad_menor}")

    while True:
        try:
            data_mayor = pd.read_csv(archivo_mayor)
            data_menor = pd.read_csv(archivo_menor)
        except FileNotFoundError:
            print(f"Archivos {archivo_mayor} o {archivo_menor} no encontrados.")
            return None, None

        data_mayor['Date'] = pd.to_datetime(data_mayor['Date'])
        data_menor['Date'] = pd.to_datetime(data_menor['Date'])

        fecha_mas_reciente_mayor = data_mayor['Date'].max()
        fecha_mas_reciente_menor = data_menor['Date'].max()
        fecha_mas_reciente = max(fecha_mas_reciente_mayor, fecha_mas_reciente_menor)

        if pd.isna(fecha_mas_reciente):
            print("No se encontraron fechas válidas en los archivos CSV.")
            return None, None

        # Usar la penúltima vela como fecha de inicio
        fecha_penultima_mayor = data_mayor['Date'].iloc[-2]
        fecha_penultima_menor = data_menor['Date'].iloc[-2]

        tiempo_actual_servidor = datetime.datetime.now(pytz.utc).astimezone(timezone)
        #print(f"Tiempo actual del servidor: {tiempo_actual_servidor}")

        start_date = fecha_penultima_mayor.strftime("%d %b, %Y %H:%M:%S")
        end_date = tiempo_actual_servidor.strftime("%d %b, %Y %H:%M:%S") 

        limit_mayor = (int((tiempo_actual_servidor - fecha_penultima_mayor).total_seconds()) // (int(temporalidad_mayor[:-1]) * 60)) + 1
        if limit_mayor > limit_myr:
            limit_mayor = limit_myr

        limit_menor = (int((tiempo_actual_servidor - fecha_penultima_menor).total_seconds()) // (int(temporalidad_menor[:-1]) * 60)) + 1
        if limit_menor > limit_mnr:
            limit_menor = limit_mnr

        nuevas_klines_mayor = descargar_datos_con_reintentos(symbol, interval_mayor, limit_mayor, start_date, end_date)
        nuevas_klines_menor = descargar_datos_con_reintentos(symbol, interval_menor, limit_menor, start_date, end_date)

        columnas = ['Open time', 'Open', 'High', 'Low', 'Close', 'Volume', 'Close time', 'Quote asset volume', 'Number of trades', 'Taker buy base asset volume', 'Taker buy quote asset volume', 'Ignore']
        nuevas_data_mayor = pd.DataFrame(nuevas_klines_mayor, columns=columnas)
        nuevas_data_menor = pd.DataFrame(nuevas_klines_menor, columns=columnas)

        # Convertir las columnas a float
        for col in ['Open', 'High', 'Low', 'Close']:
            nuevas_data_mayor[col] = nuevas_data_mayor[col].astype(float)
            nuevas_data_menor[col] = nuevas_data_menor[col].astype(float)

        nuevas_data_mayor['Date'] = pd.to_datetime(nuevas_data_mayor['Open time'], unit='ms', utc=True).dt.tz_convert(timezone)
        nuevas_data_menor['Date'] = pd.to_datetime(nuevas_data_menor['Open time'], unit='ms', utc=True).dt.tz_convert(timezone)
        nuevas_data_mayor['Close time'] = pd.to_datetime(nuevas_data_mayor['Close time'], unit='ms', utc=True).dt.tz_convert(timezone)
        nuevas_data_menor['Close time'] = pd.to_datetime(nuevas_data_menor['Close time'], unit='ms', utc=True).dt.tz_convert(timezone)

        nuevas_data_mayor = nuevas_data_mayor[['Date', 'Open', 'High', 'Low', 'Close', 'Volume', 'Close time', 'Quote asset volume', 'Number of trades', 'Taker buy base asset volume', 'Taker buy quote asset volume', 'Ignore']]
        nuevas_data_menor = nuevas_data_menor[['Date', 'Open', 'High', 'Low', 'Close', 'Volume', 'Close time', 'Quote asset volume', 'Number of trades', 'Taker buy base asset volume', 'Taker buy quote asset volume', 'Ignore']]

        # Eliminar la última vela (incompleta) antes de actualizar el dataframe[_{{{CITATION{{{_1{](https://github.com/rnarciso/NM_rebalance/tree/4fe0d4d5144512f5952a5bd1ae43c0bdeb2bd7a6/nm%2Futil.py)
        data_mayor = data_mayor[:-1]
        data_menor = data_menor[:-1]

        data_mayor = pd.concat([data_mayor, nuevas_data_mayor]).drop_duplicates(subset=['Date']).tail(limit_myr)
        data_menor = pd.concat([data_menor, nuevas_data_menor]).drop_duplicates(subset=['Date']).tail(limit_mnr)

        data_mayor.to_csv(archivo_mayor, index=False)
        data_menor.to_csv(archivo_menor, index=False)

        # Espera de 1 segundo antes de la próxima consulta
        #time.sleep(180)
        return data_mayor, data_menor

# Ejemplo de uso
if __name__ == '__main__':
    print('Grafico en tiempo real')
    API_KEY = "0PuUx339E8rAhlRmgCtvqGuHEXvinUNY6qloWHDIQ4btyLxnkOKc3JCa2zB6j8t3"
    API_SECRET = "AARVlW9c3lFEdPnPUWujt4b4F6ksWbjnSKzjL4yApWnSvAwyw1ORtTkhXgctPERA"
    local_timezone = pytz.timezone('America/Argentina/Buenos_Aires')
    imput = input('Introduce el par que vamos a descargar: ')
    while True:
        data_mayor, data_menor= descarga_datos_en_tiempo_real(API_KEY, API_SECRET, symbol=imput, temporalidad_mayor='5m', temporalidad_menor='1m', timezone=local_timezone, archivo_mayor=f'{imput}_5.csv', archivo_menor=f'{imput}_1.csv')
        print("\033c", end="")
        print(data_menor)