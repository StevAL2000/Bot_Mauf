from binance.client import Client
import pandas as pd
import datetime
import time
from requests.exceptions import RequestException, ConnectTimeout
import pytz
import os
import shutil

def crear_cliente_con_reintentos(api_key, api_secret, max_reintentos=500, retraso_reintento=1):
    for intento in range(max_reintentos):
        try:
            client = Client(api_key, api_secret, {"timeout": 40})
            client.ping()
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

    fecha_penultima_mayor = data_mayor['Date'].iloc[-2]
    fecha_penultima_menor = data_menor['Date'].iloc[-2]

    tiempo_actual_servidor = datetime.datetime.now(pytz.utc).astimezone(timezone)
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

    columnas = ['Open time', 'Open', 'High', 'Low', 'Close', 'Volume', 'Close time', 'Quote asset volume', 
                'Number of trades', 'Taker buy base asset volume', 'Taker buy quote asset volume', 'Ignore']
    
    nuevas_data_mayor = pd.DataFrame(nuevas_klines_mayor, columns=columnas)
    nuevas_data_menor = pd.DataFrame(nuevas_klines_menor, columns=columnas)

    for col in ['Open', 'High', 'Low', 'Close']:
        nuevas_data_mayor[col] = nuevas_data_mayor[col].astype(float)
        nuevas_data_menor[col] = nuevas_data_menor[col].astype(float)

    nuevas_data_mayor['Date'] = pd.to_datetime(nuevas_data_mayor['Open time'], unit='ms', utc=True).dt.tz_convert(timezone)
    nuevas_data_menor['Date'] = pd.to_datetime(nuevas_data_menor['Open time'], unit='ms', utc=True).dt.tz_convert(timezone)
    nuevas_data_mayor['Close time'] = pd.to_datetime(nuevas_data_mayor['Close time'], unit='ms', utc=True).dt.tz_convert(timezone)
    nuevas_data_menor['Close time'] = pd.to_datetime(nuevas_data_menor['Close time'], unit='ms', utc=True).dt.tz_convert(timezone)

    # === Modificación clave: Filtrar vela incompleta de 1 minuto ===
    tiempo_actual = datetime.datetime.now(pytz.utc).astimezone(timezone)
    
    # Calcular timestamp alineado para 1 minuto
    if temporalidad_menor.endswith('m'):
        minutos = int(temporalidad_menor[:-1])
        aligned_time = tiempo_actual.replace(
            minute=(tiempo_actual.minute // minutos) * minutos,
            second=0,
            microsecond=0
        )
    
    # Eliminar vela actual de 1 minuto si existe
    nuevas_data_menor = nuevas_data_menor[
        nuevas_data_menor['Date'] != aligned_time
    ]

    nuevas_data_mayor = nuevas_data_mayor[['Date', 'Open', 'High', 'Low', 'Close', 'Volume', 'Close time']]
    nuevas_data_menor = nuevas_data_menor[['Date', 'Open', 'High', 'Low', 'Close', 'Volume', 'Close time']]

    data_mayor = data_mayor[:-1]
    data_menor = data_menor[:-1]

    data_mayor = pd.concat([data_mayor, nuevas_data_mayor]).drop_duplicates(subset=['Date']).tail(limit_myr)
    data_menor = pd.concat([data_menor, nuevas_data_menor]).drop_duplicates(subset=['Date']).tail(limit_mnr)

    # Mecanismo de backup mejorado
    try:
        if os.path.exists(archivo_mayor):
            shutil.copy(archivo_mayor, f"{archivo_mayor}.backup")
        if os.path.exists(archivo_menor):
            shutil.copy(archivo_menor, f"{archivo_menor}.backup")
        
        data_mayor.to_csv(archivo_mayor, index=False)
        data_menor.to_csv(archivo_menor, index=False)
        
        # Eliminar backups si todo fue exitoso
        if os.path.exists(f"{archivo_mayor}.backup"):
            os.remove(f"{archivo_mayor}.backup")
        if os.path.exists(f"{archivo_menor}.backup"):
            os.remove(f"{archivo_menor}.backup")
            
    except Exception as e:
        print(f"Error crítico al guardar datos: {e}")
        # Restaurar backups en caso de error
        if os.path.exists(f"{archivo_mayor}.backup"):
            shutil.move(f"{archivo_mayor}.backup", archivo_mayor)
        if os.path.exists(f"{archivo_menor}.backup"):
            shutil.move(f"{archivo_menor}.backup", archivo_menor)
        return None, None

    return data_mayor, data_menor

# Ejemplo de uso
if __name__ == '__main__':
    print('Grafico en tiempo real')
    API_KEY = "TU_API_KEY"
    API_SECRET = "TU_API_SECRET"
    local_timezone = pytz.timezone('America/Argentina/Buenos_Aires')
    imput = input('Introduce el par que vamos a descargar: ')
    
    while True:
        resultado = descarga_datos_en_tiempo_real(
            API_KEY, API_SECRET, 
            symbol=imput, 
            temporalidad_mayor='5m', 
            temporalidad_menor='1m', 
            timezone=local_timezone, 
            archivo_mayor=f'{imput}_5.csv', 
            archivo_menor=f'{imput}_1.csv'
        )
        
        if resultado is not None:
            df_mayor, df_menor = resultado
            print(df_menor)
            print("Datos actualizados correctamente")
            print(f"Última actualización: {datetime.datetime.now()}")
        else:
            print("Error en la actualización, reintentando...")
        
        time.sleep(60)  # Espera 1 minuto entre actualizaciones