"""
El propósito de este bot es automatizar la generación de señales de trading basadas en la estrategia de trading logic_1.

"""
import pandas as pd
import datetime
import dates.dates_2 as dt3
import dates.dates_5 as dt2
from indicators import ema
import strategies.intradia as logic
import time
import requests
from tlgm_m import tlgm_sms_ as tl
import rp_grafics_entry as rp
import pytz

response = requests.get("https://api.binance.com/api/v3/ping", timeout=40)  # Increase timeout to 30 seconds

def load_data_with_retry(pair, max_retries=1000):
    retries = 0
    while retries < max_retries:
        try:
            # Intentar cargar los archivos CSV
            df_5min = pd.read_csv(f'{pair}_5.csv')
            df_1min = pd.read_csv(f'{pair}_1.csv')

            # Comprobar si ambos DataFrames tienen datos
            if not df_5min.empty and not df_1min.empty:
                # Verificar columnas específicas que indican que los datos están completos
                if 'Date' in df_5min.columns and 'Date' in df_1min.columns:
                    #print("Archivos cargados correctamente.")
                    return df_5min, df_1min
            else:
                print("Uno o ambos archivos están vacíos. Reintentando...")
                
        except Exception as e:
            print(f"Error al cargar los archivos: {e}")
        
        retries += 1

    raise Exception("No se pudo cargar los archivos después de varios intentos.")

# Ejemplo de uso dentro de la función `logics`
def logics(pair, fibonacci_retracement_long, fibonacci_retracement_short, min_velas_entrada, max_velas_entrada, extension_tp, extension_sl):
    """
    Función que actualiza el DataFrame con nuevas señales basadas en la estrategia.

    Args:
        df (pd.DataFrame): DataFrame con datos de mercado.

    Returns:
        pd.DataFrame: DataFrame actualizado con la columna 'entry'.
    """
    # descargar los datos
    local_timezone = pytz.timezone('America/Argentina/Buenos_Aires')
    df_5min, df_1min = dt2.descarga_datos_en_tiempo_real(API_KEY, API_SECRET, symbol=pair, temporalidad_mayor='5m', temporalidad_menor='1m', timezone=local_timezone, archivo_mayor=f'{pair}_5.csv', archivo_menor=f'{pair}_1.csv')
    # Cargar datos con la nueva lógica de reintento
    df_5min, df_1min = load_data_with_retry(pair)
    
    # Calculamos los indicadores
    df_5min = ema.calcular_ema(df_5min, length_short=10, length_long=55)
    # Aplicamos la lógica de la estrategia
    df_5min = logic.eject_logic(df_5min, df_1min, fibonacci_retracement_long, fibonacci_retracement_short, min_velas_entrada, max_velas_entrada, extension_tp, extension_sl, timeframe='5min')
    
    # Resumimos la señal
    signal = pd.DataFrame({
        'Date': [df_5min['Date'].iloc[-2]],
        'pair': [pair],  # Par de divisas
        'Close': [df_5min['Close'].iloc[-2]],
        'High': [df_5min['High'].iloc[-2]],
        'Low': [df_5min['Low'].iloc[-2]],
        'entry': [df_5min['entrys'].iloc[-2]],
        'date_entry': [None],
        'transition': [df_5min['transition'].iloc[-2]],
        'price_entry_long': [df_5min['price_entry_long'].iloc[-2] if df_5min['price_entry_long'].iloc[-2] != None else 0.0],
        'price_entry_short': [df_5min['price_entry_short'].iloc[-2] if df_5min['price_entry_short'].iloc[-2] != None else 0.0],
        'take_profit': [0.0],
        'stop_loss': [0.0],
        'time_detection': [datetime.datetime.now()],
        'status': [False],
        'Close time': [df_5min['Close time'].iloc[-2]]
    })
    df_5min.to_csv('hma_avax_bckt_10.csv')
    return (signal, df_5min, df_1min)

def wait_for_price(df, pair, date, target_price, API_KEY, API_SECRET,transition, fibonacci_retracement_long, fibonacci_retracement_short, min_velas_entrada, max_velas_entrada, extension_tp, extension_sl, chat_id, token, mensaje_id, temporalidad_g, limit_dates, image_name, apalancamiento):
   
    timeout_seconds = timeout_minutes * 60
    start_time = time.time()

    while time.time() - start_time < timeout_seconds:
        try:
            newsignal, originframe, df_1min = logics(pair,fibonacci_retracement_long, fibonacci_retracement_short, min_velas_entrada, max_velas_entrada, extension_tp, extension_sl)
            # Asegúrate de que la columna 'Date' esté en formato datetime
            originframe['Date'] = pd.to_datetime(originframe['Date'], format='%Y-%m-%d %H:%M:%S', errors='coerce')

            # Filtra el DataFrame para encontrar el índice de la fila con la fecha específica
            index = originframe.index[originframe['Date'] == date].tolist()

            # Verifica si se ha encontrado la fila correcta
            if len(index) > 0:
                fila_index = index[0]
                entry = originframe.iloc[fila_index]['entrys']
                take_profit = originframe.iloc[fila_index]['static_tp']
                stop_loss = originframe.iloc[fila_index]['static_sl']
                entry_dates = originframe.iloc[fila_index]['entry_dates_candle']
                #print(f"Fila encontrada con datos correctos: {originframe.iloc[fila_index]}")
            else:
                print("No se encontró ninguna fila con la fecha especificada.")
            # revisar transición actual para detectar cambio de transición entry_dates_candle
            new_transition = newsignal['transition'].iloc[-1]

            # Obtener la fecha y hora actual
            now = datetime.datetime.now()

            # Formatear solo la hora y los minutos
            hora_minutos = now.strftime('%H:%M:%S')
            int(entry)
            float(take_profit)
            float(stop_loss)
            
            if new_transition != transition and new_transition != None and new_transition != 'None' and new_transition != 'NaN':
                print(f'Se detectó una nueva transición a {new_transition}, no se realizó la entrada...')
                return False
            
            elif int(entry) == 1 and transition == 'transition_long':
                print(f"Entrada Long alcanzada: {target_price} hora: {datetime.datetime.now()}")
                # Actualizamos el dataframe con la nueva señal
                df.loc[df.index[-1], 'take_profit'] = take_profit
                df.loc[df.index[-1], 'stop_loss'] = stop_loss
                df.loc[df.index[-1], 'chat_id'] = chat_id
                df.loc[df.index[-1], 'message_id'] = mensaje_id
                df.loc[df.index[-1], 'token'] = token
                df.loc[df.index[-1], 'date_entry'] = entry_dates
                # Enviar imagen de la entrada
                take_profit2 = abs(((float(take_profit) - float(target_price)) / float(target_price)) * 100)
                stop_loss2 = abs(((float(stop_loss) - float(target_price)) / float(target_price)) * 100)
                dataf = df_1min # Selecciona las últimas 100 filas para generar la imagen
                ruta_imagen = rp.rp(dataf, pair, entry_dates,target_price, image_name)
                mensaje_respuesta = f"📊 Señal de Trading Activa 📊 \n\nMercado: Futuros \nActivo: {pair} \nOperación: LONG \nEntrada: {target_price} \nTake Profit (TP) precio: {round(take_profit,8)} \nStop Loss (SL) precio: {round(stop_loss,8)} \nTake Profit (TP%): {round(take_profit2,2)}% \nStop Loss (SL%): {round(stop_loss2,2)}% \nApalancamiento sugerido: {apalancamiento} \nModo: Aislado \n⏳ Hora de detección: {entry_dates}\n⏳ Hora de envío: {hora_minutos}\n\n⚠ IMPORTANTE: Las señales son resultado de un análisis técnico, pero no garantizan resultados.El uso de esta información es bajo su propio riesgo."
                tl.responder_con_mensaje_e_imagen(token, chat_id, mensaje_id, ruta_imagen, caption=mensaje_respuesta)
                tl.enviar_imagen_telegram(token, chat_id='-1002357427460', ruta_imagen=ruta_imagen, caption=mensaje_respuesta)
                return True
            
            elif int(entry) == 1 and transition == 'transition_short':
                print(f"Entrada Short alcanzada: {target_price} hora: {datetime.datetime.now()}")
                # Actualizamos el dataframe con la nueva señal
                df.loc[df.index[-1], 'take_profit'] = take_profit
                df.loc[df.index[-1], 'stop_loss'] = stop_loss
                df.loc[df.index[-1], 'chat_id'] = chat_id
                df.loc[df.index[-1], 'message_id'] = mensaje_id
                df.loc[df.index[-1], 'token'] = token
                df.loc[df.index[-1], 'date_entry'] = entry_dates
                # Enviar imagen de la entrada date_entry
                dataf = df_1min # Selecciona las últimas 100 filas para generar la imagen
                ruta_imagen = rp.rp(dataf, pair, entry_dates,target_price, image_name)
                take_profit2 = abs(((float(take_profit) - float(target_price)) / float(target_price)) * 100)
                stop_loss2 = abs(((float(stop_loss) - float(target_price)) / float(target_price)) * 100)
                mensaje_respuesta = f"📊 Señal de Trading Activa 📊 \n\nMercado: Futuros \nActivo: {pair} \nOperación: SHORT \nEntrada: {target_price} \nTake Profit (TP) precio: {round(take_profit,8)} \nStop Loss (SL) precio: {round(stop_loss,8)} \nTake Profit (TP%): {round(take_profit2,2)}% \nStop Loss (SL%): {round(stop_loss2,2)}% \nApalancamiento sugerido: {apalancamiento} \nModo: Aislado \n⏳ Hora de detección: {entry_dates}\n⏳ Hora de envío: {hora_minutos}\n\n⚠ IMPORTANTE: Las señales son resultado de un análisis técnico, pero no garantizan resultados.El uso de esta información es bajo su propio riesgo."
                tl.responder_con_mensaje_e_imagen(token, chat_id, mensaje_id, ruta_imagen=ruta_imagen, caption=mensaje_respuesta)
                tl.enviar_imagen_telegram(token, chat_id='-1002357427460', ruta_imagen=ruta_imagen, caption=mensaje_respuesta)
                return True

            time.sleep(2)    

        except Exception as e:
            print(f"Error al obtener el precio actual: {e}")
            time.sleep(0.5) # Espera 1 segundo en caso de error
            continue #Continua al siguiente ciclo del bucle

    # Aquí enviamos un mensaje para abortar la entrada ya que se pasó el tiempo límite.
    print(f"*Tiempo de espera agotado.*\n No se alcanzó el precio objetivo de {target_price}")
    return False

def trading_loop(pair, API_KEY, API_SECRET, image_name, limit_dates, temporalidad_g, fibonacci_retracement_long, fibonacci_retracement_short, min_velas_entrada, max_velas_entrada, extension_tp, extension_sl, chat_id, token, mensaje_id, apalancamiento, timeout_minutes):
    last_transition = None  # Variable para almacenar la última transición detectada

    while True:
        df, origindf, df_1min = logics(pair,fibonacci_retracement_long, fibonacci_retracement_short, min_velas_entrada, max_velas_entrada, extension_tp, extension_sl)
        transition = df['transition'].iloc[-1]
        entry = df['entry'].iloc[-1]
        date = df['Date'].iloc[-1]
        # Verificar si la transición ha cambiado
        if transition != last_transition and transition != 'None' and transition is not None and transition != 'NaN' and entry == 0: #Añado comprobaciones para evitar errores.
            print(df)
            print(f"Nueva señal detectada en: {df['Date'].iloc[-1]}, Transición: {transition}")
            last_transition = transition # Actualiza la última transición

            if transition == 'transition_long':
                target_price = df['price_entry_long'].iloc[-1]

            elif transition == 'transition_short':
                target_price = df['price_entry_short'].iloc[-1]

            # Espera a que se alcance el precio objetivo.
            if wait_for_price(df, pair, date, target_price, API_KEY, API_SECRET,transition, fibonacci_retracement_long, fibonacci_retracement_short, min_velas_entrada, max_velas_entrada, extension_tp, extension_sl, chat_id, token, mensaje_id, temporalidad_g, limit_dates, image_name, apalancamiento):
                file = 'signal_logic_3.csv' # Aquí está todo lo necesario para enviar al chat correcto 
                new_df = pd.read_csv(file)
                new_df = add_row_to_dataframe(new_df, df)
                new_df.to_csv(file, index=False)
                dfff, origindf, df_1min = logics(pair,fibonacci_retracement_long, fibonacci_retracement_short, min_velas_entrada, max_velas_entrada, extension_tp, extension_sl) #Recalcula el df para evitar errores
                print("Orden ejecutada exitosamente. Buscando nuevas señales...")

            else:
                print("Fallo en la ejecucion de la orden.")
                dfff, origindf, df_1min = logics(pair,fibonacci_retracement_long, fibonacci_retracement_short, min_velas_entrada, max_velas_entrada, extension_tp, extension_sl) #Recalcula el df para evitar errores
                continue # Continua al siguiente ciclo del bucle
        time.sleep(2)     
def add_row_to_dataframe(existing_df, new_row_dict):
    """Adds a new row (as a dictionary) to an existing DataFrame."""

    new_row_df = new_row_dict.dropna(how='all')# Filtrar las entradas vacías o todas `NA` antes de concatenar
    updated_df = pd.concat([existing_df, new_row_df], ignore_index=True)
    return updated_df

# Ejemplo de uso:
if __name__ == '__main__':

    print('BOT DE SEÑALES DE INTRADIA')
    token = input("COLCAR TOKEN DEL BOT: ")
    chat_id = input("COLOCAR CHAT ID: ")
    API_KEY = input("COLOCAR API KEY: ")
    API_SECRET = input("COLOCAR API SECRET: ")
    pair = input('Escribe el par para analizar las señales: ')
    timeout_minutes = 4000000 # ilimitado
    image_name = pair + "11"
    limit_dates = 50 # Ya no se está usando
    temporalidad_g = '1m'
    fibonacci_retracement_long = 0.6
    fibonacci_retracement_short = 0.35
    min_velas_entrada = 65
    max_velas_entrada = 9999
    extension_tp = 1.336
    extension_sl = 2.636
    mensaje_id = 7
    apalancamiento = 3
    trading_loop(pair, API_KEY, API_SECRET, image_name, limit_dates, temporalidad_g, fibonacci_retracement_long, fibonacci_retracement_short, min_velas_entrada, max_velas_entrada, extension_tp, extension_sl, chat_id, token, mensaje_id, apalancamiento, timeout_minutes)
