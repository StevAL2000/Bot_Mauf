from indicators import hma, backtet_hma_2, ema
import pandas as pd
import numpy as np
import datetime
def identificar_transiciones(df, ema_short_col='ema_short', ema_long_col='ema_long'):
    """Identifica las transiciones de cruce de hMAs."""
    df['transition'] = None
    df['velas_posteriores'] = 0

    for i in range(1, len(df)):
        if df[ema_short_col].iloc[i] > df[ema_long_col].iloc[i] and df[ema_short_col].iloc[i-1] <= df[ema_long_col].iloc[i-1]:
            df.loc[i, 'transition'] = 'transition_long'
        elif df[ema_short_col].iloc[i] < df[ema_long_col].iloc[i] and df[ema_short_col].iloc[i-1] >= df[ema_long_col].iloc[i-1]:
            df.loc[i, 'transition'] = 'transition_short'

    for i in range(len(df)):
        if df['transition'].iloc[i] is not None:
            count = 1
            j = i + 1
            while j < len(df) and df['transition'].iloc[j] is None:
                count += 1
                j += 1
            df.loc[i, 'velas_posteriores'] = count
    return df

def calcular_precios_entrada(df, porcentaje_long, porcentaje_short):
    """
    Calcula los precios de entrada para operaciones long y short basándose en las transiciones.

    Args:
        df (pd.DataFrame): DataFrame con los datos, incluyendo la columna 'transition' y 'Close'.
        porcentaje_long (float): Porcentaje a restar al precio de cierre para entradas long (ej. 0.002 para 0.2%).
        porcentaje_short (float): Porcentaje a sumar al precio de cierre para entradas short (ej. 0.002 para 0.2%).

    Returns:
        pd.DataFrame: DataFrame con las columnas 'price_entry_long' y 'price_entry_short' añadidas.
        Devuelve el dataframe original si no existen las columnas necesarias.
    """

    if 'transition' not in df.columns or 'Close' not in df.columns:
        print("Error: El DataFrame debe contener las columnas 'transition' y 'Close'.")
        return df

    df['price_entry_long'] = None
    df['price_entry_short'] = None

    for i in range(len(df)):
        if df['transition'][i] == 'transition_long':
            df.loc[i, 'price_entry_long'] = df['Close'][i] * (1 - porcentaje_long)
        elif df['transition'][i] == 'transition_short':
            df.loc[i, 'price_entry_short'] = df['Close'][i] * (1 + porcentaje_short)

    return df

def calcular_precios_entrada_fibonacci(df, df_1min, fibonacci_retracement_long=0.20, fibonacci_retracement_short=0.20, min_velas_entrada=1):
    """
    Calcula los precios de entrada para operaciones long y short basándose en los retrocesos de Fibonacci y
    la vela del rango mínimo de entrada.

    Args:
        df (pd.DataFrame): DataFrame con los datos, incluyendo la columna 'transition', 'Close', 'Low', 'High' y 'Date'.
        df_1min (pd.DataFrame): DataFrame con la columna 'Date', 'High', 'Low' en temporalidad de 1 minuto.
        fibonacci_retracement_long (float): Nivel de retroceso de Fibonacci para entradas long (ej. 0.20 para 20%).
        fibonacci_retracement_short (float): Nivel de retroceso de Fibonacci para entradas short (ej. 0.20 para 20%).
        min_velas_entrada (int): Número mínimo de velas posteriores a la señal para buscar la entrada.

    Returns:
        pd.DataFrame: DataFrame con las columnas 'price_entry_long' y 'price_entry_short' añadidas.
    """

    if 'transition' not in df.columns or 'Close' not in df.columns or 'Low' not in df.columns or 'High' not in df.columns:
        print("Error: El DataFrame debe contener las columnas 'transition', 'Close', 'Low', 'High' y 'Date'.")
        return df

    df['price_entry_long'] = None
    df['price_entry_short'] = None

    df['Date'] = pd.to_datetime(df['Date'], errors='coerce').dt.tz_localize(None)
    df_1min['Date'] = pd.to_datetime(df_1min['Date'], errors='coerce').dt.tz_localize(None)

    for i in range(len(df)):
        if pd.isna(df['transition'][i]):
            continue  # No hay transición en esta vela

        tipo_transicion = df['transition'][i]

        # Ajustar el intervalo de tiempo para incluir datos anteriores y posteriores a la fecha actual
        end_time_1min = df['Date'][i] + pd.Timedelta(minutes=min_velas_entrada)  # Hasta un minuto después de la fecha actual
        start_time_1min = df['Date'][i] - pd.Timedelta(minutes=(0))

        relevant_data_1min = df_1min[(df_1min['Date'] >= start_time_1min) & (df_1min['Date'] < end_time_1min)]
        
        #print(f"Proceso para la fila {i} con transición {tipo_transicion}:")
        #print(f"start_time_1min: {start_time_1min}, end_time_1min: {end_time_1min}, Date actual: {df['Date'][i]}")
        #print(f"Datos relevantes de 1 minuto:\n{relevant_data_1min}")

        if relevant_data_1min.empty:
            print(f"No hay datos de 1 minuto relevantes para la fila {i}")
            continue

        max_high_price = relevant_data_1min['High'].max()
        min_low_price = relevant_data_1min['Low'].min()

        if tipo_transicion == 'transition_long':
            #print(f"max_high_price para la fila {i}: {max_high_price}")
            #print(f"min_low_price para la fila {i}: {min_low_price}")
            if not pd.isna(max_high_price) and not pd.isna(min_low_price):
                fib_price = min_low_price + (max_high_price - min_low_price) * fibonacci_retracement_long
                df.loc[i, 'price_entry_long'] = fib_price
                #print(f"Precio de entrada long para la fila {i}: {fib_price}")
                #print('-------------------------------------')
                #print('')

        elif tipo_transicion == 'transition_short':
            #print(f"max_high_price para la fila {i}: {max_high_price}")
            #print(f"min_low_price para la fila {i}: {min_low_price}")
            if not pd.isna(max_high_price) and not pd.isna(min_low_price):
                fib_price = max_high_price - (max_high_price - min_low_price) * fibonacci_retracement_short
                df.loc[i, 'price_entry_short'] = fib_price
                #print(f"Precio de entrada short para la fila {i}: {fib_price}")
                #print('-------------------------------------')
                #print('')
    return df







def detectar_entradas(df, df_1min, timeframe='5min', min_velas_entrada=1, max_velas_entrada=2):
    """
    Detecta si se han realizado las entradas long o short DESPUÉS de la vela de la transición,
    con un límite máximo y mínimo de velas para la entrada y guarda la fecha de la vela de entrada.

    Args:
        df (pd.DataFrame): DataFrame con las columnas 'price_entry_long', 'price_entry_short', 'High', 'Low' y 'transition'.
        df_1min (pd.DataFrame): DataFrame con la columna 'Date', 'High' y 'Low' en temporalidad de 1 minuto.
        timeframe (str): Temporalidad del DataFrame df (por ejemplo, '5min', '15min', '30min', '1h').
        min_velas_entrada (int): Número mínimo de velas posteriores a la señal para buscar la entrada.
        max_velas_entrada (int): Número máximo de velas posteriores a la señal para buscar la entrada.

    Returns:
        pd.DataFrame: DataFrame con la columna 'entrys' y 'entry_dates_candle' añadidas.
        Devuelve el dataframe original si no existen las columnas necesarias.
    """

    columnas_requeridas = ['price_entry_long', 'price_entry_short', 'High', 'Low', 'transition', 'Date']
    if not all(col in df.columns for col in columnas_requeridas):
        print(f"Error: El DataFrame debe contener las columnas: {columnas_requeridas}")
        return df

    df['entrys'] = 0
    df['entry_dates_candle'] = pd.NaT  # Inicializamos la columna con NaT
    df['Date'] = pd.to_datetime(df['Date'], errors='coerce').dt.tz_localize(None)
    df_1min['Date'] = pd.to_datetime(df_1min['Date'], errors='coerce').dt.tz_localize(None)

    time_deltas = {
        '5min': 5,
        '15min': 15,
        '30min': 30,
        '1h': 60,
        # Puedes agregar más temporalidades según sea necesario
    }
    timeframe_minutes = time_deltas.get(timeframe, 5)  # Por defecto 5 minutos

    for i in range(len(df)):
        if pd.isna(df['transition'][i]):
            continue  # No hay transición en esta vela

        tipo_transicion = df['transition'][i]
        precio_entrada = None

        if tipo_transicion == 'transition_long' and not pd.isna(df['price_entry_long'][i]):
            precio_entrada = df['price_entry_long'][i]
        elif tipo_transicion == 'transition_short' and not pd.isna(df['price_entry_short'][i]):
            precio_entrada = df['price_entry_short'][i]
        else:
            continue  # No hay precio de entrada para esta transición

        start_time_1min = df['Date'][i] + pd.Timedelta(minutes=timeframe_minutes)

        # Buscar en df_1min desde el inicio de la siguiente vela de la temporalidad especificada
        relevant_data_1min = df_1min[df_1min['Date'] >= start_time_1min]

        if relevant_data_1min.empty:
            continue

        for j in range(min(max_velas_entrada, len(relevant_data_1min))):  # Iteramos hasta max_velas_entrada o hasta que se acaben los datos
            if j < min_velas_entrada - 1:
                continue  # Saltar hasta alcanzar el mínimo de velas requeridas

            index_1min = relevant_data_1min.index[j]
            # Verificamos si hay una nueva transición antes de tomar la entrada
            current_time = relevant_data_1min['Date'][index_1min]
            df_transitions = df[(df['Date'] >= start_time_1min) & (df['Date'] <= current_time) & (df['transition'].notna())]
            if not df_transitions.empty:
                break  # Salir del bucle si hay una nueva transición

            if tipo_transicion == 'transition_long':
                if relevant_data_1min['Low'][index_1min] <= precio_entrada <= relevant_data_1min['High'][index_1min]:
                    df.loc[i, 'entrys'] = 1
                    df.loc[i, 'entry_dates_candle'] = pd.to_datetime(relevant_data_1min['Date'][index_1min]).tz_localize(None)
                    break  # Entrada encontrada, salir del bucle interno
            elif tipo_transicion == 'transition_short':
                if relevant_data_1min['Low'][index_1min] <= precio_entrada <= relevant_data_1min['High'][index_1min]:
                    df.loc[i, 'entrys'] = 1
                    df.loc[i, 'entry_dates_candle'] = pd.to_datetime(relevant_data_1min['Date'][index_1min]).tz_localize(None)
                    break  # Entrada encontrada, salir del bucle interno

    return df



def calcular_diferencias_porcentuales_entradas_salidas(df_5min, df_1min):
    """
    Calcula las diferencias porcentuales entre los precios de entrada y los extremos
    (high/low) hasta la vela de salida, utilizando las velas de entrada y salida.

    Args:
        df_5min: DataFrame de 5 minutos con columnas 'entrys', 'entry_dates_candle', 'date_tp_sl', 'transition', 'price_entry_long_fib', 'price_entry_short_fib'.
        df_1min: DataFrame de 1 minuto con la columna 'Date', 'High', 'Low'.

    Returns:
        DataFrame modificado con las nuevas columnas 'dif_percent_long_to_tp', 'dif_percent_long_to_sl',
                                        'dif_percent_short_to_tp', 'dif_percent_short_to_sl',
        o el DataFrame original si las columnas necesarias no existen.
    """

    columnas_requeridas = ['entrys', 'entry_dates_candle', 'date_tp_sl', 'transition', 'price_entry_long', 'price_entry_short']
    if not all(col in df_5min.columns for col in columnas_requeridas):
        print(f"Error: Faltan columnas requeridas en df_5min: {columnas_requeridas}")
        return df_5min

    df_5min['dif_percent_long_to_tp'] = np.nan
    df_5min['dif_percent_long_to_sl'] = np.nan
    df_5min['dif_percent_short_to_tp'] = np.nan
    df_5min['dif_percent_short_to_sl'] = np.nan

    for i in df_5min[df_5min['entrys'] == 1].index:
        entry_date = df_5min.loc[i, 'entry_dates_candle']
        exit_date = df_5min.loc[i, 'date_tp_sl']

        if pd.isna(entry_date) or pd.isna(exit_date):
            continue

        transition = df_5min.loc[i, 'transition']

        if transition == 'transition_long' and not pd.isna(df_5min.loc[i, 'price_entry_long']):
            precio_calculado = df_5min.loc[i, 'price_entry_long']
        elif transition == 'transition_short' and not pd.isna(df_5min.loc[i, 'price_entry_short']):
            precio_calculado = df_5min.loc[i, 'price_entry_short']
        else:
            continue  # No hay precio de entrada para esta transición

        relevant_data_1min = df_1min[(df_1min['Date'] >= entry_date) & (df_1min['Date'] <= exit_date)]

        if relevant_data_1min.empty:
            print(f"No hay datos de 1 minuto entre {entry_date} y {exit_date}")
            continue

        if transition == 'transition_long':
            high_max = relevant_data_1min['High'].max()
            low_min = relevant_data_1min['Low'].min()
            
            if not pd.isna(high_max) and precio_calculado != 0:
                df_5min.loc[i, 'dif_percent_long_to_tp'] = ((high_max - precio_calculado) / precio_calculado) * 100
            if not pd.isna(low_min) and precio_calculado != 0:
                df_5min.loc[i, 'dif_percent_long_to_sl'] = ((low_min - precio_calculado) / precio_calculado) * 100

        elif transition == 'transition_short':
            low_min = relevant_data_1min['Low'].min()
            high_max = relevant_data_1min['High'].max()
            
            if not pd.isna(low_min) and precio_calculado != 0:
                df_5min.loc[i, 'dif_percent_short_to_tp'] = ((low_min - precio_calculado) / precio_calculado) * 100
            if not pd.isna(high_max) and precio_calculado != 0:
                df_5min.loc[i, 'dif_percent_short_to_sl'] = ((high_max - precio_calculado) / precio_calculado) * 100

    return df_5min



def calcular_diferencias_porcentuales(df):
    """
    Calcula las diferencias porcentuales entre los precios de entrada (long/short) y los extremos
    (high/low) dentro de cada transición.

    Args:
        df: DataFrame de entrada con columnas 'transition', 'price_entry_long', 'price_entry_short', 'High', 'Low'.

    Returns:
        DataFrame con las nuevas columnas 'dif_percent_long_to_tp', 'dif_percent_long_to_sl',
                                        'dif_percent_short_to_tp', 'dif_percent_short_to_sl',
        o el DataFrame original si las columnas necesarias no existen.
    """

    columnas_requeridas = ['transition', 'price_entry_long', 'price_entry_short', 'High', 'Low']
    if not all(col in df.columns for col in columnas_requeridas):
        print(f"Error: Faltan columnas requeridas: {columnas_requeridas}")
        return df

    df_modificado = df.copy()  # Importante: trabajar sobre una copia para evitar SettingWithCopyWarning
    df_modificado['dif_percent_long_to_tp'] = np.nan
    df_modificado['dif_percent_long_to_sl'] = np.nan
    df_modificado['dif_percent_short_to_tp'] = np.nan
    df_modificado['dif_percent_short_to_sl'] = np.nan

    for i in range(len(df)):
        transition = df.loc[i, 'transition']

        if transition == 'transition_long' and not pd.isna(df.loc[i, 'price_entry_long']):
            precio_calculado = df.loc[i, 'price_entry_long']
            j = i + 1
            while j < len(df) and df.loc[j, 'transition'] not in ('transition_short', 'transition_long'):
                j += 1
            high_max = df.loc[i:j-1, 'High'].max()
            low_min = df.loc[i:j-1, 'Low'].min()

            if not pd.isna(high_max) and precio_calculado != 0:
                df_modificado.loc[i, 'dif_percent_long_to_tp'] = ((high_max - precio_calculado) / precio_calculado) * 100
            if not pd.isna(low_min) and precio_calculado != 0:
                df_modificado.loc[i, 'dif_percent_long_to_sl'] = ((low_min - precio_calculado) / precio_calculado) * 100

        elif transition == 'transition_short' and not pd.isna(df.loc[i, 'price_entry_short']):
            precio_calculado = df.loc[i, 'price_entry_short']
            j = i + 1
            while j < len(df) and df.loc[j, 'transition'] not in ('transition_short', 'transition_long'):
                j += 1

            low_min = df.loc[i:j-1, 'Low'].min()
            high_max = df.loc[i:j-1, 'High'].max()
            if not pd.isna(low_min) and precio_calculado != 0:
                df_modificado.loc[i, 'dif_percent_short_to_tp'] = ((low_min - precio_calculado) / precio_calculado) * 100
            if not pd.isna(high_max) and precio_calculado != 0:
                df_modificado.loc[i, 'dif_percent_short_to_sl'] = ((high_max - precio_calculado) / precio_calculado) * 100

    return df_modificado

def transformar_valores_post_negat(df, porcentaje_tp, porcentaje_sl):
    """
    Selecciona las columnas 'porcentaje_tp' y 'porcentaje_sl',
    aplica valor absoluto y maneja valores NaN.

    Args:
        df: DataFrame de entrada.

    Returns:
        DataFrame con las columnas modificadas, o el DataFrame original
        si las columnas no existen.
    """
    # Verifica si las columnas existen en el DataFrame
    if porcentaje_tp not in df.columns or porcentaje_sl not in df.columns:
        print("Advertencia: Una o ambas columnas ('porcentaje_tp', 'porcentaje_sl') no existen en el DataFrame.")
        return df  # Devuelve el DataFrame original sin modificar

    # Copia el DataFrame para evitar modificar el original directamente
    df_modificado = df.copy()

    # Aplica valor absoluto y rellena NaN con 0 en ambas columnas
    df_modificado[porcentaje_tp] = df_modificado[porcentaje_tp].abs().fillna(0)
    df_modificado[porcentaje_sl] = df_modificado[porcentaje_sl].abs().fillna(0)
    
    # Convierte a negativo los valores de las columnas
    df_modificado[porcentaje_tp] = abs(df_modificado[porcentaje_tp])
    df_modificado[porcentaje_sl] = -df_modificado[porcentaje_sl]

    return df_modificado

def imprimir_modas_extendido(df, *columnas, top_n=5, bins=15):
    """
    Imprime la moda (o valores más frecuentes) de las columnas especificadas de un DataFrame.

    Args:
        df (pd.DataFrame): El DataFrame.
        *columnas (str): Nombres de las columnas.
        top_n (int): Número de valores más frecuentes a mostrar si no hay una moda clara.
        bins (int): Número de bins para agrupar datos numéricos.

    Returns:
        None. Imprime la información en la consola.
    """
    if not columnas:
        print("No se especificaron columnas.")
        return

    print("-------------------- Análisis de Frecuencia de Columnas --------------------")
    for nombre_columna in columnas:
        if nombre_columna in df.columns:
            columna = df[nombre_columna]
            if columna.empty:
                print(f"Columna vacía: {nombre_columna}")
                continue

            if pd.api.types.is_numeric_dtype(columna): #Manejo de datos numericos
                if len(columna.unique()) > bins: #Si hay muchos valores unicos, se agrupan en bins
                    columna_categorizada = pd.cut(columna, bins=bins, include_lowest=True)
                    frecuencias = columna_categorizada.value_counts().sort_values(ascending=False)
                    print(f"Frecuencias de {nombre_columna} (agrupado en {bins} rangos):")
                    for rango, frecuencia in frecuencias.head(top_n).items():
                        print(f"  Rango: {rango}, Frecuencia: {frecuencia}")
                    continue #Continua a la siguiente columna

            frecuencias = columna.value_counts().sort_values(ascending=False)
            if len(frecuencias) == 0:
                print(f"No hay datos en la columna: {nombre_columna}")
                continue

            if len(frecuencias) == 1:
                print(f"Solo un valor en la columna {nombre_columna}: {frecuencias.index[0]} (Frecuencia: {frecuencias.iloc[0]})")
                continue

            if len(frecuencias) > 1 and frecuencias.iloc[0] == frecuencias.iloc[1]: #Manejo de multiples modas
                modas = frecuencias[frecuencias == frecuencias.iloc[0]].index.tolist()
                print(f"Múltiples modas en {nombre_columna}: {', '.join(map(str, modas))} (Frecuencia: {frecuencias.iloc[0]})")
            elif len(frecuencias) > top_n and frecuencias.iloc[0] > 1: #Si no hay una moda clara se muestran los top_n
                print(f"Valores más frecuentes en {nombre_columna} (top {top_n}):")
                for valor, frecuencia in frecuencias.head(top_n).items():
                    print(f"  Valor: {valor}, Frecuencia: {frecuencia}")
            else: #Si hay una moda clara se muestra
                print(f"Moda de {nombre_columna}: {frecuencias.index[0]} (Frecuencia: {frecuencias.iloc[0]})")

        else:
            print(f"Columna no encontrada: {nombre_columna}")
    print("---------------------------  DIMANICS SL Y TP ------------------------------------")

def tp_sl_statics_fibonacci(df_5min, df_1min, tp_fib_percent=0.60, sl_fib_percent=0.60, min_velas_entrada=1, timeframe='5min'):
    """
    Simula órdenes límite con TP/SL buscando a lo largo de todo el DataFrame DESDE LA FECHA DE ENTRADA,
    evaluando TP y SL en cada vela individualmente, utilizando retrocesos de Fibonacci.

    Args:
        df_5min (pd.DataFrame): DataFrame con las columnas 'price_entry_long', 'price_entry_short', 'High', 'Low' y 'transition'.
        df_1min (pd.DataFrame): DataFrame con la columna 'Date', 'High' y 'Low' en temporalidad de 1 minuto.
        tp_fib_percent (float): Porcentaje de Take Profit basado en Fibonacci.
        sl_fib_percent (float): Porcentaje de Stop Loss basado en Fibonacci.
        min_velas_entrada (int): Número mínimo de velas para calcular la entrada.
        timeframe (str): Temporalidad del DataFrame df_5min (por ejemplo, '5min', '15min', '30min', '1h').

    Returns:
        pd.DataFrame: DataFrame con las columnas 'static_tp', 'static_sl', 'static_tp_confirm', 'static_sl_confirm', 
                      'static_tp_percent', 'static_sl_percent' y 'date_tp_sl' añadidas.
    """
    df_5min['static_tp'] = np.nan
    df_5min['static_sl'] = np.nan
    df_5min['static_tp_confirm'] = 0
    df_5min['static_sl_confirm'] = 0
    df_5min['static_tp_percent'] = 0.0
    df_5min['static_sl_percent'] = 0.0
    df_5min['date_tp_sl'] = pd.NaT

    long_entries = (df_5min['entrys'] == 1) & (df_5min['transition'] == 'transition_long')
    short_entries = (df_5min['entrys'] == 1) & (df_5min['transition'] == 'transition_short')

    df_5min['Date'] = pd.to_datetime(df_5min['Date'], errors='coerce').dt.tz_localize(None)
    df_1min['Date'] = pd.to_datetime(df_1min['Date'], errors='coerce').dt.tz_localize(None)

    for i in df_5min[df_5min['entrys'] == 1].index:
        entry_row_5min = df_5min.loc[i]
        entry_time_1min = entry_row_5min['entry_dates_candle']

        if pd.isna(entry_time_1min):
            continue

        try:
            start_index_1min = df_1min.index.get_loc(df_1min[df_1min['Date'] == entry_time_1min].index[0])
        except IndexError:
            print(f"No se encontró la fecha de entrada en df_1min para el índice {i}: {entry_time_1min}")
            continue

        relevant_data_1min = df_1min.iloc[max(0, start_index_1min - min_velas_entrada):start_index_1min + 1]

        if relevant_data_1min.empty:
            print(f"No hay datos de 1 minuto entre {entry_time_1min}")
            continue

        low_min = relevant_data_1min['Low'].min()
        high_max = relevant_data_1min['High'].max()
        transition = entry_row_5min['transition']

        if transition == 'transition_long' and not pd.isna(entry_row_5min['price_entry_long']):
            entry_price = entry_row_5min['price_entry_long']
            tp_price = entry_price + (high_max - entry_price) * tp_fib_percent
            sl_price = entry_price - (entry_price - low_min) * sl_fib_percent

            df_5min.loc[i, 'static_tp'] = tp_price
            df_5min.loc[i, 'static_sl'] = sl_price

        elif transition == 'transition_short' and not pd.isna(entry_row_5min['price_entry_short']):
            entry_price = entry_row_5min['price_entry_short']
            tp_price = entry_price - (entry_price - low_min) * tp_fib_percent
            sl_price = entry_price + (high_max - entry_price) * sl_fib_percent

            df_5min.loc[i, 'static_tp'] = tp_price
            df_5min.loc[i, 'static_sl'] = sl_price
        else:
            continue

        tp_reached = False
        sl_reached = False
        tp_date = pd.NaT
        sl_date = pd.NaT

        for j in range(len(df_1min.iloc[start_index_1min + 1:])):
            index_1min = df_1min.index[start_index_1min + 1 + j]
            high = df_1min['High'][index_1min]
            low = df_1min['Low'][index_1min]
            date = df_1min['Date'][index_1min]

            if transition == 'transition_long':
                if high >= tp_price:
                    tp_reached = True
                    tp_date = date
                    break
                if low <= sl_price:
                    sl_reached = True
                    sl_date = date
                    break
            elif transition == 'transition_short':
                if low <= tp_price:
                    tp_reached = True
                    tp_date = date
                    break
                if high >= sl_price:
                    sl_reached = True
                    sl_date = date
                    break

        if tp_reached:
            tp_percent = ((tp_price - entry_price) / entry_price) * 100 if transition == 'transition_long' else ((entry_price - tp_price) / entry_price) * 100
            df_5min.loc[i, 'static_tp_confirm'] = 1
            df_5min.loc[i, 'static_tp_percent'] = tp_percent
            df_5min.loc[i, 'date_tp_sl'] = pd.to_datetime(tp_date).tz_localize(None)
        elif sl_reached:
            sl_percent = ((sl_price - entry_price) / entry_price) * 100 if transition == 'transition_long' else ((entry_price - sl_price) / entry_price) * 100
            df_5min.loc[i, 'static_sl_confirm'] = 1
            df_5min.loc[i, 'static_sl_percent'] = sl_percent
            df_5min.loc[i, 'date_tp_sl'] = pd.to_datetime(sl_date).tz_localize(None)

    return df_5min




def tp_sl_statics(df_5min, df_1min, tp_percent, sl_percent, timeframe='5min'):
    """Simula órdenes límite con TP/SL buscando a lo largo de todo el DataFrame DESDE LA FECHA DE ENTRADA,
       evaluando TP y SL en cada vela individualmente.

    Args:
        df_5min (pd.DataFrame): DataFrame con las columnas 'price_entry_long', 'price_entry_short', 'High', 'Low' y 'transition'.
        df_1min (pd.DataFrame): DataFrame con la columna 'Date', 'High' y 'Low' en temporalidad de 1 minuto.
        tp_percent (float): Porcentaje de Take Profit.
        sl_percent (float): Porcentaje de Stop Loss.
        timeframe (str): Temporalidad del DataFrame df_5min (por ejemplo, '5min', '15min', '30min', '1h').

    Returns:
        pd.DataFrame: DataFrame con las columnas 'static_tp', 'static_sl', 'static_tp_confirm', 'static_sl_confirm', 'static_tp_percent', 'static_sl_percent' y 'date_tp_sl' añadidas.
    """
    df_5min['static_tp'] = np.nan
    df_5min['static_sl'] = np.nan
    df_5min['static_tp_confirm'] = 0
    df_5min['static_sl_confirm'] = 0
    df_5min['static_tp_percent'] = 0.0
    df_5min['static_sl_percent'] = 0.0
    df_5min['date_tp_sl'] = pd.NaT

    long_entries = (df_5min['entrys'] == 1) & (df_5min['transition'] == 'transition_long')
    short_entries = (df_5min['entrys'] == 1) & (df_5min['transition'] == 'transition_short')

    df_5min.loc[long_entries, 'static_tp'] = (df_5min.loc[long_entries, 'price_entry_long'] * (1 + tp_percent)).astype(np.float64)
    df_5min.loc[long_entries, 'static_sl'] = (df_5min.loc[long_entries, 'price_entry_long'] * (1 - sl_percent)).astype(np.float64)

    df_5min.loc[short_entries, 'static_tp'] = (df_5min.loc[short_entries, 'price_entry_short'] * (1 - tp_percent)).astype(np.float64)
    df_5min.loc[short_entries, 'static_sl'] = (df_5min.loc[short_entries, 'price_entry_short'] * (1 + sl_percent)).astype(np.float64)

    df_5min['Date'] = pd.to_datetime(df_5min['Date'], errors='coerce').dt.tz_localize(None)
    df_1min['Date'] = pd.to_datetime(df_1min['Date'], errors='coerce').dt.tz_localize(None)

    time_deltas = {
        '5min': 5,
        '15min': 15,
        '30min': 30,
        '1h': 60,
        # Puedes agregar más temporalidades según sea necesario
    }
    timeframe_minutes = time_deltas.get(timeframe, 5)  # Por defecto 5 minutos

    min_date = df_5min['Date'].min()
    max_date = df_5min['Date'].max()
    df_1min_filtered = df_1min[(df_1min['Date'] >= min_date) & (df_1min['Date'] <= max_date)].copy()
    if df_1min_filtered.empty:
        print("Advertencia: df_1min está vacío o no contiene datos en el rango de df_5min.")
        return df_5min

    for i in df_5min[df_5min['entrys'] == 1].index:
        entry_row_5min = df_5min.loc[i]
        entry_time_1min = entry_row_5min['entry_dates_candle']

        if pd.isna(entry_time_1min):
            continue

        try:
            start_index_1min = df_1min_filtered.index.get_loc(df_1min_filtered[df_1min_filtered['Date'] == entry_time_1min].index[0])
        except IndexError:
            print(f"No se encontró la fecha de entrada en df_1min_filtered para el índice {i}: {entry_time_1min}")
            continue

        relevant_data_1min = df_1min_filtered.iloc[start_index_1min+1:]

        if relevant_data_1min.empty:
            print(f"No hay datos de 1 minuto DESPUÉS de la entrada en {entry_time_1min}")
            continue

        static_tp = entry_row_5min['static_tp']
        static_sl = entry_row_5min['static_sl']
        transition = entry_row_5min['transition']

        tp_reached = False
        sl_reached = False
        tp_date = pd.NaT
        sl_date = pd.NaT

        for j in range(len(relevant_data_1min)):
            index_1min = relevant_data_1min.index[j]
            high = relevant_data_1min['High'][index_1min]
            low = relevant_data_1min['Low'][index_1min]
            date = relevant_data_1min['Date'][index_1min]

            if transition == 'transition_long':
                if high >= static_tp:
                    tp_reached = True
                    tp_date = date
                    break
                if low <= static_sl:
                    sl_reached = True
                    sl_date = date
                    break
            else:  # transition_short
                if low <= static_tp:
                    tp_reached = True
                    tp_date = date
                    break
                if high >= static_sl:
                    sl_reached = True
                    sl_date = date
                    break

        if tp_reached:
            df_5min.loc[i, 'static_tp_confirm'] = 1
            df_5min.loc[i, 'static_tp_percent'] = tp_percent * 100
            df_5min.loc[i, 'date_tp_sl'] = pd.to_datetime(tp_date).tz_localize(None)
        elif sl_reached:
            df_5min.loc[i, 'static_sl_confirm'] = 1
            df_5min.loc[i, 'static_sl_percent'] = -sl_percent * 100
            df_5min.loc[i, 'date_tp_sl'] = pd.to_datetime(sl_date).tz_localize(None)

    return df_5min




def calculate_exits(df):
    """Calcula las salidas basadas en la siguiente transición y registra resultados (TP/SL)."""

    df['static_tp'] = np.nan
    df['static_sl'] = np.nan
    df['static_tp_confirm'] = 0
    df['static_sl_confirm'] = 0
    df['static_tp_percent'] = 0.0
    df['static_sl_percent'] = 0.0
    df['date_tp_sl'] = pd.NaT

    for i in df[df['entrys'] == 1].index:
        entry_row = df.loc[i].copy()

        if pd.isna(entry_row['transition']):
            print(f"¡Advertencia! entrys == 1 pero sin transición en índice {i}")
            continue

        velas_posteriores = entry_row['velas_posteriores']

        if pd.isna(velas_posteriores) or velas_posteriores == 0:
            print(f"Sin siguiente transición para entrada en índice {i} (velas_posteriores: {velas_posteriores})")
            continue

        next_transition_index = i + int(velas_posteriores) + 1

        # Corrección crucial: Manejar el caso donde no hay una transición posterior
        if next_transition_index >= len(df):
            print(f"Advertencia: No hay transición posterior para entrada en índice {i}. Usando el último precio disponible.")
            next_transition_index = len(df) - 1  # Usar el último índice válido

        exit_price = df.loc[next_transition_index, 'Close']
        exit_date = df.loc[next_transition_index, 'Date']
        percentage_change = 0.0

        if entry_row['transition'] == 'transition_long':
            percentage_change = ((exit_price - entry_row['price_entry_long']) / entry_row['price_entry_long']) * 100
        elif entry_row['transition'] == 'transition_short':
            # Corrección: Cálculo correcto del porcentaje para short
            percentage_change = ((entry_row['price_entry_short'] - exit_price) / entry_row['price_entry_short']) * 100
        else:
            print(f"Transición desconocida en índice {i}: {entry_row['transition']}")
            continue

        if percentage_change > 0:
            df.loc[i, 'static_tp'] = exit_price
            df.loc[i, 'static_tp_confirm'] = 1
            df.loc[i, 'static_tp_percent'] = percentage_change
        else:
            df.loc[i, 'static_sl'] = exit_price
            df.loc[i, 'static_sl_confirm'] = 1
            df.loc[i, 'static_sl_percent'] = percentage_change
        df.loc[i, 'date_tp_sl'] = exit_date

    return df


def calcular_extensiones_fibonacci(df_5min, df_1min, extension_tp=1.618, extension_sl=0.618, min_velas_entrada=1, timeframe='5min'):
    """
    Calcula los precios de Take Profit (TP) y Stop Loss (SL) utilizando extensiones de Fibonacci,
    simulando la ejecución de órdenes de forma realista.

    Args:
        df_5min (pd.DataFrame): DataFrame con 'price_entry_long', 'price_entry_short', 'High', 'Low', 'transition', 'entrys', 'entry_dates_candle' y 'Date'.
        df_1min (pd.DataFrame): DataFrame con 'Date', 'Open', 'High', 'Low', 'Close'.
        extension_tp (float): Nivel de extensión de Fibonacci para TP (ej. 1.618).
        extension_sl (float): Nivel de extensión de Fibonacci para SL (ej. 0.618).
        min_velas_entrada (int): Número mínimo de velas para calcular la entrada.
        timeframe (str): Temporalidad del DataFrame df_5min.

    Returns:
        pd.DataFrame: DataFrame con las columnas 'ext_tp', 'ext_sl', 'ext_tp_confirm', 'ext_sl_confirm', 'ext_tp_percent', 'ext_sl_percent' y 'date_ext_tp_sl' añadidas.
        Devuelve el dataframe original si no existen las columnas necesarias.
    """

    columnas_requeridas_5min = ['price_entry_long', 'price_entry_short', 'High', 'Low', 'transition', 'entrys', 'entry_dates_candle', 'Date']
    columnas_requeridas_1min = ['Date', 'Open', 'High', 'Low', 'Close']

    if not all(col in df_5min.columns for col in columnas_requeridas_5min):
        print(f"Error: Faltan columnas requeridas en df_5min: {columnas_requeridas_5min}")
        return df_5min
    if not all(col in df_1min.columns for col in columnas_requeridas_1min):
        print(f"Error: Faltan columnas requeridas en df_1min: {columnas_requeridas_1min}")
        return df_1min

    df_5min['static_tp'] = np.nan
    df_5min['static_sl'] = np.nan
    df_5min['static_tp_confirm'] = 0
    df_5min['static_sl_confirm'] = 0
    df_5min['static_tp_percent'] = 0.0
    df_5min['static_sl_percent'] = 0.0
    df_5min['date_tp_sl'] = pd.NaT

    df_5min['Date'] = pd.to_datetime(df_5min['Date'], errors='coerce').dt.tz_localize(None)
    df_1min['Date'] = pd.to_datetime(df_1min['Date'], errors='coerce').dt.tz_localize(None)

    for i in df_5min[df_5min['entrys'] == 1].index:
        entry_row_5min = df_5min.loc[i]
        entry_time_1min = entry_row_5min['entry_dates_candle']

        if pd.isna(entry_time_1min):
            continue

        try:
            start_index_1min = df_1min.index.get_loc(df_1min[df_1min['Date'] == entry_time_1min].index[0])
        except IndexError:
            print(f"No se encontró la fecha de entrada en df_1min para el índice {i}: {entry_time_1min}")
            continue

        relevant_data_1min = df_1min.iloc[max(0, start_index_1min - min_velas_entrada):start_index_1min + 1]

        if relevant_data_1min.empty:
            print(f"No hay datos de 1 minuto entre {entry_time_1min}")
            continue

        low_min = relevant_data_1min['Low'].min()
        high_max = relevant_data_1min['High'].max()
        transition = entry_row_5min['transition']

        if transition == 'transition_long' and not pd.isna(entry_row_5min['price_entry_long']):
            entry_price = entry_row_5min['price_entry_long']
            rango = high_max - low_min
            ext_tp_price = entry_price + rango * extension_tp
            ext_sl_price = entry_price - rango * extension_sl

        elif transition == 'transition_short' and not pd.isna(entry_row_5min['price_entry_short']):
            entry_price = entry_row_5min['price_entry_short']
            rango = high_max - low_min
            ext_tp_price = entry_price - rango * extension_tp
            ext_sl_price = entry_price + rango * extension_sl
        else:
            continue

        df_5min.loc[i, 'static_tp'] = ext_tp_price
        df_5min.loc[i, 'static_sl'] = ext_sl_price

        tp_reached = False
        sl_reached = False
        tp_date = pd.NaT
        sl_date = pd.NaT

        for j in range(len(df_1min.iloc[start_index_1min + 1:])):
            index_1min = df_1min.index[start_index_1min + 1 + j]
            open_price = df_1min['Open'][index_1min]
            high = df_1min['High'][index_1min]
            low = df_1min['Low'][index_1min]
            close = df_1min['Close'][index_1min]
            date = df_1min['Date'][index_1min]

            if transition == 'transition_long':
                if open_price <= ext_sl_price and low <= ext_sl_price:
                    sl_reached = True
                    sl_date = date
                    break
                if open_price <= ext_tp_price and high >= ext_tp_price:
                    tp_reached = True
                    tp_date = date
                    break

            elif transition == 'transition_short':
                if open_price >= ext_sl_price and high >= ext_sl_price:
                    sl_reached = True
                    sl_date = date
                    break
                if open_price >= ext_tp_price and low <= ext_tp_price:
                    tp_reached = True
                    tp_date = date
                    break

        if tp_reached:
            tp_percent = ((ext_tp_price - entry_price) / entry_price) * 100 if transition == 'transition_long' else ((entry_price - ext_tp_price) / entry_price) * 100
            df_5min.loc[i, 'static_tp_confirm'] = 1
            df_5min.loc[i, 'static_tp_percent'] = tp_percent
            df_5min.loc[i, 'date_tp_sl'] = pd.to_datetime(tp_date).tz_localize(None)
        elif sl_reached:
            sl_percent = ((ext_sl_price - entry_price) / entry_price) * 100 if transition == 'transition_long' else ((entry_price - ext_sl_price) / entry_price) * 100
            df_5min.loc[i, 'static_sl_confirm'] = 1
            df_5min.loc[i, 'static_sl_percent'] = sl_percent
            df_5min.loc[i, 'date_tp_sl'] = pd.to_datetime(sl_date).tz_localize(None)

    return df_5min


def calcular_extensiones_fibonacci_solo_calculo(df_5min, df_1min, extension_tp, extension_sl, min_velas_entrada):
    """
    Calcula los precios de Take Profit (TP) y Stop Loss (SL) utilizando extensiones de Fibonacci,
    SIN verificar si se alcanzan en los datos de 1 minuto. Itera sobre TODAS las transiciones.

    Args:
        df_5min (pd.DataFrame): DataFrame con 'price_entry_long', 'price_entry_short', 'High', 'Low', 'transition', 'Date'.
        df_1min (pd.DataFrame): DataFrame con 'Date', 'High', 'Low'.
        extension_tp (float): Nivel de extensión de Fibonacci para TP (ej. 1.618).
        extension_sl (float): Nivel de extensión de Fibonacci para SL (ej. 0.618).
        min_velas_entrada (int): Número mínimo de velas para calcular la entrada.

    Returns:
        pd.DataFrame: DataFrame con las columnas 'precio_takeprofit' y 'precio_stoploss' añadidas.
        Devuelve el dataframe original si no existen las columnas necesarias.
    """

    columnas_requeridas_5min = ['price_entry_long', 'price_entry_short', 'High', 'Low', 'transition', 'Date']
    columnas_requeridas_1min = ['Date', 'High', 'Low']

    if not all(col in df_5min.columns for col in columnas_requeridas_5min):
        print(f"Error: El DataFrame debe contener las columnas requeridas en df_5min: {columnas_requeridas_5min}")
        return df_5min
    if not all(col in df_1min.columns for col in columnas_requeridas_1min):
        print(f"Error: Faltan columnas requeridas en df_1min: {columnas_requeridas_1min}")
        return df_1min

    df_5min['precio_takeprofit'] = np.nan
    df_5min['precio_stoploss'] = np.nan

    df_5min['Date'] = pd.to_datetime(df_5min['Date'], errors='coerce').dt.tz_localize(None)
    df_1min['Date'] = pd.to_datetime(df_1min['Date'], errors='coerce').dt.tz_localize(None)

    # Iterar sobre TODAS las filas donde hay una transición
    for i in df_5min[df_5min['transition'].notna()].index:
        entry_row_5min = df_5min.loc[i]
        entry_time_5min = entry_row_5min['entry_dates_candle']

        if pd.isna(entry_time_5min):
            continue
        
        # Encuentra el índice EXACTO en df_1min usando merge_asof para el caso de datos incompletos
        df_temp = pd.DataFrame({'Date': [entry_time_5min]})
        df_merged = pd.merge_asof(df_temp, df_1min, on='Date', direction='backward')

        if df_merged.empty or pd.isna(df_merged['Date'].iloc[0]):
            print(f"No se encontró la fecha de entrada en df_1min para el índice {i}: {entry_time_5min}")
            continue

        entry_time_1min = df_merged['Date'].iloc[0]
        start_index_1min = df_1min.index.get_loc(df_1min[df_1min['Date'] == entry_time_1min].index[0])

        relevant_data_1min = df_1min.iloc[max(0, start_index_1min - min_velas_entrada):start_index_1min + 1]

        if relevant_data_1min.empty:
            print(f"No hay datos de 1 minuto entre {entry_time_1min}")
            continue

        low_min = relevant_data_1min['Low'].min()
        high_max = relevant_data_1min['High'].max()
        transition = entry_row_5min['transition']

        if transition == 'transition_long' and not pd.isna(entry_row_5min['price_entry_long']):
            entry_price = entry_row_5min['price_entry_long']
            rango = high_max - low_min
            tp_price = entry_price + rango * extension_tp
            sl_price = entry_price - rango * extension_sl

        elif transition == 'transition_short' and not pd.isna(entry_row_5min['price_entry_short']):
            entry_price = entry_row_5min['price_entry_short']
            rango = high_max - low_min
            tp_price = entry_price - rango * extension_tp
            sl_price = entry_price + rango * extension_sl
        else:
            continue

        df_5min.loc[i, 'precio_takeprofit'] = tp_price
        df_5min.loc[i, 'precio_stoploss'] = sl_price

    return df_5min
if __name__ == "__main__":
 
    print('LOGICA DE MAUF')
    '''
    Este módulo es la copia de logic_5 pero con las correccioines de la detección de entradas y la 
    corrección de la detección de take profit y stop loss. Los detecta correctamente para 5min y 1min.

    - Falta hacer que sea capaz de detectar las salidas por cruces como el tp o sl
    - Falta hacer que sea capaz de realizar análisis en otras temporalidades usando los datos de 1 min como los datos bases para realizar
    las entradas.

    '''
    df = pd.read_csv('ETHUSDT_5.csv')
    df_1min = pd.read_csv('ETHUSDT_1.csv')

    # agregar indicadores ema
    df = ema.calcular_ema(df, length_short=10, length_long=50)

    # Detectar cruces de hmas
    indicator1 = 'ema_short'
    indicator2 = 'ema_long'
    df = identificar_transiciones(df, indicator1, indicator2)

    # Obtener precio de entrada para long y short
    porcentaje_long = 0.0025
    porcentaje_short = 0.0025
    #df = calcular_precios_entrada(df, porcentaje_long, porcentaje_short)
    # Otra forma de calcular los precios de entrada:

    df = calcular_precios_entrada_fibonacci(df, df_1min, fibonacci_retracement_long=0.6, fibonacci_retracement_short=0.35, min_velas_entrada=65)

    # Detectar si se logró la entrada de short y long
    df = detectar_entradas(df, df_1min, timeframe='5min', min_velas_entrada=65, max_velas_entrada=9000)

    # Calcula las diferencias porcentuales entre los precios de entrada (long/short) y los extremos (high/low) dentro de cada transición
    #df = calcular_diferencias_porcentuales(df)

    # Tp y Sl Fijos9
    tp_percent = 0.008 # Take profit fijo
    sl_percent = 0.01 # Stop Loss Fijo
    #df = tp_sl_statics(df, df_1min, tp_percent, sl_percent, timeframe='5min')

    #TP Y SL DINÁMICOS
    #df = tp_sl_statics_fibonacci(df, df_1min, tp_fib_percent=10.0, sl_fib_percent=40.0, min_velas_entrada=15, timeframe='5min')
    df = calcular_extensiones_fibonacci(df, df_1min, extension_tp=1.336, extension_sl=2.636, min_velas_entrada=65, timeframe='5min')
    #df = calcular_extensiones_fibonacci_solo_calculo(df, df_1min, extension_tp=4.236, extension_sl=4.236, min_velas_entrada=15)
    # Otra forma de realizar las salidas usando los cruces
    #df = calculate_exits(df)

    # Otra forma de calcular diferencias porcentuales basado en las entrads y salidas de la estrategia a analizar
    df = calcular_diferencias_porcentuales_entradas_salidas(df, df_1min)

    # transformar a positivo los porcentajes o valores deseados
    df = transformar_valores_post_negat(df, porcentaje_tp='dif_percent_long_to_tp', porcentaje_sl='dif_percent_long_to_sl')
    df = transformar_valores_post_negat(df, porcentaje_tp='dif_percent_short_to_tp', porcentaje_sl='dif_percent_short_to_sl')
    df = transformar_valores_post_negat(df, porcentaje_tp='static_tp_percent', porcentaje_sl='static_sl_percent')

    # print(df)
    df.to_csv('hma_avax_bckt33.csv')
    backtet_hma_2.eject(df)

    pass

