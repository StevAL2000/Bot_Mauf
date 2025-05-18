import pandas as pd
import numpy as np

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
                df.loc[i, 'price_entry_short'] = 0.0
                #print(f"Precio de entrada long para la fila {i}: {fib_price}")
                #print('-------------------------------------')
                #print('')

        elif tipo_transicion == 'transition_short':
            #print(f"max_high_price para la fila {i}: {max_high_price}")
            #print(f"min_low_price para la fila {i}: {min_low_price}")
            if not pd.isna(max_high_price) and not pd.isna(min_low_price):
                fib_price = max_high_price - (max_high_price - min_low_price) * fibonacci_retracement_short
                df.loc[i, 'price_entry_short'] = fib_price
                df.loc[i, 'price_entry_long'] = 0.0
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


def eject_logic(df, df_1min, fibonacci_retracement_long, fibonacci_retracement_short, min_velas_entrada, max_velas_entrada, extension_tp, extension_sl, timeframe='5min'):
    """
    Ejecuta la lógica de la estrategia y devuelve un DataFrame con los resultados."""

    # Detectar cruces de hmas
    indicator1 = 'ema_short'
    indicator2 = 'ema_long'
    df = identificar_transiciones(df, indicator1, indicator2)

    # Obtener precio de entrada para long y short
    df = calcular_precios_entrada_fibonacci(df, df_1min, fibonacci_retracement_long, fibonacci_retracement_short, min_velas_entrada)

    # Detectar si se logró la entrada de short y long
    df = detectar_entradas(df, df_1min, timeframe, min_velas_entrada, max_velas_entrada)

    # Tp y Sl Fijos
    df = calcular_extensiones_fibonacci(df, df_1min, extension_tp, extension_sl, min_velas_entrada=65, timeframe='5min')

    return df