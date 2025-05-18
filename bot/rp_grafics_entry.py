import pandas as pd
import numpy as np
import mplfinance as mpf
import matplotlib.pyplot as plt
from datetime import datetime
import warnings

# Suprimir advertencias FutureWarning
warnings.simplefilter(action='ignore', category=FutureWarning)

def convert(dataframe, columnas):
    for columna in columnas:
        if columna in dataframe.columns:
            try:
                dataframe.loc[:, columna] = dataframe[columna].astype(float)
            except ValueError:
                print(f"Error: No se pudo convertir la columna '{columna}' a float.")
        else:
            print(f"Error: La columna '{columna}' no existe en el DataFrame.")
    return dataframe

def rp(df, symbol, fecha_entrada, precio_entrada, image_name):
    df = df.tail(80)

    required_columns = ['Date', 'Open', 'High', 'Low', 'Close', 'Volume']
    if not all(col in df.columns for col in required_columns):
        print(f"Error: El DataFrame no contiene todas las columnas necesarias: {required_columns}")
        return None

    df = convert(df, columnas=['Open', 'Close', 'High', 'Low', 'Volume'])

    df.loc[:, 'Date'] = pd.to_datetime(df['Date'], errors='coerce').dt.tz_localize(None)
    if df['Date'].isnull().any():
        print("Error: Hay valores nulos en la columna 'Date' después de la conversión.")
        return None
    
    df.set_index('Date', inplace=True)

    df = df.dropna(subset=['High', 'Low'])

    if df.empty:
        print("Error: El DataFrame está vacío.")
        return None

    try:
        precio_entrada = float(precio_entrada)
    except ValueError:
        print("Error: 'precio_entrada' no es un número válido.")
        return None

    try:
        fecha_entrada = pd.to_datetime(fecha_entrada, format='%Y-%m-%d %H:%M:%S').tz_localize(None)
    except ValueError:
        print("Error: 'fecha_entrada' no es una fecha válida.")
        return None

    if fecha_entrada < df.index.min() or fecha_entrada > df.index.max():
        fecha_entrada = df.index[-1]

    fecha_cercana = df.index.asof(fecha_entrada)

    if pd.isna(fecha_cercana):
        print("Error: No se encontró una fecha cercana válida.")
        return None

    arrows_up_fecha = pd.Series(np.nan, index=df.index)
    arrows_up_fecha.loc[fecha_cercana] = df.loc[fecha_cercana, 'Close']

    arrows_up_precio = pd.Series(np.nan, index=df.index)
    for i in range(len(df) - 2, len(df)):
        high = df.iloc[i].High
        low = df.iloc[i].Low
        if high >= precio_entrada >= low:
            arrows_up_precio.iloc[i] = precio_entrada
            break

    if arrows_up_fecha.dropna().empty and arrows_up_precio.dropna().empty:
        print("Error: No hay datos válidos para flechas o precios.")
        return None

    mc = mpf.make_marketcolors(up='#089981', down='#f23645', edge='i', wick='i', volume='in', inherit=True)
    s = mpf.make_mpf_style(marketcolors=mc, figcolor='#fffdfd', gridcolor='#1f232e', facecolor='#141823', edgecolor='#fffdfd')
    
    apdict = [
        mpf.make_addplot(arrows_up_fecha, type='scatter', markersize=700, marker='o', color='black'),
        mpf.make_addplot(arrows_up_fecha, type='scatter', markersize=550, marker='o', color='w'),
        mpf.make_addplot(arrows_up_fecha, type='scatter', markersize=300, marker='o', color='#24dca4'),
        mpf.make_addplot(arrows_up_fecha, type='scatter', markersize=100, marker='o', color='#ffffff'),
        mpf.make_addplot(arrows_up_fecha, type='scatter', markersize=20, marker='o', color='r')
    ]

    fig, axlist = mpf.plot(df, type='candle', style=s, addplot=apdict, figratio=(16, 8), returnfig=True)
    fig.subplots_adjust(left=0, right=1, top=1, bottom=0)

    for ax in axlist:
        ax.tick_params(axis='x', colors='white')
        ax.tick_params(axis='y', colors='white')
        ax.xaxis.label.set_color('white')
        ax.yaxis.label.set_color('white')
        ax.spines['bottom'].set_color('white')
        ax.spines['left'].set_color('white')
        ax.spines['top'].set_color('white')
        ax.spines['right'].set_color('white')

    fig.suptitle(f'{symbol} precio de entrada: {round(precio_entrada, 3)}', fontsize=30, color='white', ha='center', fontname='Arial', weight='bold')

    try:
        fig.savefig(f'{image_name}.png', bbox_inches='tight', pad_inches=1, facecolor='black')
    except Exception as e:
        print(f"Error al guardar la imagen: {e}")
        return None
    #mpf.show()
    return f'{image_name}.png'

if __name__ == "__main__":
    try:
        df = pd.read_csv('DOGEUSDT_1.csv', index_col=False)
        print(df.columns)  # Verificar nombres de columnas
        
        df.loc[:, 'Date'] = pd.to_datetime(df['Date'], errors='coerce').dt.tz_localize(None)
        if df['Date'].isnull().any():
            print("Error: Hay valores nulos en la columna 'Date' después de la conversión.")
        
        symbol = "DOGEUSDT"
        fecha_entrada = '2025-02-03 00:34:00'
        precio_entrada = df['Close'].iloc[-1]
        image_name = 'DOGEUSDT'

        rp(df, symbol, fecha_entrada, precio_entrada, image_name)
    except Exception as e:
        print(f"Error al cargar el archivo o procesar los datos: {e}")