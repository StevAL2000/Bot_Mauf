import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
def sumar_columna(df, nombre_columna):
    """
    Suma los valores de una columna de un DataFrame y devuelve el resultado como float.

    Args:
        df (pd.DataFrame): El DataFrame.
        nombre_columna (str): El nombre de la columna a sumar.

    Returns:
        float: La suma de los valores de la columna.
        None: Si la columna no existe o si no se pueden sumar sus valores.
    """
    if not isinstance(df, pd.DataFrame):
        print("Error: El primer argumento debe ser un DataFrame de pandas.")
        return None

    if not isinstance(nombre_columna, str):
        print("Error: El nombre de la columna debe ser una cadena.")
        return None

    if nombre_columna not in df.columns:
        print(f"Error: La columna '{nombre_columna}' no existe en el DataFrame.")
        return None

    columna = df[nombre_columna]

    if columna.empty:
        print(f"Advertencia: La columna '{nombre_columna}' está vacía. Se devuelve 0.0.")
        return 0.0

    if not pd.api.types.is_numeric_dtype(columna):
        try:
            columna = pd.to_numeric(columna, errors='raise') #Intenta convertir a números, si falla lanza una excepción
        except ValueError:
            print(f"Error: La columna '{nombre_columna}' contiene datos no numéricos que no se pueden convertir.")
            return None

    suma = float(columna.sum())  # Calcula la suma y la convierte a float
    return suma


def mediana(df, columna):
    """
    Calcula la mediana de una columna, manejando valores NaN, y elimina valores 0.0 y -0.0 antes de calcular la mediana.
    """
    # Usamos pd.notna() para filtrar valores que NO son NaN
    valores = df[columna][pd.notna(df[columna])]
    
    # Filtrar valores que no sean 0.0 y -0.0
    valores = valores[valores != 0]

    # Verificamos que la columna no este vacia despues de eliminar los NaN y 0
    if valores.empty:
        return np.nan
    
    # Calculamos la mediana con numpy que ignora los NaN
    result = np.median(valores)
    return result


def calcular_drawdown_porcentual(df, tp_confirm, sl_confirm, sl_percent):
    operaciones = df[(df[tp_confirm] == 1) | (df[sl_confirm] == 1)].copy()
    drawdowns = []
    drawdown_actual = 0

    for i in range(len(operaciones)):
        if operaciones.iloc[i][sl_confirm] == 1:
            drawdown_actual += operaciones.iloc[i][sl_percent]
        else:
            drawdown_actual = 0 # Reiniciamos el drawdown si hay una ganancia

        drawdowns.append(drawdown_actual)

    if drawdowns:
        drawdown_maximo = min(drawdowns)
    else:
        drawdown_maximo = 0
    return drawdown_maximo

def report_sl_tp_statics(df):
    # Calculate metrics
    total_trades = sumar_columna(df, 'entrys')
    if total_trades is None:
        return  # Handle potential errors from sumar_columna
    total_tp = sumar_columna(df, 'static_tp_confirm')
    if total_tp is None:
        return
    total_sl = sumar_columna(df, 'static_sl_confirm')
    if total_sl is None:
        return
    total_ganancias = sumar_columna(df, 'static_tp_percent')
    if total_ganancias is None:
        return
    total_perdidas = sumar_columna(df, 'static_sl_percent')
    if total_perdidas is None:
        return

    try:
        taza_acierto = (total_tp / total_trades) * 100
    except ZeroDivisionError:
        taza_acierto = 0.0  # Handle division by zero (no trades)

    drawdown = calcular_drawdown_porcentual(df, 'static_tp_confirm', 'static_sl_confirm', 'static_sl_percent')
    media_wyns = mediana(df, 'static_tp_percent')
    media_lose = mediana(df, 'static_sl_percent') 
    rentabilidad = total_ganancias + total_perdidas

    # Obtener la primera y última fecha del DataFrame
    start_time = df['Date'].min()
    end_time = df['Date'].max()

    # Crear DataFrame con valores calculados
    report_df = {
        'Time': f'{start_time} hasta {end_time}',
        'Total Trades': total_trades,
        'Total TP': total_tp,
        'Total SL': total_sl,
        'Total Ganancias TP %': total_ganancias,
        'Total Pérdidas SL %': total_perdidas,
        'Rentabilidad total': rentabilidad,
        'Tasa de Aciertos %': taza_acierto,
        'Ganancia media por operación Ganadora': media_wyns,
        'Perdida media por operación Perdedora': media_lose,
        'Drawdown': drawdown
    }

    # Imprimir el informe
    for clave, valor in report_df.items():
        print(f"{clave} : {valor}")

    return

def imprimir_modas_extendido(df, *columnas, top_n=5, bins=10):
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
    print("--------------------------------------------------------------")

def graficar_columnas_binarias(df, columna_binaria1='columna1', columna_binaria2='columna2',columna_tiempo='Date'):
    """
    Genera un gráfico de líneas para dos columnas binarias en relación con una columna de tiempo.

    Args:
        df (pd.DataFrame): El DataFrame.
        columna_tiempo (str): El nombre de la columna de tiempo.
        columna_binaria1 (str): El nombre de la primera columna binaria.
        columna_binaria2 (str): El nombre de la segunda columna binaria.

    Returns:
        None. Muestra el gráfico.
    """
    # Convertir la columna de tiempo a tipo datetime
    df[columna_tiempo] = pd.to_datetime(df[columna_tiempo])

    # Crear el gráfico
    plt.figure(figsize=(12, 6))
    plt.plot(df[columna_tiempo], df[columna_binaria1], label='Columna 1', color='blue')
    plt.plot(df[columna_tiempo], df[columna_binaria2], label='Columna 2', color='red')

    # Configurar etiquetas y título
    plt.xlabel('Tiempo')
    plt.ylabel('Valores Binarios')
    plt.title('Gráfico de Columnas Binarias en Relación con el Tiempo')
    plt.legend()

    # Mostrar el gráfico
    plt.show()

# Ejemplo de uso
if __name__ == '__main__':
    df = pd.read_csv('hma_avax_bckt_10.csv')
    graficar_columnas_binarias(df, columna_tiempo='Date', columna_binaria1='static_tp_confirm', columna_binaria2='static_sl_confirm')

def eject(df):

    imprimir_modas_extendido(df, 'dif_percent_long_to_tp', 'dif_percent_short_to_tp', 'dif_percent_long_to_sl','dif_percent_short_to_sl')
    report_sl_tp_statics(df)
    #graficar_columnas_binarias(df, columna_binaria1='direction_hma_9_short_hma_10_long', columna_binaria2='prediccion')
    pass

if __name__=='__main__':
    # Ejemplos de uso (incluyendo datos con decimales y sin repetición):
    df = pd.read_csv('hma_avax_bckt_10.csv')
    imprimir_modas_extendido(df, 'dif_percent_long_to_tp', 'dif_percent_short_to_tp', 'dif_percent_long_to_sl','dif_percent_short_to_sl')
    report_sl_tp_statics(df)
    
