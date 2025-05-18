import pandas as pd
import numpy as np

def wma(series, period):
    """Weighted Moving Average (WMA) calculation."""
    weights = np.arange(1, period + 1)
    return series.rolling(period).apply(lambda x: np.dot(x, weights) / weights.sum(), raw=True)

def hma(series, length):
    """Hull Moving Average (HMA) calculation."""
    if length < 1:
        raise ValueError("Length must be at least 1")
    
    half_length = int(length / 2)
    sqrt_length = int(np.sqrt(length))
    
    wma_half = wma(series, half_length)
    wma_full = wma(series, length)
    diff = 2 * wma_half - wma_full
    return wma(diff, sqrt_length)

def eject(df, length_short=10, length_long=50):
    df['hma_short'] = hma(df['Close'], length_short)
    df['hma_long'] = hma(df['Close'], length_long)
    return df

# Ejemplo de uso

if __name__ == "__main__":
    # Datos
    df = pd.read_csv('info_velas.csv')
    # Cálculo de las HMAs
    df = eject(df)
    print(df)
    pass