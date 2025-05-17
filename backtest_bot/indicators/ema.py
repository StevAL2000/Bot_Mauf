import pandas as pd

def calcular_ema(df, length_short=10, length_long=55, number=None):
    def ema(series, period):
        """Exponential Moving Average (EMA) calculation."""
        alpha = 2 / (period + 1)  # Smoothing factor
        return series.ewm(alpha=alpha, min_periods=period, adjust=False).mean()
    
    # Cálculo de las EMAs
    df[f'ema_short{number}' if number else 'ema_short'] = ema(df['Close'], length_short)
    df[f'ema_long{number+1}' if number else 'ema_long'] = ema(df['Close'], length_long)
    
    #df.to_csv('emas_and_dates.csv', index=False)
    return df

if __name__ == "__main__":
    date = pd.read_csv('info_velas.csv')
    emas = calcular_ema(date, length_short=10, length_long=55)
    print(emas)
