import pandas as pd
from ta.trend import EMAIndicator, ADXIndicator
from ta.volatility import DonchianChannel, BollingerBands, AverageTrueRange
from ta.volume import MFIIndicator

def apply_strategy(df: pd.DataFrame, dc_window: int = 20, ema_window: int = 50, atr_window: int = 14, bb_window: int = 20, adx_window: int = 14, mfi_window: int = 14):
    """
    Применяет гибридную торговую стратегию, которая адаптируется к состоянию рынка,
    избегая заглядывания в будущее (look-ahead bias).
    """
    if df.empty or len(df) < max(dc_window, ema_window, bb_window, adx_window, mfi_window):
        return pd.DataFrame()

    # 1. Расчет всех необходимых индикаторов
    df['atr'] = AverageTrueRange(df['high'], df['low'], df['close'], window=atr_window).average_true_range()
    adx_indicator = ADXIndicator(df['high'], df['low'], df['close'], window=adx_window)
    df['adx'] = adx_indicator.adx()
    dc = DonchianChannel(df['high'], df['low'], df['close'], window=dc_window)
    df['dc_upper'] = dc.donchian_channel_hband()
    df['dc_lower'] = dc.donchian_channel_lband()
    df['ema_fast'] = EMAIndicator(df['close'], window=ema_window).ema_indicator()
    bb = BollingerBands(df['close'], window=bb_window, window_dev=2)
    df['bb_upper'] = bb.bollinger_hband()
    df['bb_lower'] = bb.bollinger_lband()
    df['bb_mid'] = bb.bollinger_mavg()
    df['mfi'] = MFIIndicator(df['high'], df['low'], df['close'], df['volume'], window=mfi_window).money_flow_index()

    df.dropna(inplace=True)
    if df.empty:
        return df

    # 2. Логика сигналов (без look-ahead bias)
    df['signal'] = 0
    df['strategy_name'] = 'None'

    # --- Определяем условия для каждой стратегии для всего DataFrame ---
    
    # Условия для трендовой стратегии
    is_trending = df['adx'] > 25
    breakout_up = (df['close'].shift(1) < df['dc_upper'].shift(1)) & (df['close'] > df['dc_upper'])
    breakout_down = (df['close'].shift(1) > df['dc_lower'].shift(1)) & (df['close'] < df['dc_lower'])
    uptrend = df['close'] > df['ema_fast']
    downtrend = df['close'] < df['ema_fast']
    volume_confirm_up_trend = df['mfi'] > 50
    volume_confirm_down_trend = df['mfi'] < 50
    
    trend_buy_condition = is_trending & breakout_up & uptrend & volume_confirm_up_trend
    trend_sell_condition = is_trending & breakout_down & downtrend & volume_confirm_down_trend

    # Условия для боковой стратегии
    is_ranging = df['adx'] < 20
    touch_lower = df['low'] <= df['bb_lower']
    touch_upper = df['high'] >= df['bb_upper']
    volume_confirm_up_range = df['mfi'] < 20
    volume_confirm_down_range = df['mfi'] > 80

    range_buy_condition = is_ranging & touch_lower & volume_confirm_up_range
    range_sell_condition = is_ranging & touch_upper & volume_confirm_down_range

    # --- Применяем сигналы и расчеты SL/TP ---
    
    # Трендовые сигналы
    df.loc[trend_buy_condition, 'signal'] = 1
    df.loc[trend_sell_condition, 'signal'] = -1
    df.loc[trend_buy_condition | trend_sell_condition, 'strategy_name'] = 'TREND'
    df.loc[trend_buy_condition, 'stop_loss'] = df['dc_upper'] - (df['atr'] * 0.5)
    df.loc[trend_buy_condition, 'tp1'] = df['close'] + (df['atr'] * 3.0)
    df.loc[trend_buy_condition, 'tp2'] = df['close'] + (df['atr'] * 6.0)
    df.loc[trend_sell_condition, 'stop_loss'] = df['dc_lower'] + (df['atr'] * 0.5)
    df.loc[trend_sell_condition, 'tp1'] = df['close'] - (df['atr'] * 3.0)
    df.loc[trend_sell_condition, 'tp2'] = df['close'] - (df['atr'] * 6.0)

    # Боковые сигналы
    df.loc[range_buy_condition, 'signal'] = 1
    df.loc[range_sell_condition, 'signal'] = -1
    df.loc[range_buy_condition | range_sell_condition, 'strategy_name'] = 'RANGE'
    df.loc[range_buy_condition, 'stop_loss'] = df['close'] - (df['atr'] * 2.0)
    df.loc[range_buy_condition, 'tp1'] = df['bb_mid']
    df.loc[range_buy_condition, 'tp2'] = df['bb_mid'] # Для боковика TP1 и TP2 одинаковы
    df.loc[range_sell_condition, 'stop_loss'] = df['close'] + (df['atr'] * 2.0)
    df.loc[range_sell_condition, 'tp1'] = df['bb_mid']
    df.loc[range_sell_condition, 'tp2'] = df['bb_mid']

    return df
