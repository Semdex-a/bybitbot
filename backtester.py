import os
import time
import logging
import pandas as pd
from pybit.unified_trading import HTTP
from dotenv import load_dotenv
from strategy import apply_strategy

# --- Настройка ---
load_dotenv()
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
EMA_WINDOW = 100 # Определяем окно и здесь для консистентности

# --- Клиент Bybit ---
try:
    api_key = os.getenv("BYBIT_API_KEY")
    api_secret = os.getenv("BYBIT_API_SECRET")
    if not api_key or not api_secret:
        raise ValueError("BYBIT_API_KEY и BYBIT_API_SECRET должны быть установлены в .env для бэктестинга")
    
    session = HTTP(
        testnet=os.getenv("TESTNET", "false").lower() == "true",
        api_key=api_key,
        api_secret=api_secret,
    )
except (ValueError, TypeError) as e:
    logging.error(f"Ошибка конфигурации: {e}")
    exit()

def fetch_historical_data(symbol, interval='60', limit=1000):
    """Загружает исторические данные с Bybit."""
    logging.info(f"Загрузка исторических данных для {symbol}...")
    
    # Bybit отдает данные в обратном порядке, до 1000 свечей за раз
    response = session.get_kline(
        category="linear",
        symbol=symbol,
        interval=interval,
        limit=limit
    )

    if response['retCode'] != 0:
        logging.error(f"Ошибка API Bybit для {symbol}: {response['retMsg']}")
        return pd.DataFrame()

    data = response['result']['list']
    if not data:
        logging.warning(f"Нет данных для {symbol}")
        return pd.DataFrame()

    # Создаем DataFrame
    df = pd.DataFrame(data, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume', 'turnover'])
    df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
    df.set_index('timestamp', inplace=True)
    
    # Bybit отдает данные от новых к старым, переворачиваем
    df = df.iloc[::-1] 
    
    # Конвертируем колонки в числовой формат
    for col in ['open', 'high', 'low', 'close', 'volume']:
        df[col] = pd.to_numeric(df[col])
        
    logging.info(f"Загружено {len(df)} свечей для {symbol}.")
    return df

def run_backtest(df: pd.DataFrame, symbol: str):
    """Запускает симуляцию торгов и выводит статистику."""
    if 'signal' not in df.columns or df[df['signal'] != 0].empty:
        logging.warning(f"Для {symbol} не найдено сигналов для бэктеста.")
        return

    trades = []
    active_trade = None

    for i, row in df.iterrows():
        # Логика выхода из сделки
        if active_trade:
            if active_trade['type'] == 'buy':
                if row['low'] <= active_trade['stop_loss']:
                    active_trade['exit_price'] = active_trade['stop_loss']
                    active_trade['exit_date'] = i
                    trades.append(active_trade)
                    active_trade = None
                elif row['high'] >= active_trade['take_profit']:
                    active_trade['exit_price'] = active_trade['take_profit']
                    active_trade['exit_date'] = i
                    trades.append(active_trade)
                    active_trade = None
            elif active_trade['type'] == 'sell':
                if row['high'] >= active_trade['stop_loss']:
                    active_trade['exit_price'] = active_trade['stop_loss']
                    active_trade['exit_date'] = i
                    trades.append(active_trade)
                    active_trade = None
                elif row['low'] <= active_trade['take_profit']:
                    active_trade['exit_price'] = active_trade['take_profit']
                    active_trade['exit_date'] = i
                    trades.append(active_trade)
                    active_trade = None
        
        # Логика входа в сделку
        if not active_trade and row['signal'] != 0:
            active_trade = {
                'type': 'buy' if row['signal'] == 1 else 'sell',
                'entry_price': row['close'],
                'entry_date': i,
                'stop_loss': row['stop_loss'],
                'take_profit': row['take_profit']
            }

    # --- Анализ результатов ---
    if not trades:
        logging.warning(f"Для {symbol} не было совершено ни одной полной сделки.")
        return

    results = pd.DataFrame(trades)
    results['pnl'] = results.apply(
        lambda x: (x['exit_price'] - x['entry_price']) if x['type'] == 'buy' else (x['entry_price'] - x['exit_price']),
        axis=1
    )
    results['pnl_percent'] = (results['pnl'] / results['entry_price']) * 100

    wins = results[results['pnl'] > 0]
    losses = results[results['pnl'] <= 0]

    print("\n" + "="*50)
    print(f"РЕЗУЛЬТАТЫ БЭКТЕСТА ДЛЯ: {symbol}")
    print("="*50)
    print(f"Всего сделок: {len(results)}")
    if not wins.empty:
        print(f"Прибыльных сделок: {len(wins)} ({len(wins)/len(results):.2%})")
        print(f"Средняя прибыль: {wins['pnl_percent'].mean():.2f}%")
    if not losses.empty:
        print(f"Убыточных сделок: {len(losses)}")
        print(f"Средний убыток: {losses['pnl_percent'].mean():.2f}%")
    
    total_pnl_percent = results['pnl_percent'].sum()
    profit_factor = wins['pnl'].sum() / abs(losses['pnl'].sum()) if not losses.empty and losses['pnl'].sum() != 0 else float('inf')
    
    print(f"Итоговая прибыль/убыток: {total_pnl_percent:.2f}%")
    print(f"Профит-фактор: {profit_factor:.2f}")
    print("="*50 + "\n")


def main():
    symbols_str = os.getenv("SYMBOLS", "BTCUSDT,ETHUSDT")
    symbols = [s.strip() for s in symbols_str.split(',')]

    for symbol in symbols:
        # Загружаем часовые данные за последние ~40 дней
        historical_df = fetch_historical_data(symbol, interval='60', limit=1000)
        if historical_df.empty:
            continue
        
        # Применяем стратегию
        strategy_df = apply_strategy(historical_df, ema_window=EMA_WINDOW)
        
        # Запускаем бэктест
        run_backtest(strategy_df, symbol)
        time.sleep(1) # Чтобы не превысить лимиты API

if __name__ == "__main__":
    main()
