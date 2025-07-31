import os
import time
import logging
import threading
import asyncio
from collections import deque
import pandas as pd
from pybit.unified_trading import WebSocket, HTTP
from telegram import Bot
from dotenv import load_dotenv
from strategy import apply_strategy
from trader import BybitTrader
from protector import Protector
from trade_state import TradeState

# --- Конфигурация ---
load_dotenv()

# Загружаем переменные окружения
SYMBOLS_STR = os.getenv("SYMBOLS", "BTCUSDT,ETHUSDT")
SYMBOLS = [symbol.strip() for symbol in SYMBOLS_STR.split(',')]
TESTNET = os.getenv("TESTNET", "false").lower() == "true"
ENABLE_TRADING = os.getenv("ENABLE_TRADING", "false").lower() == "true"
LEVERAGE = int(os.getenv("LEVERAGE", "5"))
TREND_RISK = float(os.getenv("TREND_RISK_PER_TRADE_PERCENT", "1"))
RANGE_RISK = float(os.getenv("RANGE_RISK_PER_TRADE_PERCENT", "0.5"))
PARTIAL_TP_PERCENT = int(os.getenv("PARTIAL_TP_PERCENT", "50"))

# Общие параметры стратегии
TIMEFRAME_MINUTES = 1
DC_WINDOW = 20
EMA_WINDOW = 50
BB_WINDOW = 20
ADX_WINDOW = 14
COOLDOWN_PERIOD = 300

# --- Настройка логгирования ---
class SymbolFilter(logging.Filter):
    def filter(self, record):
        if not hasattr(record, 'symbol'):
            record.symbol = 'SYSTEM'
        return True

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - [%(symbol)s] - %(message)s')
logger = logging.getLogger(__name__)
for handler in logging.root.handlers:
    handler.addFilter(SymbolFilter())

# --- Инициализация ---
trade_state_manager = TradeState()
http_session = None
trader = None
tg_bot = None

try:
    api_key = os.getenv("BYBIT_API_KEY")
    api_secret = os.getenv("BYBIT_API_SECRET")
    if ENABLE_TRADING and (not api_key or not api_secret):
        raise ValueError("BYBIT_API_KEY и BYBIT_API_SECRET должны быть установлены для торговли.")
    
    http_session = HTTP(testnet=TESTNET, api_key=api_key, api_secret=api_secret)
    trader = BybitTrader(http_session, LEVERAGE, PARTIAL_TP_PERCENT, trade_state_manager)

    telegram_token = os.getenv("TELEGRAM_TOKEN")
    chat_id = os.getenv("CHAT_ID")
    if not telegram_token or not chat_id:
        logger.warning("TELEGRAM_TOKEN или CHAT_ID не найдены. Уведомления будут отключены.")
    else:
        tg_bot = Bot(telegram_token)

except (ValueError, TypeError) as e:
    logger.error(f"Критическая ошибка конфигурации: {e}", extra={'symbol': 'SYSTEM'})
    exit()

# --- Asynchronous Event Loop for Telegram ---
def run_async_loop(loop):
    asyncio.set_event_loop(loop)
    loop.run_forever()

async_loop = asyncio.new_event_loop()
async_thread = threading.Thread(target=run_async_loop, args=(async_loop,), daemon=True)
async_thread.start()

def send_telegram_signal(text: str):
    if not tg_bot: return
    
    async def send_message_async():
        try:
            await tg_bot.send_message(chat_id=chat_id, text=text, parse_mode='Markdown')
        except Exception as e:
            logger.error(f"Ошибка отправки в Telegram: {e}", extra={'symbol': 'TELEGRAM'})
    
    asyncio.run_coroutine_threadsafe(send_message_async(), async_loop)

# --- Класс для отслеживания каждого символа ---
class SymbolTracker:
    def __init__(self, symbol: str, cooldown_period: int, trader_instance: BybitTrader, http_session_instance: HTTP):
        self.symbol = symbol
        self.cooldown_period = cooldown_period
        self.trader = trader_instance
        self.http_session = http_session_instance
        self.klines = deque(maxlen=max(DC_WINDOW, EMA_WINDOW, BB_WINDOW, ADX_WINDOW) + 50)
        self.last_signal_time = 0
        self.log_adapter = logging.LoggerAdapter(logger, {'symbol': self.symbol})

    def preload_history(self):
        """Предзагружает исторические данные для быстрого старта."""
        try:
            limit = max(DC_WINDOW, EMA_WINDOW, BB_WINDOW, ADX_WINDOW) + 5
            self.log_adapter.info(f"Загрузка последних {limit} свечей...")
            
            resp = self.http_session.get_kline(
                category="linear",
                symbol=self.symbol,
                interval=TIMEFRAME_MINUTES,
                limit=limit
            )

            if resp['retCode'] == 0 and resp['result']['list']:
                history = resp['result']['list']
                history.reverse() # API отдает от новых к старым, нам нужен обратный порядок

                for kline in history:
                    self.klines.append({
                        'timestamp': int(kline[0]),
                        'open': float(kline[1]),
                        'high': float(kline[2]),
                        'low': float(kline[3]),
                        'close': float(kline[4]),
                        'volume': float(kline[5])
                    })
                self.log_adapter.info(f"Успешно загружено {len(self.klines)} исторических свечей.")
            else:
                self.log_adapter.error(f"Не удалось загрузить историю: {resp.get('retMsg', 'Unknown error')}")

        except Exception as e:
            self.log_adapter.error(f"Исключение при загрузке истории: {e}", exc_info=True)

    def add_kline(self, kline_data: dict):
        """Добавляет завершенную свечу и запускает анализ, избегая дубликатов."""
        # Защита от дубликатов: новая свеча должна иметь более поздний timestamp
        if self.klines and kline_data['timestamp'] <= self.klines[-1]['timestamp']:
            return

        self.klines.append(kline_data)
        self.log_adapter.debug(f"Новая свеча: O:{kline_data['open']} H:{kline_data['high']} L:{kline_data['low']} C:{kline_data['close']} V:{kline_data['volume']}")
        self.analyze()

    def analyze(self):
        required_klines = max(DC_WINDOW, EMA_WINDOW, BB_WINDOW, ADX_WINDOW)
        if len(self.klines) < required_klines:
            self.log_adapter.info(f"Собрано {len(self.klines)}/{required_klines} свечей для анализа.")
            return

        df = pd.DataFrame(list(self.klines))
        for col in ['open', 'high', 'low', 'close', 'volume']:
            df[col] = pd.to_numeric(df[col])
            
        strategy_df = apply_strategy(df.copy())
        
        if strategy_df.empty: return
        
        last_row = strategy_df.iloc[-1]
        signal = last_row.get('signal', 0)
        
        self.log_adapter.info(f"Анализ: Цена={last_row['close']:,.4f}, ADX={last_row['adx']:.1f}, Стратегия='{last_row['strategy_name']}', Сигнал={int(signal)}")

        current_time = time.time()
        if signal != 0 and (current_time - self.last_signal_time) >= self.cooldown_period:
            self.log_adapter.info(f"СИГНАЛ {int(signal)} по стратегии {last_row['strategy_name']}!")
            
            signal_data = last_row.copy()
            signal_data['symbol'] = self.symbol
            risk_percent = TREND_RISK if last_row['strategy_name'] == 'TREND' else RANGE_RISK
            
            if ENABLE_TRADING and self.trader:
                trade_result = self.trader.execute_trade(signal_data, risk_percent)
                if trade_result:
                    message = self.format_trade_confirmation_message(trade_result, last_row['strategy_name'])
                    send_telegram_signal(message)
            else:
                message = self.format_simulation_message(signal, last_row['strategy_name'])
                send_telegram_signal(message)

            self.last_signal_time = current_time

    def format_trade_confirmation_message(self, trade_result: dict, strategy_name: str) -> str:
        side_text = "🟢 ПОКУПКА" if trade_result['side'] == 'Buy' else "🔴 ПРОДАЖА"
        entry_price = float(trade_result['entry_price'])
        return (f"**РЕАЛЬНАЯ СДЕЛКА ({strategy_name})**\n\n"
                f"**Инструмент:** `{trade_result['symbol']}`\n"
                f"**Направление:** {side_text}\n"
                f"**Объем:** `{trade_result['qty']}`\n"
                f"**Цена входа:** `~{entry_price:,.4f}`\n\n"
                f"ID ордера: `{trade_result['order_id']}`")

    def format_simulation_message(self, signal: int, strategy_name: str) -> str:
        side_text = "🟢 СИГНАЛ НА ПОКУПКУ" if signal == 1 else "🔴 СИГНАЛ НА ПРОДАЖУ"
        return (f"**СИГНАЛ (СИМУЛЯЦИЯ) ({strategy_name})**\n\n"
                f"**Инструмент:** `{self.symbol}`\n"
                f"**Направление:** {side_text}")

# --- Main ---
trackers = {symbol: SymbolTracker(symbol, COOLDOWN_PERIOD, trader, http_session) for symbol in SYMBOLS}

def handle_kline_message(msg: dict):
    try:
        if msg.get('topic', '').startswith('kline.'):
            kline_data_list = msg.get('data', [])
            if not kline_data_list: return

            for kline in kline_data_list:
                if kline.get('confirm', False):
                    symbol = msg['topic'].split('.')[-1]
                    if symbol in trackers:
                        trackers[symbol].add_kline({
                            'timestamp': int(kline['start']),
                            'open': float(kline['open']),
                            'high': float(kline['high']),
                            'low': float(kline['low']),
                            'close': float(kline['close']),
                            'volume': float(kline['volume'])
                        })
                
    except KeyError as e:
        logger.warning(f"Отсутствует ключ в сообщении kline: {e}", extra={'symbol': 'WEBSOCKET'})
    except (ValueError, TypeError) as e:
        logger.error(f"Ошибка преобразования данных из kline: {e}", extra={'symbol': 'WEBSOCKET'})
    except Exception as e:
        logger.error(f"Неизвестная ошибка обработки kline: {e}", extra={'symbol': 'WEBSOCKET'}, exc_info=True)

def main():
    logger.info(f"Запуск гибридного бота для: {', '.join(SYMBOLS)}", extra={'symbol': 'SYSTEM'})
    
    # --- Предзагрузка истории ---
    for symbol, tracker in trackers.items():
        tracker.preload_history()
        time.sleep(0.5) # Небольшая задержка между запросами к API

    protector_thread = None
    protector = None
    if ENABLE_TRADING and trader:
        logger.warning("!!! РЕЖИМ РЕАЛЬНОЙ ТОРГОВЛИ АКТИВИРОВАН !!!", extra={'symbol': 'SYSTEM'})
        protector = Protector(trader, trade_state_manager)
        protector_thread = threading.Thread(target=protector.start, daemon=True)
        protector_thread.start()

    ws = WebSocket(testnet=TESTNET, channel_type="linear")
    for symbol in SYMBOLS:
        ws.kline_stream(symbol=symbol, interval=TIMEFRAME_MINUTES, callback=handle_kline_message)
    
    logger.info("Успешно подписались на все потоки kline.", extra={'symbol': 'SYSTEM'})
    
    try:
        while True:
            time.sleep(60)
    except KeyboardInterrupt:
        logger.info("Завершение работы...")
    finally:
        if protector:
            protector.stop()
            if protector_thread:
                protector_thread.join()
        async_loop.call_soon_threadsafe(async_loop.stop)
        logger.info("Бот остановлен.")

if __name__ == "__main__":
    main()
