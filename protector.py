import logging
import time
from decimal import Decimal, ROUND_DOWN, ROUND_UP
from trader import BybitTrader
from trade_state import TradeState

class Protector:
    def __init__(self, trader: BybitTrader, trade_state_manager: TradeState):
        self.log = logging.getLogger(self.__class__.__name__)
        self.trader = trader
        self.trade_state = trade_state_manager
        self.running = False

    def check_tp1_order(self, symbol: str, state: dict):
        """Проверяет статус лимитного ордера TP1 и управляет переходом в безубыток."""
        if not state.get('tp1_order_id'):
            self.log.debug(f"Для сделки {symbol} нет TP1 ордера для проверки.")
            return

        try:
            resp = self.trader.session.get_open_orders(category="linear", symbol=symbol, orderId=state['tp1_order_id'])
            
            # Если ордер исполнен (не найден в списке активных)
            if resp['retCode'] == 0 and not resp['result']['list']:
                self.log.info(f"TP1 для {symbol} исполнен! Перемещаю SL в безубыток и ставлю TP2.")
                
                instrument_info = self.trader.get_instrument_info(symbol)
                if not instrument_info:
                    self.log.error(f"Не удалось получить инфо для {symbol}, невозможно установить SL/TP.")
                    return

                tick_size = Decimal(instrument_info['priceFilter']['tickSize'])
                
                # Новые уровни SL и TP
                sl_price = Decimal(str(state['entry_price'])).quantize(tick_size, rounding=ROUND_DOWN if state['side'] == "Buy" else ROUND_UP)
                tp_price = Decimal(str(state['tp2_price'])).quantize(tick_size, rounding=ROUND_UP if state['side'] == "Buy" else ROUND_DOWN)
                
                # Устанавливаем новые SL/TP для оставшейся позиции
                if self.trader.set_trading_stop(symbol, str(sl_price), str(tp_price), state['side']):
                    # Обновляем состояние и сохраняем
                    state['state'] = "BE_PENDING"  # BE = BreakEven
                    state['sl_price'] = float(sl_price)
                    self.trade_state.set_state(symbol, state)
                    self.log.info(f"Состояние для {symbol} обновлено на {state['state']} и сохранено.")
                else:
                    self.log.error(f"Не удалось установить новый SL/TP для {symbol} после исполнения TP1.")

        except Exception as e:
            self.log.error(f"Ошибка при проверке TP1 для {symbol}: {e}")

    def run_management_cycle(self):
        """Основной цикл управления открытыми позициями на основе сохраненного состояния."""
        all_states = self.trade_state.get_all_states()
        if not all_states:
            return

        self.log.info(f"Менеджер позиций: проверка {len(all_states)} отслеживаемых сделок...")
        
        symbols_to_remove = []
        for symbol, state in all_states.items():
            position = self.trader.get_open_positions(symbol)
            
            if not position:
                self.log.info(f"Позиция по {symbol} больше не активна. Удаляю из отслеживания.")
                symbols_to_remove.append(symbol)
                continue

            # Проверяем, нужно ли двигать стоп
            if state.get('state') == "TP1_PENDING":
                self.check_tp1_order(symbol, state)

        # Очищаем закрытые сделки из файла состояний
        if symbols_to_remove:
            for symbol in symbols_to_remove:
                self.trade_state.remove_state(symbol)
            self.log.info(f"Удалены закрытые сделки: {', '.join(symbols_to_remove)}")

    def start(self, interval_seconds=15):
        """Запускает бесконечный цикл управления."""
        self.log.info(f"Менеджер позиций будет запускаться каждые {interval_seconds} секунд.")
        self.running = True
        while self.running:
            try:
                self.run_management_cycle()
                time.sleep(interval_seconds)
            except KeyboardInterrupt:
                self.stop()
            except Exception as e:
                self.log.error(f"Критическая ошибка в цикле менеджера: {e}", exc_info=True)
                time.sleep(interval_seconds * 2)

    def stop(self):
        """Останавливает цикл."""
        self.log.info("Остановка менеджера позиций...")
        self.running = False
