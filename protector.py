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
            order_id = state['tp1_order_id']
            self.log.info(f"Проверка статуса ордера TP1 {order_id} для {symbol}...")
            
            # Используем запрос истории ордеров, так как он показывает и исполненные ордера
            resp = self.trader.session.get_order_history(
                category="linear", 
                symbol=symbol, 
                orderId=order_id,
                limit=1
            )
            
            if resp['retCode'] != 0:
                self.log.error(f"Ошибка API при проверке ордера {order_id} для {symbol}: {resp.get('retMsg')}")
                return

            order_list = resp.get('result', {}).get('list', [])
            if not order_list:
                self.log.warning(f"Не удалось найти историю для ордера {order_id}. Возможно, он еще не исполнен.")
                return

            latest_order_status = order_list[0].get('orderStatus')
            self.log.info(f"Текущий статус ордера TP1 для {symbol} на бирже: {latest_order_status}")

            # Если ордер исполнен
            if latest_order_status == 'Filled':
                self.log.info(f"TP1 для {symbol} исполнен! Запускаю процедуру управления позицией.")
                
                # --- Этап 1: Проверка, не закрылась ли позиция по SL одновременно с TP1 ---
                time.sleep(0.5) # Небольшая пауза, чтобы дать состоянию на бирже синхронизироваться
                position = self.trader.get_open_positions(symbol)
                if not position:
                    self.log.warning(f"Позиция по {symbol} была закрыта (вероятно, по SL) почти одновременно с TP1. Завершаю управление.")
                    self.trade_state.remove_state(symbol)
                    return

                # --- Этап 2: Отмена старого стоп-лосса ---
                self.trader.cancel_all_stop_orders(symbol)
                time.sleep(0.5)

                # --- Этап 3: Повторная проверка и получение актуального размера ---
                position = self.trader.get_open_positions(symbol)
                if not position:
                    self.log.warning(f"Позиция по {symbol} была закрыта сразу после отмены SL. Завершаю управление.")
                    self.trade_state.remove_state(symbol)
                    return
                
                remaining_size = position['size']
                self.log.info(f"Оставшийся размер позиции {symbol}: {remaining_size}")

                # --- Этап 4: Установка нового SL/TP ---
                instrument_info = self.trader.get_instrument_info(symbol)
                tick_size = Decimal(instrument_info['priceFilter']['tickSize'])
                
                sl_price_target = Decimal(str(state['entry_price'])).quantize(tick_size, rounding=ROUND_DOWN if state['side'] == "Buy" else ROUND_UP)
                tp_price_target = Decimal(str(state['tp2_price'])).quantize(tick_size, rounding=ROUND_UP if state['side'] == "Buy" else ROUND_DOWN)
                
                self.trader.set_trading_stop(
                    symbol=symbol, 
                    side=state['side'], 
                    sl_price=str(sl_price_target), 
                    tp_price=str(tp_price_target)
                )

                # --- Этап 5: Верификация ---
                time.sleep(3)
                
                final_position_check = self.trader.get_open_positions(symbol)
                if not final_position_check:
                    self.log.warning(f"Позиция по {symbol} закрылась сразу после установки нового SL. Завершаю.")
                    self.trade_state.remove_state(symbol)
                    return

                current_sl = final_position_check.get('stopLoss', '')
                
                if current_sl and Decimal(current_sl) == sl_price_target:
                    self.log.info(f"УСПЕХ: Стоп-лосс для {symbol} успешно перемещен на {current_sl}.")
                    state['state'] = "BE_PENDING"
                    state['sl_price'] = float(sl_price_target)
                    self.trade_state.set_state(symbol, state)
                else:
                    self.log.critical(f"ОШИБКА ВЕРИФИКАЦИИ: Не удалось переместить стоп-лосс для {symbol}! "
                                      f"Цель: {sl_price_target}, Текущий SL на бирже: {current_sl}. "
                                      f"ТРЕБУЕТСЯ РУЧНОЕ ВМЕШАТЕЛЬСТВО!")
            
        except Exception as e:
            self.log.error(f"Критическая ошибка в check_tp1_order для {symbol}: {e}", exc_info=True)

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
