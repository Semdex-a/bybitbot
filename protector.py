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

        # --- Этап 1: Проверка, активен ли еще ордер ---
        open_orders_resp = self.trader.session.get_open_orders(category="linear", symbol=symbol, orderId=order_id, limit=1)
        
        if open_orders_resp.get('retCode') == 0 and open_orders_resp.get('result', {}).get('list'):
            order_status = open_orders_resp['result']['list'][0].get('orderStatus')
            self.log.info(f"Ордер TP1 для {symbol} все еще активен. Статус: {order_status}. Ожидаем исполнения.")
            return # Ордер еще открыт, выходим

        # --- Этап 2: Если ордер не активен, проверяем историю ---
        history_resp = self.trader.session.get_order_history(category="linear", symbol=symbol, orderId=order_id, limit=1)

        if history_resp.get('retCode') != 0:
            self.log.error(f"Ошибка API при проверке истории ордера {order_id} для {symbol}: {history_resp.get('retMsg')}")
            return

        order_list = history_resp.get('result', {}).get('list', [])
        if not order_list:
            self.log.error(f"Критическая ошибка: ордер {order_id} не найден ни в активных, ни в истории.")
            return

        final_order_status = order_list[0].get('orderStatus')
        self.log.info(f"Ордер TP1 для {symbol} не активен. Финальный статус в истории: {final_order_status}")

        if final_order_status == 'Filled':
            self.log.info(f"TP1 для {symbol} исполнен! Запускаю процедуру управления позицией.")
            
            # Проверяем позицию
            time.sleep(1)  # Увеличиваем задержку
            position = self.trader.get_open_positions(symbol)
            if not position:
                self.log.warning(f"Позиция по {symbol} была закрыта почти одновременно с TP1. Завершаю управление.")
                self.trade_state.remove_state(symbol)
                return

            # Отменяем все стоп-ордера
            self.log.info(f"Отмена всех стоп-ордеров для {symbol}...")
            cancel_success = self.trader.cancel_all_stop_orders(symbol)
            if not cancel_success:
                self.log.error(f"Не удалось отменить стоп-ордера для {symbol}. Прерываю операцию.")
                return
            
            # Увеличиваем задержку после отмены
            time.sleep(2)

            # Повторно проверяем позицию
            position = self.trader.get_open_positions(symbol)
            if not position:
                self.log.warning(f"Позиция по {symbol} была закрыта сразу после отмены SL. Завершаю управление.")
                self.trade_state.remove_state(symbol)
                return
            
            # Получаем информацию об инструменте
            instrument_info = self.trader.get_instrument_info(symbol)
            if not instrument_info:
                self.log.error(f"Не удалось получить информацию об инструменте {symbol}")
                return
                
            tick_size = Decimal(instrument_info['priceFilter']['tickSize'])
            
            # Рассчитываем цены (исправляем логику округления)
            entry_price_decimal = Decimal(str(state['entry_price']))
            tp2_price_decimal = Decimal(str(state['tp2_price']))
            
            # Для SL в безубыток - округляем в безопасную сторону
            if state['side'] == "Buy":
                # Для лонга: SL чуть ниже входа (округляем вниз)
                sl_price_target = entry_price_decimal.quantize(tick_size, rounding=ROUND_DOWN)
                # TP округляем вверх (в нашу пользу)
                tp_price_target = tp2_price_decimal.quantize(tick_size, rounding=ROUND_UP)
            else:
                # Для шорта: SL чуть выше входа (округляем вверх)  
                sl_price_target = entry_price_decimal.quantize(tick_size, rounding=ROUND_UP)
                # TP округляем вниз (в нашу пользу)
                tp_price_target = tp2_price_decimal.quantize(tick_size, rounding=ROUND_DOWN)
            
            # КЛЮЧЕВОЕ ИЗМЕНЕНИЕ: Устанавливаем SL и TP одним вызовом
            self.log.info(f"Установка SL={sl_price_target} и TP2={tp_price_target} для {symbol} одним вызовом")
            set_success = self.trader.set_trading_stop(
                symbol=symbol, 
                side=state['side'], 
                sl_price=str(sl_price_target),
                tp_price=str(tp_price_target)
            )

            if not set_success:
                self.log.error(f"Не удалось установить SL/TP для {symbol}. Проверьте логи трейдера.")
                return

            # Увеличиваем время ожидания для верификации
            time.sleep(5)
            
            # Верификация
            final_position_check = self.trader.get_open_positions(symbol)
            if not final_position_check:
                self.log.warning(f"Позиция по {symbol} закрылась во время верификации. Завершаю.")
                self.trade_state.remove_state(symbol)
                return

            current_sl = final_position_check.get('stopLoss', '')
            current_tp = final_position_check.get('takeProfit', '')

            # Проверяем с небольшой толерантностью из-за возможных расхождений в точности
            sl_ok = current_sl and abs(Decimal(current_sl) - sl_price_target) <= tick_size
            tp_ok = current_tp and abs(Decimal(current_tp) - tp_price_target) <= tick_size

            if sl_ok and tp_ok:
                self.log.info(f"УСПЕХ: SL ({current_sl}) и TP ({current_tp}) для {symbol} успешно установлены.")
                state['state'] = "BE_PENDING"
                state['sl_price'] = float(sl_price_target)
                state['tp2_order_id'] = None  # Сбрасываем TP1, теперь у нас TP2
                self.trade_state.set_state(symbol, state)
            else:
                self.log.critical(f"ОШИБКА ВЕРИФИКАЦИИ для {symbol}!")
                self.log.critical(f"SL: Цель={sl_price_target}, Факт={current_sl}, OK={sl_ok}")
                self.log.critical(f"TP: Цель={tp_price_target}, Факт={current_tp}, OK={tp_ok}")
                self.log.critical(f"ТРЕБУЕТСЯ РУЧНОЕ ВМЕШАТЕЛЬСТВО!")
        
        elif final_order_status == 'Cancelled':
            self.log.warning(f"Ордер TP1 для {symbol} был отменен (вероятно, из-за срабатывания SL). Удаляю сделку из отслеживания.")
            self.trade_state.remove_state(symbol)
        
        else:
            self.log.error(f"Неизвестный финальный статус для ордера {order_id}: {final_order_status}. Требуется анализ.")

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
