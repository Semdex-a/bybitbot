import logging
import time
import pandas as pd
from pybit.unified_trading import HTTP
from decimal import Decimal, ROUND_DOWN, ROUND_UP
from trade_state import TradeState

class BybitTrader:
    def __init__(self, session: HTTP, leverage: int, partial_tp_percent: int, trade_state_manager: TradeState):
        self.session = session
        self.leverage = leverage
        self.partial_tp_percent = partial_tp_percent
        self.trade_state = trade_state_manager
        self.instrument_info_cache = {}
        self.log = logging.getLogger(__name__)
        self.initialized_symbols = set()

    def get_instrument_info(self, symbol: str):
        """Получает и кэширует информацию об инструменте."""
        if symbol in self.instrument_info_cache and (time.time() - self.instrument_info_cache[symbol]['timestamp'] < 3600):
            return self.instrument_info_cache[symbol]['data']
        
        self.log.info(f"Получение информации об инструменте для {symbol}...")
        try:
            resp = self.session.get_instruments_info(category="linear", symbol=symbol)
            if resp['retCode'] == 0 and resp['result']['list']:
                info = resp['result']['list'][0]
                self.instrument_info_cache[symbol] = {'timestamp': time.time(), 'data': info}
                return info
            self.log.error(f"Ошибка в ответе API при получении информации для {symbol}: {resp.get('retMsg')}")
            return None
        except Exception as e:
            self.log.error(f"Исключение при получении информации для {symbol}: {e}")
            return None

    def get_balance(self, coin="USDT"):
        """Получает доступный баланс кошелька."""
        try:
            resp = self.session.get_wallet_balance(accountType="UNIFIED", coin=coin)
            if resp['retCode'] == 0 and resp['result']['list']:
                for acc in resp['result']['list']:
                    if acc['accountType'] == "UNIFIED":
                        balance = float(acc['totalWalletBalance'])
                        self.log.info(f"Баланс Единого Аккаунта: {balance:.2f} {coin}")
                        return balance
                self.log.warning("Не найден баланс для Единого Аккаунта.")
                return 0
            self.log.error(f"Ошибка в ответе API при получении баланса: {resp.get('retMsg')}")
            return 0
        except Exception as e:
            self.log.error(f"Исключение при получении баланса: {e}")
            return 0

    def set_leverage(self, symbol: str):
        """Устанавливает кредитное плечо."""
        self.log.info(f"Установка плеча {self.leverage}x для {symbol}...")
        try:
            self.session.set_leverage(category="linear", symbol=symbol, buyLeverage=str(self.leverage), sellLeverage=str(self.leverage))
        except Exception as e:
            if "leverage not modified" in str(e).lower():
                self.log.info(f"Плечо для {symbol} уже было установлено.")
            else:
                self.log.error(f"Ошибка установки плеча для {symbol}: {e}")

    def calculate_position_size(self, approx_entry_price: float, stop_loss_price: float, symbol: str, lot_size_filter, risk_percent: float):
        """Рассчитывает размер позиции с тройным контролем: по риску, по лимиту маржи и по мин. ордеру."""
        balance = self.get_balance()
        if balance <= 0: return None

        margin_limit_percent = 0.20
        allowed_margin = balance * margin_limit_percent
        max_position_value = allowed_margin * self.leverage
        qty_by_margin_limit = max_position_value / approx_entry_price

        risk_amount = balance * (risk_percent / 100)
        price_risk_per_unit = abs(approx_entry_price - stop_loss_price)
        if price_risk_per_unit == 0: 
            self.log.error("Цена входа и стоп-лосс совпадают, невозможно рассчитать риск.")
            return None
        qty_by_risk = risk_amount / price_risk_per_unit
        
        final_qty = min(qty_by_risk, qty_by_margin_limit)
        self.log.info(f"Расчет размера (риск={risk_percent}%): по риску={qty_by_risk:.4f}, по лимиту маржи={qty_by_margin_limit:.4f}. Выбран меньший: {final_qty:.4f}")

        min_order_qty = float(lot_size_filter['minOrderQty'])
        qty_step = Decimal(lot_size_filter['qtyStep'])

        if final_qty < min_order_qty:
            self.log.warning(f"Расчетный объем {final_qty:.6f} меньше минимального {min_order_qty}. Сделка отменена.")
            return None
        
        return float(Decimal(str(final_qty)).quantize(qty_step, rounding=ROUND_DOWN))

    def place_market_order(self, symbol: str, side: str, qty: str):
        """Размещает рыночный ордер БЕЗ SL/TP, правильно указывая positionIdx."""
        self.log.info(f"Этап 1: Размещение рыночного ордера: {side} {qty} {symbol}")
        
        position_idx = 1 if side == "Buy" else 2
        
        try:
            resp = self.session.place_order(
                category="linear", symbol=symbol, side=side, orderType="Market",
                qty=str(qty), positionIdx=position_idx
            )
            if resp['retCode'] == 0:
                order_id = resp['result']['orderId']
                self.log.info(f"Рыночный ордер {order_id} успешно отправлен.")
                return order_id
            else:
                self.log.error(f"Ошибка размещения рыночного ордера: {resp.get('retMsg', 'Unknown error')}")
                return None
        except Exception as e:
            self.log.error(f"Исключение при размещении рыночного ордера: {e}")
            return None

    def set_trading_stop(self, symbol: str, side: str, sl_price: str = None, tp_price: str = None):
        """Устанавливает SL/TP для существующей позиции. Отправляет только необходимые параметры."""
        self.log.info(f"Установка SL={sl_price} TP={tp_price} для {symbol}")
    
        if not sl_price and not tp_price:
            self.log.warning("Не указаны ни SL, ни TP. Операция отменена.")
            return False
    
        position_idx = 1 if side == "Buy" else 2
    
        params = {
            "category": "linear",
            "symbol": symbol,
            "positionIdx": position_idx
        }
        if sl_price:
            params["stopLoss"] = sl_price
        if tp_price:
            params["takeProfit"] = tp_price

        self.log.info(f"Отправка запроса на установку SL/TP с параметрами: {params}")

        try:
            resp = self.session.set_trading_stop(**params)
        
            self.log.info(f"Ответ API для установки SL/TP: retCode={resp.get('retCode')}, retMsg={resp.get('retMsg')}")
        
            if resp['retCode'] == 0:
                self.log.info(f"SL/TP для {symbol} успешно установлены/изменены.")
                return True
            else:
                error_msg = resp.get('retMsg', 'Unknown error')
                self.log.error(f"Ошибка установки SL/TP для {symbol}: {error_msg}")
            
                # Логируем дополнительную информацию для диагностики
                if 'result' in resp:
                    self.log.error(f"Дополнительная информация: {resp['result']}")
            
                return False
            
        except Exception as e:
            self.log.error(f"Исключение при установке SL/TP для {symbol}: {e}", exc_info=True)
            return False

            
    

    def place_reduce_only_limit_order(self, symbol: str, side: str, qty: str, price: str, original_side: str):
        """Размещает лимитный ордер на частичное закрытие."""
        self.log.info(f"Размещение Reduce-Only ордера: {side} {qty} {symbol} @ {price}")
        # `positionIdx` должен соответствовать ИСХОДНОЙ позиции, а не стороне ордера на закрытие
        position_idx = 1 if original_side == "Buy" else 2
        try:
            resp = self.session.place_order(
                category="linear", symbol=symbol, side=side, orderType="Limit",
                qty=str(qty), price=str(price), positionIdx=position_idx, reduceOnly=True
            )
            if resp['retCode'] == 0:
                order_id = resp['result']['orderId']
                self.log.info(f"Reduce-Only ордер {order_id} успешно размещен.")
                return order_id
            else:
                self.log.error(f"Ошибка размещения Reduce-Only ордера: {resp.get('retMsg')}")
                return None
        except Exception as e:
            self.log.error(f"Исключение при размещении Reduce-Only ордера: {e}")
            return None

    def cancel_all_stop_orders(self, symbol: str, max_retries: int = 3):
        """Отменяет все стоп-ордера (SL/TP) для символа с проверкой результата."""
        self.log.info(f"Отмена всех стоп-ордеров для {symbol}...")
    
        for attempt in range(max_retries):
            try:
                # Сначала получаем список всех активных стоп-ордеров
                open_orders_resp = self.session.get_open_orders(
                    category="linear",
                    symbol=symbol,
                    orderFilter="StopOrder"
                )
            
                if open_orders_resp.get('retCode') != 0:
                    self.log.error(f"Ошибка получения стоп-ордеров для {symbol}: {open_orders_resp.get('retMsg')}")
                    continue
                
                active_stop_orders = open_orders_resp.get('result', {}).get('list', [])
            
                if not active_stop_orders:
                    self.log.info(f"Нет активных стоп-ордеров для {symbol}.")
                    return True
            
                self.log.info(f"Найдено {len(active_stop_orders)} активных стоп-ордеров для {symbol}. Отменяем...")
            
                # Отменяем все стоп-ордера
                resp = self.session.cancel_all_orders(
                    category="linear",
                    symbol=symbol,
                    orderFilter="StopOrder"
                )
            
                self.log.info(f"Ответ API на отмену стоп-ордеров: retCode={resp.get('retCode')}, retMsg={resp.get('retMsg')}")
            
                if resp.get('retCode') == 0:
                    # Проверяем, что ордера действительно отменены
                    time.sleep(1)
                
                    verify_resp = self.session.get_open_orders(
                        category="linear",
                        symbol=symbol,
                        orderFilter="StopOrder"
                    )
                
                    if verify_resp.get('retCode') == 0:
                        remaining_orders = verify_resp.get('result', {}).get('list', [])
                        if not remaining_orders:
                            self.log.info(f"Все стоп-ордера для {symbol} успешно отменены (попытка {attempt + 1}).")
                            return True
                        else:
                            self.log.warning(f"Осталось {len(remaining_orders)} неотмененных стоп-ордеров для {symbol}")
                            if attempt < max_retries - 1:
                                time.sleep(2)  # Ждем перед повторной попыткой
                                continue
                    else:
                        self.log.error(f"Не удалось проверить статус отмены для {symbol}")
                    
                else:
                    error_msg = resp.get('retMsg', 'Unknown error')
                    # Ошибки "no orders to cancel" являются нормальными
                    if "no orders to cancel" in error_msg.lower():
                        self.log.info(f"Нет активных стоп-ордеров для отмены по {symbol}.")
                        return True
                    else:
                        self.log.error(f"Ошибка при отмене стоп-ордеров для {symbol}: {error_msg}")
                        if attempt < max_retries - 1:
                            time.sleep(2)
                            continue
                        
            except Exception as e:
                self.log.error(f"Исключение при отмене стоп-ордеров для {symbol} (попытка {attempt + 1}): {e}")
                if attempt < max_retries - 1:
                    time.sleep(2)
                    continue
    
        self.log.error(f"Не удалось отменить стоп-ордера для {symbol} после {max_retries} попыток")
        return False

    def get_open_positions(self, symbol: str):
        """Проверяет наличие открытых позиций."""
        try:
            resp = self.session.get_positions(category="linear", symbol=symbol)
            if resp['retCode'] == 0 and resp['result']['list']:
                for pos in resp['result']['list']:
                    if float(pos['size']) > 0:
                        return pos
            return None
        except Exception as e:
            self.log.error(f"Исключение при проверке позиций: {e}")
            return None

    def switch_position_mode(self, symbol: str):
        """Проверяет и при необходимости переключает режим позиции в Hedge Mode."""
        self.log.info(f"Проверка режима позиции для {symbol}...")
        try:
            resp = self.session.get_positions(category="linear", symbol=symbol)
            if resp['retCode'] == 0 and resp['result']['list']:
                # В режиме хеджирования для одного символа будет два ответа
                # Проверяем первый, он должен содержать поле isHedgeMode
                position_info = resp['result']['list'][0]
                is_hedge_mode = position_info.get('isHedgeMode', False)

                if not is_hedge_mode:
                    self.log.warning(f"Символ {symbol} не в режиме хеджирования. Попытка переключения...")
                    switch_resp = self.session.switch_position_mode(
                        category="linear",
                        symbol=symbol,
                        mode=3 # 3 = Hedge Mode
                    )
                    if switch_resp.get('retCode') == 0:
                        self.log.info(f"Успешно переключен режим позиции для {symbol} на Hedge Mode.")
                        return True
                    else:
                        self.log.error(f"Не удалось переключить режим для {symbol}: {switch_resp.get('retMsg')}")
                        return False
                else:
                    self.log.info(f"Режим хеджирования для {symbol} уже активен.")
                    return True
            else:
                # Если позиций нет, API может не вернуть isHedgeMode.
                # В этом случае мы делаем "слепую" попытку переключения.
                self.log.info(f"Не удалось определить режим для {symbol}, попытка установить Hedge Mode.")
                switch_resp = self.session.switch_position_mode(
                    category="linear",
                    symbol=symbol,
                    mode=3
                )
                if switch_resp.get('retCode') == 0 or "Position mode is not modified" in switch_resp.get('retMsg', ''):
                    self.log.info(f"Режим хеджирования для {symbol} успешно установлен/подтвержден.")
                    return True
                else:
                    self.log.error(f"Не удалось установить режим хеджирования для {symbol}: {switch_resp.get('retMsg')}")
                    return False
        except Exception as e:
            self.log.error(f"Исключение при проверке/переключении режима позиции для {symbol}: {e}")
            return False

    def execute_trade(self, signal_data: pd.Series, risk_percent: float):
        """Полный цикл: вход по рынку, установка SL, установка лимитного TP1."""
        symbol = signal_data['symbol']

        if symbol not in self.initialized_symbols:
            self.get_instrument_info(symbol)
            self.switch_position_mode(symbol) # <-- ВАЖНОЕ ИЗМЕНЕНИЕ
            self.set_leverage(symbol)
            self.initialized_symbols.add(symbol)
        
        if self.get_open_positions(symbol) or self.trade_state.get_state(symbol):
            self.log.info(f"Позиция по {symbol} уже открыта или находится в обработке.")
            return None

        instrument_info = self.instrument_info_cache.get(symbol, {}).get('data')
        if not instrument_info:
            self.log.error(f"Не удалось получить информацию об инструменте для {symbol}. Сделка отменена.")
            return None
            
        lot_size_filter = instrument_info['lotSizeFilter']
        price_filter = instrument_info['priceFilter']
        tick_size = Decimal(price_filter['tickSize'])
        
        side = "Buy" if signal_data['signal'] == 1 else "Sell"
        
        approx_entry_price = signal_data['close']
        approx_sl_price = signal_data['stop_loss']
        
        qty = self.calculate_position_size(approx_entry_price, approx_sl_price, symbol, lot_size_filter, risk_percent)
        if not qty: return None

        order_id = self.place_market_order(symbol, side, qty)
        if not order_id: return None

        time.sleep(2)
        position = self.get_open_positions(symbol)
        if not position:
            self.log.error("ПОЗИЦИЯ НЕ НАЙДЕНА ПОСЛЕ ОТКРЫТИЯ!")
            return None

        real_entry_price = Decimal(position['avgPrice'])
        
        sl_price = Decimal(str(signal_data['stop_loss'])).quantize(tick_size, rounding=ROUND_DOWN if side == "Buy" else ROUND_UP)
        tp1_price = Decimal(str(signal_data['tp1'])).quantize(tick_size, rounding=ROUND_UP if side == "Buy" else ROUND_DOWN)
        
        # Устанавливаем только стоп-лосс при открытии
        self.set_trading_stop(symbol=symbol, side=side, sl_price=str(sl_price))

        partial_qty_decimal = (Decimal(str(qty)) * Decimal(str(self.partial_tp_percent / 100))).quantize(Decimal(lot_size_filter['qtyStep']), rounding=ROUND_DOWN)
        partial_qty = float(partial_qty_decimal)

        if partial_qty < float(lot_size_filter['minOrderQty']):
             self.log.warning(f"Объем для частичного TP ({partial_qty}) меньше минимально допустимого. TP1 не будет установлен.")
             tp1_order_id = None
        else:
            tp1_side = "Sell" if side == "Buy" else "Buy"
            tp1_order_id = self.place_reduce_only_limit_order(symbol, tp1_side, str(partial_qty), str(tp1_price), original_side=side)

        # Сохраняем состояние сделки
        new_state = {
            "state": "TP1_PENDING" if tp1_order_id else "FULL_POSITION",
            "side": side,
            "initial_size": qty,
            "tp1_order_id": tp1_order_id,
            "tp2_price": signal_data['tp2'],
            "entry_price": float(real_entry_price),
            "sl_price": float(sl_price)
        }
        self.trade_state.set_state(symbol, new_state)
        self.log.info(f"Состояние для {symbol} сохранено: {new_state}")

        return {"order_id": order_id, "symbol": symbol, "side": side, "qty": qty, "entry_price": position['avgPrice']}
