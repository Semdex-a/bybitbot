import os
import time
import logging
from pybit.unified_trading import HTTP
from dotenv import load_dotenv

# --- Настройка ---
load_dotenv()
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def test_bybit_connection():
    """Тестирует подключение к Bybit API и проверяет основные функции."""
    
    try:
        api_key = os.getenv("BYBIT_API_KEY")
        api_secret = os.getenv("BYBIT_API_SECRET")
        testnet = os.getenv("TESTNET", "false").lower() == "true"
        
        if not api_key or not api_secret:
            raise ValueError("BYBIT_API_KEY и BYBIT_API_SECRET должны быть установлены")
        
        session = HTTP(testnet=testnet, api_key=api_key, api_secret=api_secret)
        
        print(f"\n{'='*60}")
        print(f"ТЕСТИРОВАНИЕ BYBIT API ({'TESTNET' if testnet else 'MAINNET'})")
        print(f"{'='*60}")
        
        # 1. Проверка баланса
        print("\n1. Проверка баланса...")
        try:
            resp = session.get_wallet_balance(accountType="UNIFIED", coin="USDT")
            if resp['retCode'] == 0:
                balance = 0
                for acc in resp['result']['list']:
                    if acc['accountType'] == "UNIFIED":
                        balance = float(acc['totalWalletBalance'])
                        break
                print(f"✅ Баланс USDT: {balance:.2f}")
            else:
                print(f"❌ Ошибка получения баланса: {resp['retMsg']}")
        except Exception as e:
            print(f"❌ Исключение при получении баланса: {e}")
        
        # 2. Получение информации об инструменте
        test_symbol = "BTCUSDT"
        print(f"\n2. Получение информации о {test_symbol}...")
        try:
            resp = session.get_instruments_info(category="linear", symbol=test_symbol)
            if resp['retCode'] == 0 and resp['result']['list']:
                info = resp['result']['list'][0]
                print(f"✅ Информация получена:")
                print(f"   - Минимальный размер ордера: {info['lotSizeFilter']['minOrderQty']}")
                print(f"   - Шаг размера: {info['lotSizeFilter']['qtyStep']}")
                print(f"   - Минимальная цена: {info['priceFilter']['minPrice']}")
                print(f"   - Шаг цены: {info['priceFilter']['tickSize']}")
            else:
                print(f"❌ Ошибка получения информации: {resp['retMsg']}")
        except Exception as e:
            print(f"❌ Исключение при получении информации: {e}")
            
        # 3. Проверка режима позиций
        print(f"\n3. Проверка текущего режима позиций для {test_symbol}...")
        try:
            resp = session.get_positions(category="linear", symbol=test_symbol)
            if resp['retCode'] == 0:
                print(f"✅ Позиции получены успешно. Найдено позиций: {len(resp['result']['list'])}")
                for pos in resp['result']['list']:
                    print(f"   - Размер позиции: {pos['size']}")
                    print(f"   - Режим позиции указан как: {pos.get('positionIdx', 'не указан')}")
            else:
                print(f"❌ Ошибка получения позиций: {resp['retMsg']}")
        except Exception as e:
            print(f"❌ Исключение при получении позиций: {e}")
            
        # 4. Попытка установки One-Way Mode
        print(f"\n4. Установка One-Way Mode для {test_symbol}...")
        try:
            resp = session.switch_position_mode(category="linear", symbol=test_symbol, mode=3)
            if resp['retCode'] == 0:
                print("✅ One-Way Mode успешно установлен")
            else:
                if "110025" in resp['retMsg']:
                    print("✅ One-Way Mode уже был установлен")
                else:
                    print(f"❌ Ошибка установки режима: {resp['retMsg']}")
        except Exception as e:
            if "110025" in str(e):
                print("✅ One-Way Mode уже был установлен")
            else:
                print(f"❌ Исключение при установке режима: {e}")
        
        # 4.1 Дополнительная проверка режима позиций после установки
        print(f"\n4.1 Проверка режима позиций после установки...")
        try:
            time.sleep(1)  # Даем время на применение изменений
            resp = session.get_positions(category="linear", symbol=test_symbol)
            if resp['retCode'] == 0:
                for pos in resp['result']['list']:
                    position_idx = pos.get('positionIdx', 'не указан')
                    print(f"   ℹ️ positionIdx в ответе: {position_idx}")
                    if position_idx == 0:
                        print("   ✅ Режим: One-Way Mode (positionIdx=0)")
                    elif position_idx in [1, 2]:
                        print(f"   ⚠️ Режим: Hedge Mode (positionIdx={position_idx})")
                    else:
                        print(f"   ❓ Неизвестный режим (positionIdx={position_idx})")
        except Exception as e:
            print(f"   ❌ Ошибка проверки режима: {e}")
                
        # 5. Установка плеча
        print(f"\n5. Установка плеча 5x для {test_symbol}...")
        try:
            resp = session.set_leverage(category="linear", symbol=test_symbol, buyLeverage="5", sellLeverage="5")
            if resp['retCode'] == 0:
                print("✅ Плечо успешно установлено")
            else:
                print(f"ℹ️ Ответ API: {resp['retMsg']}")
        except Exception as e:
            if "leverage not modified" in str(e).lower():
                print("✅ Плечо уже было установлено")
            else:
                print(f"❌ Исключение при установке плеча: {e}")
                
        # 6. Тест размещения тестового ордера (очень маленький размер)
        print(f"\n6. ТЕСТ: Попытка размещения минимального тестового ордера...")
        print("   ⚠️ ВНИМАНИЕ: Это попытка реального ордера с минимальным размером!")
        
        user_confirm = input("   Продолжить тест размещения ордера? (y/N): ").lower()
        if user_confirm == 'y':
            try:
                # Получаем текущую цену для расчета минимального размера
                resp = session.get_tickers(category="linear", symbol=test_symbol)
                if resp['retCode'] == 0:
                    current_price = float(resp['result']['list'][0]['lastPrice'])
                    
                    # Получаем минимальный размер ордера
                    resp = session.get_instruments_info(category="linear", symbol=test_symbol)
                    min_qty = float(resp['result']['list'][0]['lotSizeFilter']['minOrderQty'])
                    
                    print(f"   Текущая цена: {current_price}")
                    print(f"   Минимальный размер: {min_qty}")
                    print(f"   Стоимость минимального ордера: ~${current_price * min_qty:.2f}")
                    
                    # Пробуем разные варианты positionIdx
                    position_indices = [0, 1, 2, None]
                    success = False
                    
                    for position_idx in position_indices:
                        try:
                            order_params = {
                                "category": "linear",
                                "symbol": test_symbol,
                                "side": "Buy",
                                "orderType": "Market",
                                "qty": str(min_qty)
                            }
                            
                            if position_idx is not None:
                                order_params["positionIdx"] = position_idx
                            
                            print(f"   Попытка с positionIdx={position_idx}...")
                            resp = session.place_order(**order_params)
                            
                            if resp['retCode'] == 0:
                                order_id = resp['result']['orderId']
                                print(f"   ✅ Тестовый ордер успешно размещен! ID: {order_id}")
                                print(f"   ✅ РАБОТАЮЩИЙ positionIdx: {position_idx}")
                                success = True
                                
                                # Сразу пытаемся отменить ордер, если он не исполнился
                                time.sleep(1)
                                try:
                                    cancel_resp = session.cancel_order(category="linear", symbol=test_symbol, orderId=order_id)
                                    if cancel_resp['retCode'] == 0:
                                        print("   ✅ Тестовый ордер отменен")
                                    else:
                                        print("   ℹ️ Ордер уже исполнился или отменился автоматически")
                                except:
                                    print("   ℹ️ Не удалось отменить ордер (вероятно, уже исполнился)")
                                break
                            else:
                                print(f"   ❌ Ошибка с positionIdx={position_idx}: {resp['retMsg']}")
                                
                        except Exception as e:
                            print(f"   ❌ Исключение с positionIdx={position_idx}: {e}")
                    
                    if not success:
                        print("   ❌ НИ ОДИН positionIdx НЕ РАБОТАЕТ!")
                        print("   ℹ️ Возможные причины:")
                        print("      - Недостаточно средств для минимального ордера")
                        print("      - Проблемы с настройками аккаунта")
                        print("      - Ограничения API ключа")
                            
            except Exception as e:
                print(f"   ❌ Критическая ошибка теста ордера: {e}")
        else:
            print("   ℹ️ Тест размещения ордера пропущен")
            
        print(f"\n{'='*60}")
        print("ТЕСТИРОВАНИЕ ЗАВЕРШЕНО")
        print(f"{'='*60}")
        
    except Exception as e:
        logger.error(f"Критическая ошибка: {e}")

if __name__ == "__main__":
    test_bybit_connection()