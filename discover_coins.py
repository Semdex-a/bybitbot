import logging
from pybit.unified_trading import HTTP

# --- Настройка ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
# Количество топовых монет, которые мы хотим получить
TOP_N_COINS = 40

def discover_liquid_coins():
    """
    Подключается к Bybit, получает список всех USDT-фьючерсов,
    сортирует их по 24-часовому обороту и выводит готовый список
    для вставки в .env файл.
    """
    log = logging.getLogger(__name__)
    log.info("Подключение к Bybit для получения списка самых ликвидных монет...")

    try:
        # Для публичных данных ключи не требуются
        session = HTTP(testnet=False)
        
        # Получаем тикеры для всех ликвидных USDT-фьючерсов
        response = session.get_tickers(category="linear")

        if response['retCode'] != 0:
            log.error(f"Ошибка API Bybit: {response['retMsg']}")
            return

        tickers = response['result']['list']
        log.info(f"Получено {len(tickers)} торговых пар. Начинаю сортировку...")

        # Фильтруем и сортируем
        # Убеждаемся, что оборот - это число, и сортируем по убыванию
        sorted_tickers = sorted(
            [t for t in tickers if t.get('turnover24h') and float(t['turnover24h']) > 0],
            key=lambda x: float(x['turnover24h']),
            reverse=True
        )

        # Выбираем топ N монет
        top_coins = sorted_tickers[:TOP_N_COINS]
        
        # Формируем строку для .env
        top_coin_symbols = [coin['symbol'] for coin in top_coins]
        env_string = ",".join(top_coin_symbols)

        print("\n" + "="*80)
        print(f"✅ Готово! Вот топ-{TOP_N_COINS} самых ликвидных монет на Bybit (USDT-фьючерсы).")
        print("Скопируйте всю строку ниже и вставьте ее в ваш .env файл вместо старой.")
        print("="*80 + "\n")
        # Печатаем саму строку в чистом виде для легкого копирования
        print(env_string)
        print("\n" + "="*80)

    except Exception as e:
        log.error(f"Произошла непредвиденная ошибка: {e}")

if __name__ == "__main__":
    discover_liquid_coins()
