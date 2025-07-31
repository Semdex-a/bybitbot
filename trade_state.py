import json
import os
import threading
from typing import Dict, Optional

class TradeState:
    def __init__(self, filename: str = "trade_states.json"):
        self.filename = filename
        self.lock = threading.Lock()
        self.states = self._load_states()

    def _load_states(self) -> dict:
        """Загружает состояния из файла."""
        if os.path.exists(self.filename):
            try:
                with open(self.filename, 'r', encoding='utf-8') as f:
                    return json.load(f)
            except (json.JSONDecodeError, IOError):
                return {}
        return {}

    def _save_states(self):
        """Сохраняет состояния в файл."""
        try:
            with open(self.filename, 'w', encoding='utf-8') as f:
                json.dump(self.states, f, indent=2, ensure_ascii=False)
        except IOError as e:
            print(f"Ошибка сохранения состояний: {e}")

    def set_state(self, symbol: str, state: dict):
        """Устанавливает состояние для символа."""
        with self.lock:
            self.states[symbol] = state
            self._save_states()

    def get_state(self, symbol: str) -> Optional[dict]:
        """Получает состояние для символа."""
        with self.lock:
            return self.states.get(symbol)

    def get_all_states(self) -> Dict[str, dict]:
        """Получает все состояния."""
        with self.lock:
            return self.states.copy()

    def remove_state(self, symbol: str):
        """Удаляет состояние для символа."""
        with self.lock:
            if symbol in self.states:
                del self.states[symbol]
                self._save_states()

    def clear_all_states(self):
        """Удаляет все состояния."""
        with self.lock:
            self.states.clear()
            self._save_states()
