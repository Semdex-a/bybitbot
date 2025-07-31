import json
import logging
import os
import threading
from typing import Dict, Any

class TradeState:
    """
    Manages the state of trades in a thread-safe manner, persisting to a JSON file.
    The file path can be configured via the STATE_PATH environment variable.
    """
    def __init__(self, default_file_path: str = 'trade_states.json'):
        # Если переменная окружения STATE_PATH установлена, используем ее.
        # В противном случае, используем путь по умолчанию.
        # Это позволяет легко переопределить путь для Docker или облачных платформ.
        state_dir = os.getenv('STATE_PATH')
        if state_dir:
            # Убедимся, что директория существует
            os.makedirs(state_dir, exist_ok=True)
            self.file_path = os.path.join(state_dir, default_file_path)
        else:
            self.file_path = default_file_path
        
        self.states: Dict[str, Any] = {}
        self._lock = threading.Lock()
        self.log = logging.getLogger(__name__)
        self.log.info(f"Файл состояний будет использоваться по пути: {self.file_path}")
        self.load()

    def load(self):
        """Loads trade states from the JSON file."""
        with self._lock:
            try:
                with open(self.file_path, 'r') as f:
                    self.states = json.load(f)
                    self.log.info(f"Состояния сделок успешно загружены из {self.file_path}")
            except FileNotFoundError:
                self.log.warning(f"Файл состояний {self.file_path} не найден. Будет создан новый.")
                self.states = {}
            except json.JSONDecodeError:
                self.log.error(f"Ошибка декодирования JSON из {self.file_path}. Файл может быть поврежден.")
                self.states = {}

    def save(self):
        """Saves the current trade states to the JSON file."""
        with self._lock:
            try:
                with open(self.file_path, 'w') as f:
                    json.dump(self.states, f, indent=4)
            except Exception as e:
                self.log.error(f"Не удалось сохранить состояние сделок в {self.file_path}: {e}")

    def get_state(self, symbol: str) -> Dict[str, Any]:
        """Gets the state for a specific symbol."""
        with self._lock:
            return self.states.get(symbol)

    def set_state(self, symbol: str, state: Dict[str, Any]):
        """Sets the state for a specific symbol and saves it."""
        with self._lock:
            self.states[symbol] = state
        self.save()

    def remove_state(self, symbol: str):
        """Removes the state for a specific symbol and saves the change."""
        with self._lock:
            if symbol in self.states:
                del self.states[symbol]
        self.save()

    def get_all_states(self) -> Dict[str, Any]:
        """Returns a copy of all current states."""
        with self._lock:
            return self.states.copy()
