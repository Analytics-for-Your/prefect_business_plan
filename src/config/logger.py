# config/logger.py
import logging
import os
import sys

def setup_logger(name: str) -> logging.Logger:
    """Настройка логгера с JSON-форматом для Fluentbit и консольным выводом."""
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)

    # Форматтер для JSON
    log_format = '{"time": "%(asctime)s", "level": "%(levelname)s", "name": "%(name)s", "message": "%(message)s"}'
    formatter = logging.Formatter(log_format)

    # Определяем директорию для логов в зависимости от окружения
    if os.getenv("KUBERNETES_SERVICE_HOST"):
        log_dir = "/opt/logs"  # Для Kubernetes
    else:
        log_dir = os.path.join(os.path.dirname(__file__), "..", "logs")  # Локально: src/logs/
    
    os.makedirs(log_dir, exist_ok=True)
    
    # Обработчик для файла
    file_handler = logging.FileHandler(f"{log_dir}/etl.log", encoding='utf-8')
    file_handler.setFormatter(formatter)

    # Обработчик для консоли (для локальной отладки)
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)

    # Очистка существующих обработчиков и добавление новых
    logger.handlers.clear()
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)

    # Отключаем распространение логов в родительские логгеры
    logger.propagate = False

    return logger