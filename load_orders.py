"""
load_orders.py

Скрипт загружает заказы из orders-1.json в SQLite-базу orders.db.
Делает:
- чтение JSON
- нормализацию полей (в т.ч. даты и customer.region)
- проверку дублей по order_id
- логирование шагов и ошибок
"""

import json
import logging
import sqlite3
from datetime import datetime
from typing import List, Dict, Tuple


def setup_logging(log_file: str = "orders_loader.log") -> logging.Logger:
    """Настройка логирования в файл и консоль."""
    logger = logging.getLogger("orders_loader")
    logger.setLevel(logging.DEBUG)

    # чтобы при повторных запусках не плодились хендлеры
    logger.handlers = []

    fmt = logging.Formatter(
        "%(asctime)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    fh = logging.FileHandler(log_file)
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(fmt)

    ch = logging.StreamHandler()
    ch.setLevel(logging.INFO)
    ch.setFormatter(fmt)

    logger.addHandler(fh)
    logger.addHandler(ch)

    return logger


class OrdersDatabase:
    """Обёртка над SQLite для работы с таблицей orders."""

    def __init__(self, db_path: str = "orders.db", logger: logging.Logger | None = None):
        self.db_path = db_path
        self.logger = logger or logging.getLogger("orders_loader")
        self.conn: sqlite3.Connection | None = None

    def connect(self) -> None:
        try:
            self.conn = sqlite3.connect(self.db_path)
            self.logger.info(f"Подключение к БД: {self.db_path}")
        except sqlite3.Error as e:
            self.logger.error(f"Ошибка подключения к БД: {e}")
            raise

    def close(self) -> None:
        if self.conn:
            self.conn.close()
            self.logger.info("Соединение с БД закрыто")

    def create_schema(self) -> None:
        """Создаёт таблицу orders, если её ещё нет."""
        sql = """
        CREATE TABLE IF NOT EXISTS orders (
            order_id        TEXT PRIMARY KEY,
            status          TEXT NOT NULL,
            date            TIMESTAMP NOT NULL,
            amount          REAL NOT NULL,
            customer_region TEXT NOT NULL,
            loaded_at       TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """
        try:
            cur = self.conn.cursor()
            cur.execute(sql)
            self.conn.commit()
            self.logger.info("Схема БД проверена/создана (таблица orders)")
        except sqlite3.Error as e:
            self.logger.error(f"Ошибка при создании схемы: {e}")
            raise

    def get_existing_order_ids(self) -> set[str]:
        """Возвращает множество уже существующих order_id."""
        try:
            cur = self.conn.cursor()
            cur.execute("SELECT order_id FROM orders")
            ids = {row[0] for row in cur.fetchall()}
            self.logger.debug(f"Найдено существующих заказов: {len(ids)}")
            return ids
        except sqlite3.Error as e:
            self.logger.error(f"Ошибка при чтении существующих order_id: {e}")
            raise

    def insert_orders(self, orders: List[Dict]) -> Tuple[int, int, int]:
        """
        Вставляет заказы с проверкой дублей.
        Возвращает (inserted, skipped_duplicates, errors).
        """
        inserted = 0
        skipped = 0
        errors = 0

        existing_ids = self.get_existing_order_ids()
        cur = self.conn.cursor()

        for raw in orders:
            try:
                order_id = raw["order_id"]
                if order_id in existing_ids:
                    self.logger.debug(f"Дубликат, пропущен: order_id={order_id}")
                    skipped += 1
                    continue

                status = raw["status"]
                date_str = raw["date"]
                amount = raw["amount"]
                customer_region = raw.get("customer", {}).get("region")

                # конвертация даты
                try:
                    order_dt = datetime.fromisoformat(date_str)
                except Exception:
                    self.logger.warning(
                        f"Некорректная дата у order_id={order_id}: {date_str}"
                    )
                    errors += 1
                    continue

                if customer_region is None:
                    self.logger.warning(
                        f"Не найден customer.region у order_id={order_id}"
                    )
                    errors += 1
                    continue

                cur.execute(
                    """
                    INSERT INTO orders (order_id, status, date, amount, customer_region)
                    VALUES (?, ?, ?, ?, ?)
                    """,
                    (order_id, status, order_dt, amount, customer_region),
                )
                inserted += 1
                existing_ids.add(order_id)
                self.logger.debug(f"Загружен заказ order_id={order_id}")

            except KeyError as e:
                self.logger.error(f"Отсутствует обязательное поле {e} в записи: {raw}")
                errors += 1
            except sqlite3.IntegrityError as e:
                # на всякий случай, если дубликат не отловили выше
                self.logger.warning(
                    f"IntegrityError при вставке order_id={raw.get('order_id')}: {e}"
                )
                skipped += 1
            except Exception as e:
                self.logger.error(f"Ошибка при обработке записи {raw}: {e}")
                errors += 1

        self.conn.commit()
        self.logger.info(
            f"Загрузка завершена: вставлено={inserted}, дубликатов={skipped}, ошибок={errors}"
        )
        return inserted, skipped, errors


def load_json(path: str, logger: logging.Logger) -> List[Dict]:
    """Читает JSON-файл с заказами."""
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        logger.info(f"Прочитан файл {path}, записей: {len(data)}")
        return data
    except FileNotFoundError:
        logger.error(f"Файл не найден: {path}")
        raise
    except json.JSONDecodeError as e:
        logger.error(f"Ошибка парсинга JSON ({path}): {e}")
        raise


def main() -> None:
    logger = setup_logging()
    logger.info("=" * 70)
    logger.info("СТАРТ ЗАГРУЗКИ ЗАКАЗОВ ИЗ JSON В SQLite")
    logger.info("=" * 70)

    json_path = "orders-1.json"
    db_path = "orders.db"

    try:
        orders = load_json(json_path, logger)

        db = OrdersDatabase(db_path, logger)
        db.connect()
        db.create_schema()

        inserted, skipped, errors = db.insert_orders(orders)

        logger.info("-" * 70)
        logger.info(f"ИТОГ: вставлено={inserted}, дубликатов={skipped}, ошибок={errors}")
        logger.info("-" * 70)

    except Exception as e:
        logger.error(f"Критическая ошибка: {e}")
        raise
    finally:
        if "db" in locals():
            db.close()
        logger.info("Завершение работы")


if __name__ == "__main__":
    main()
