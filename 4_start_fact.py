import mysql.connector
from decimal import Decimal
import re

def create_connection():
    return mysql.connector.connect(
        host="localhost",
        user="root",
        password="Ms_phone_fox62052!",
        database="ms_phone"
    )

def create_fact_ms_phone(cursor, conn):
    # Получаем уникальные категории
    cursor.execute("""
        SELECT DISTINCT category
        FROM net_sales
        WHERE category IS NOT NULL
        ORDER BY category
    """)
    categories = [row[0] for row in cursor.fetchall()]

    # Формируем список полей
    fields = ""
    for category in categories:
        safe_cat = re.sub(r'\W+', '_', category)
        fields += f"`{safe_cat}_amount` DECIMAL(14,2), "
        fields += f"`{safe_cat}_quantity` DECIMAL(12,3), "

    # Создаём таблицу
    cursor.execute(f"""
        CREATE TABLE IF NOT EXISTS fact_ms_phone (
            id INT AUTO_INCREMENT PRIMARY KEY,
            store TEXT,
            total_net DECIMAL(14,2),
            {fields[:-2]} -- удаляем последнюю запятую
        )
    """)
    conn.commit()

    # Очищаем старые данные
    cursor.execute("DELETE FROM fact_ms_phone")
    conn.commit()

    # Получаем список магазинов
    cursor.execute("SELECT DISTINCT store FROM net_sales")
    stores = cursor.fetchall()

    total_records = 0

    for store_tuple in stores:
        store = store_tuple[0]

        # Считаем общий net_amount
        cursor.execute("""
            SELECT SUM(net_amount)
            FROM net_sales
            WHERE store = %s
        """, (store,))
        total_net = cursor.fetchone()[0] or Decimal(0)

        # Подготовка словаря
        category_data = {}
        for category in categories:
            safe_cat = re.sub(r'\W+', '_', category)
            category_data[f"{safe_cat}_amount"] = Decimal(0)
            category_data[f"{safe_cat}_quantity"] = Decimal(0)

        # Получаем значения по категориям
        cursor.execute("""
            SELECT category, SUM(net_amount), SUM(net_quantity)
            FROM net_sales
            WHERE store = %s
            GROUP BY category
        """, (store,))
        rows = cursor.fetchall()

        for category, amount, quantity in rows:
            safe_cat = re.sub(r'\W+', '_', category)
            if f"{safe_cat}_amount" in category_data:
                category_data[f"{safe_cat}_amount"] = amount or Decimal(0)
            if f"{safe_cat}_quantity" in category_data:
                category_data[f"{safe_cat}_quantity"] = quantity or Decimal(0)

        # Готовим SQL-запрос
        columns = ['store', 'total_net'] + list(category_data.keys())
        values = [store, total_net] + list(category_data.values())
        columns_sql = ', '.join([f"`{col}`" for col in columns])
        placeholders = ', '.join(['%s'] * len(values))

        cursor.execute(f"""
            INSERT INTO fact_ms_phone ({columns_sql})
            VALUES ({placeholders})
        """, values)

        total_records += 1

    conn.commit()
    print(f"✅ Таблица fact_ms_phone успешно создана. Загружено {total_records} строк.")

# Запуск
if __name__ == "__main__":
    conn = create_connection()
    cursor = conn.cursor()

    create_fact_ms_phone(cursor, conn)

    cursor.close()
    conn.close()
