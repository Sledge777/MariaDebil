import mysql.connector
from decimal import Decimal

# Подключение к базе данных
def create_connection():
    return mysql.connector.connect(
        host="localhost",
        user="root",
        password="Ms_phone_fox62052!",
        database="ms_phone"
    )

# Функция создания широкой таблицы
def create_store_category_pivot(cursor, conn):
    # Получаем все уникальные категории, исключая 'Неизвестная категория'
    cursor.execute("""
        SELECT DISTINCT category
        FROM net_sales
        WHERE category != 'Неизвестная категория'
        ORDER BY category
    """)
    categories = [row[0] for row in cursor.fetchall()]

    # Строим динамические столбцы для категорий
    category_columns = ""
    for category in categories:
        safe_category = category.replace(' ', '_').replace('-', '_')
        category_columns += f"`{safe_category}_amount` DECIMAL(14,2), `{safe_category}_quantity` DECIMAL(12,3), "

    # Создаем новую таблицу
    cursor.execute(f"""
        CREATE TABLE IF NOT EXISTS store_category_pivot (
            id INT AUTO_INCREMENT PRIMARY KEY,
            store TEXT,
            regional_director TEXT,
            total_net_amount DECIMAL(14,2),
            {category_columns[:-2]} -- убираем последнюю запятую
        )
    """)
    conn.commit()

    # Очищаем таблицу перед загрузкой новых данных
    cursor.execute("DELETE FROM store_category_pivot")
    conn.commit()

    # Получаем список всех магазинов
    cursor.execute("""
        SELECT DISTINCT store, regional_director
        FROM net_sales
    """)
    stores = cursor.fetchall()

    total_records = 0

    for store, regional_director in stores:
        # Считаем общий оборот магазина
        cursor.execute("""
            SELECT SUM(net_amount) 
            FROM net_sales
            WHERE store = %s
        """, (store,))
        total_net_amount = cursor.fetchone()[0] or Decimal(0)

        # Готовим словарь под категории
        category_data = {f"{cat.replace(' ', '_').replace('-', '_')}_amount": Decimal(0) for cat in categories}
        category_data.update({f"{cat.replace(' ', '_').replace('-', '_')}_quantity": Decimal(0) for cat in categories})

        # Получаем сумму и количество по категориям, исключая 'Неизвестная категория'
        cursor.execute("""
            SELECT category, 
                   SUM(net_amount) AS category_net_amount,
                   SUM(net_quantity) AS category_net_quantity
            FROM net_sales
            WHERE store = %s
            AND category != 'Неизвестная категория'
            GROUP BY category
        """, (store,))
        rows = cursor.fetchall()

        for category, cat_amount, cat_quantity in rows:
            safe_category = category.replace(' ', '_').replace('-', '_')
            category_data[f"{safe_category}_amount"] = cat_amount or Decimal(0)
            category_data[f"{safe_category}_quantity"] = cat_quantity or Decimal(0)

        # Формируем список столбцов и значений для вставки
        columns = ['store', 'regional_director', 'total_net_amount'] + list(category_data.keys())
        values = [store, regional_director, total_net_amount] + list(category_data.values())

        # Экранируем имена колонок через обратные кавычки
        columns_safe = [f"`{col}`" for col in columns]
        placeholders = ', '.join(['%s'] * len(values))

        cursor.execute(f"""
            INSERT INTO store_category_pivot ({', '.join(columns_safe)})
            VALUES ({placeholders})
        """, values)

        total_records += 1

    conn.commit()
    print(f"✅ Широкая таблица успешно создана. Загружено {total_records} строк.")

# Основной блок
if __name__ == "__main__":
    conn = create_connection()
    cursor = conn.cursor()

    # Создание широкой таблицы
    create_store_category_pivot(cursor, conn)

    cursor.close()
    conn.close()
