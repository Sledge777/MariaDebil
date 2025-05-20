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

# Функция создания таблицы fact_ms_phone и загрузки данных
def create_fact_ms_phone(cursor, conn):
    # Создаем таблицу fact_ms_phone с жесткой структурой
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS fact_ms_phone (
            id INT AUTO_INCREMENT PRIMARY KEY,
            store TEXT,
            total_net DECIMAL(14,2),
            На_связи_quantity DECIMAL(12,3),
            На_связи_amount DECIMAL(14,2),
            Аксессуары_quantity DECIMAL(12,3),
            Аксессуары_amount DECIMAL(14,2),
            LifeStyle_quantity DECIMAL(12,3),
            LifeStyle_amount DECIMAL(14,2),
            MS_Home_quantity DECIMAL(12,3),
            MS_Home_amount DECIMAL(14,2),
            Подарки_quantity DECIMAL(12,3),
            Подарки_amount DECIMAL(14,2),
            ТВ_quantity DECIMAL(12,3),
            ТВ_amount DECIMAL(14,2),
            Настройки_amount DECIMAL(14,2),
            Страховки_amount DECIMAL(14,2),
            Сим_карты_quantity DECIMAL(12,3)
        )
    """)
    conn.commit()

    # Очищаем таблицу перед загрузкой новых данных
    cursor.execute("DELETE FROM fact_ms_phone")
    conn.commit()

    # Получаем список всех магазинов
    cursor.execute("""
        SELECT DISTINCT store
        FROM net_sales
    """)
    stores = cursor.fetchall()

    total_records = 0

    for store, in stores:
        # Считаем общий оборот магазина
        cursor.execute("""
            SELECT SUM(net_amount) 
            FROM net_sales
            WHERE store = %s
        """, (store,))
        total_net_amount = cursor.fetchone()[0] or Decimal(0)

        # Готовим словарь под категории
        category_data = {
            "На_связи_quantity": Decimal(0),
            "На_связи_amount": Decimal(0),
            "Аксессуары_quantity": Decimal(0),
            "Аксессуары_amount": Decimal(0),
            "LifeStyle_quantity": Decimal(0),
            "LifeStyle_amount": Decimal(0),
            "MS_Home_quantity": Decimal(0),
            "MS_Home_amount": Decimal(0),
            "Подарки_quantity": Decimal(0),
            "Подарки_amount": Decimal(0),
            "ТВ_quantity": Decimal(0),
            "ТВ_amount": Decimal(0),
            "Настройки_amount": Decimal(0),
            "Страховки_amount": Decimal(0),
            "Сим_карты_quantity": Decimal(0),
        }

        # Получаем сумму и количество по категориям
        cursor.execute("""
            SELECT category, 
                   SUM(net_amount) AS category_net_amount,
                   SUM(net_quantity) AS category_net_quantity
            FROM net_sales
            WHERE store = %s
            GROUP BY category
        """, (store,))
        rows = cursor.fetchall()

        for category, cat_amount, cat_quantity in rows:
            safe_category = category.replace(' ', '_').replace('-', '_')

            # Заполняем соответствующие категории
            if f"{safe_category}_amount" in category_data:
                category_data[f"{safe_category}_amount"] = cat_amount or Decimal(0)
            if f"{safe_category}_quantity" in category_data:
                category_data[f"{safe_category}_quantity"] = cat_quantity or Decimal(0)

        # Формируем список столбцов и значений для вставки
        columns = ['store', 'total_net'] + list(category_data.keys())
        values = [store, total_net_amount] + list(category_data.values())

        # Экранируем имена колонок через обратные кавычки
        columns_safe = [f"`{col}`" for col in columns]
        placeholders = ', '.join(['%s'] * len(values))

        cursor.execute(f"""
            INSERT INTO fact_ms_phone ({', '.join(columns_safe)})
            VALUES ({placeholders})
        """, values)

        total_records += 1

    conn.commit()
    print(f"✅ Таблица fact_ms_phone успешно создана. Загружено {total_records} строк.")

# Основной блок
if __name__ == "__main__":
    conn = create_connection()
    cursor = conn.cursor()

    # Создание и заполнение таблицы fact_ms_phone
    create_fact_ms_phone(cursor, conn)

    cursor.close()
    conn.close()
