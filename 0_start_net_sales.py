import mysql.connector
from decimal import Decimal
from datetime import datetime

def create_connection():
    try:
        conn = mysql.connector.connect(
            host="localhost",
            user="root",
            password="Ms_phone_fox62052!",
            database="ms_phone"
        )
        print("Соединение с базой данных успешно установлено.")
        return conn
    except mysql.connector.Error as err:
        print(f"Ошибка при подключении к базе данных: {err}")
        return None

def calculate_net_sales(cursor, conn):
    # Получаем все продажи
    cursor.execute("""
        SELECT order_id, store, employee, goods_id, quantity, amount, promo_discount, manual_discount,
               goods_price_group, serial_number, date
        FROM sales_by_orders
    """)
    sales_data = cursor.fetchall()

    # Получаем возвраты, сгруппированные по serial_number
    cursor.execute("""
        SELECT serial_number, SUM(quantity), SUM(amount)
        FROM refunds_by_orders
        GROUP BY serial_number
    """)
    refunds_map = {row[0]: (row[1], row[2]) for row in cursor.fetchall() if row[0]}

    # Справочники
    cursor.execute("SELECT store, regional_director FROM company_structure")
    directors = {store: regional_director for store, regional_director in cursor.fetchall()}

    cursor.execute("SELECT goods_id, goods_name FROM goods_hierarchy")
    goods_names = {goods_id: goods_name for goods_id, goods_name in cursor.fetchall()}

    cursor.execute("SELECT goods_price_group, category, commission_percent FROM commission_rates")
    commission_info = {row[0]: (row[1], row[2]) for row in cursor.fetchall()}

    # Очистим таблицу перед вставкой (опционально)
    cursor.execute("DELETE FROM net_sales")

    count = 0
    for sale in sales_data:
        order_id, store, employee, goods_id, quantity, amount, promo_discount, manual_discount, goods_price_group, serial_number, date = sale

        # Преобразуем дату
        if isinstance(date, str):
            date = datetime.strptime(date, '%Y-%m-%d %H:%M:%S')
        formatted_date = date.strftime('%Y-%m-%d %H:%M:%S')

        # Получаем возвраты по серийному номеру
        refund_quantity, refund_amount = (0, 0)
        if serial_number and serial_number in refunds_map:
            refund_quantity, refund_amount = refunds_map[serial_number]

        # Вычисляем net значения
        net_quantity = Decimal(quantity) - Decimal(refund_quantity)
        net_amount = Decimal(amount) - Decimal(refund_amount)

        # Название товара
        goods_name = goods_names.get(goods_id, "Неизвестный товар")
        if goods_name == "Неизвестный товар":
            continue

        # Региональный директор
        regional_director = directors.get(store, None)

        # Категория и комиссия
        category, commission_percent = commission_info.get(goods_price_group, ('Неизвестная категория', 0))
        if category == 'Неизвестная категория':
            continue
        commission_amount = Decimal(net_amount) * Decimal(commission_percent) / Decimal(100)

        # Вставка в net_sales
        try:
            cursor.execute("""
                INSERT INTO net_sales (
                    order_id, date, store, regional_director, employee, goods_id, goods_name,
                    net_quantity, net_amount, refund_quantity, refund_amount,
                    promo_discount, manual_discount, goods_price_group, serial_number,
                    commission_amount, category
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                order_id, formatted_date, store, regional_director, employee, goods_id, goods_name,
                net_quantity, net_amount, refund_quantity, refund_amount,
                promo_discount or 0, manual_discount or 0, goods_price_group, serial_number,
                commission_amount, category
            ))
            count += 1
        except mysql.connector.Error as err:
            print(f"Ошибка вставки: {err}")

    conn.commit()
    print(f"✅ Загружено {count} записей в таблицу net_sales.")

if __name__ == "__main__":
    conn = create_connection()
    if conn:
        cursor = conn.cursor()

        # Таблица net_sales (создание)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS net_sales (
                id INT AUTO_INCREMENT PRIMARY KEY,
                order_id TEXT,
                date DATETIME,
                store TEXT,
                regional_director TEXT,
                employee TEXT,
                goods_id TEXT,
                goods_name TEXT,
                net_quantity DECIMAL(10,3),
                net_amount DECIMAL(12,2),
                refund_quantity DECIMAL(10,3),
                refund_amount DECIMAL(12,2),
                promo_discount DECIMAL(12,2),
                manual_discount DECIMAL(12,2),
                goods_price_group TEXT,
                serial_number TEXT,
                commission_amount DECIMAL(12,2),
                category TEXT
            )
        """)

        calculate_net_sales(cursor, conn)
        cursor.close()
        conn.close()
    else:
        print("Ошибка подключения к базе.")
