from decimal import Decimal
from datetime import datetime
import mysql.connector

# Подключение к базе данных
def create_connection():
    return mysql.connector.connect(
        host="localhost",
        user="root",
        password="Ms_phone_fox62052!",
        database="ms_phone"
    )

# Функция для расчёта и записи данных по категориям с датой и временем, товаром и группой товаров
def calculate_sales_with_goods_price_group(cursor, conn):
    # Выбираем данные из таблицы net_sales и сортируем по employee ASC
    cursor.execute("""
        SELECT store, regional_director, employee, category, goods_name, net_amount, commission_amount, net_quantity, date, goods_price_group
        FROM net_sales
        ORDER BY employee ASC
    """)
    sales_data = cursor.fetchall()

    # Подготовим структуру для хранения агрегированных данных
    aggregated_sales = {}

    # Агрегируем данные по категориям для каждого сотрудника
    for sale in sales_data:
        store, regional_director, employee, category, goods_name, net_amount, commission_amount, net_quantity, sale_date, goods_price_group = sale

        # Пропускаем товары с неизвестными наименованиями или категориями
        if not goods_name or not category:
            continue

        # Преобразуем дату в формат с временем
        sale_date = sale_date.strftime('%Y-%m-%d %H:%M:%S') if isinstance(sale_date, datetime) else datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        # Создаем ключ для идентификации магазина, сотрудника, категории, товара и даты
        key = (store, regional_director, employee, category, goods_name, sale_date, goods_price_group)

        if key not in aggregated_sales:
            aggregated_sales[key] = {'total_sales': Decimal(0), 'total_commission': Decimal(0), 'total_quantity': 0}

        aggregated_sales[key]['total_sales'] += net_amount
        aggregated_sales[key]['total_commission'] += commission_amount
        aggregated_sales[key]['total_quantity'] += net_quantity

    # Записываем данные в новую таблицу sales_detail
    count = 0
    for key, value in aggregated_sales.items():
        store, regional_director, employee, category, goods_name, sale_date, goods_price_group = key
        total_sales = value['total_sales']
        total_commission = value['total_commission']
        total_quantity = value['total_quantity']

        cursor.execute("""
            INSERT INTO sales_detail (store, regional_director, employee, category, sale_date, goods_name, total_sales, total_commission, total_quantity, goods_price_group)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (store, regional_director, employee, category, sale_date, goods_name, total_sales, total_commission, total_quantity, goods_price_group))

        count += 1

    conn.commit()

    # Выводим количество записей и время выполнения коммита
    commit_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    print(f"✅ Загружено {count} записей в таблицу sales_detail. Коммит выполнен в: {commit_time}")

# Основной блок
if __name__ == "__main__":
    conn = create_connection()
    cursor = conn.cursor()

    # Создание таблицы для продаж с данными по категориям, товарам и группе товаров, включая дату и время
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS sales_detail (
            id INT AUTO_INCREMENT PRIMARY KEY,
            store TEXT,
            regional_director TEXT,
            employee TEXT,
            category TEXT,
            sale_date DATETIME,
            goods_name TEXT,
            total_sales DECIMAL(12, 2),
            total_commission DECIMAL(12, 2),
            total_quantity INT,
            goods_price_group TEXT
        )
    """)

    # Выполняем расчёт и запись данных
    calculate_sales_with_goods_price_group(cursor, conn)

    # Закрываем соединение
    cursor.close()
    conn.close()
