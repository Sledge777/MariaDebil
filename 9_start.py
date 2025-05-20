import mysql.connector
from decimal import Decimal
import re

# Подключение к базе данных
def create_connection():
    return mysql.connector.connect(
        host="localhost",
        user="root",
        password="Ms_phone_fox62052!",
        database="ms_phone"
    )

# Маппинг коэффициентов по категориям
coefficient_mapping = {
    'На_связи': [0.8, 1.0, 1.2],
    'Аксессуары': [0.8, 1.0, 1.2],
    'LifeStyle': [0.8, 1.0, 1.2],
    'MS_Home': [0.8, 1.0, 1.2],
    'Настройки': [1.0, 1.2, 1.5],
    'Страховки': [1.0, 1.2, 1.5],
    'Сим_карты': [0.3, 0.4, 0.5],
    'Подарки': [1.0, 1.2, 1.5],
    'ТВ': [0.8, 1.0, 1.2]
}

# Приведение названия категории к стандартному виду
def normalize_category_name(category):
    return category.replace(' ', '_')

# Получение коэффициента в зависимости от категории и процента выполнения
def get_coefficient(category, percentage):
    category = normalize_category_name(category)
    if category not in coefficient_mapping:
        return 1
    if percentage < 90:
        return coefficient_mapping[category][0]
    elif 90 <= percentage <= 100:
        return coefficient_mapping[category][1]
    else:
        return coefficient_mapping[category][2]

# Создание таблицы KPI, если её нет
def create_sales_detail_kpi_table(cursor):
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS sales_detail_kpi (
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
            goods_price_group TEXT,
            kpi_coefficient DECIMAL(5, 2),
            total_sum DECIMAL(12, 2)
        )
    """)

# Обновление таблицы KPI с учетом коэффициента и бонуса Xiaomi
def update_sales_detail_kpi(cursor, conn):
    cursor.execute("""
        SELECT id, store, category, total_commission
        FROM sales_detail
    """)
    sales_data = cursor.fetchall()

    for row in sales_data:
        sales_id = row[0]
        store = row[1]
        category = row[2]
        base_commission = row[3]

        # Получаем процент выполнения плана
        cursor.execute("""
            SELECT total_net_percentage
            FROM ms_phone_performance
            WHERE store = %s
        """, (store,))
        performance_data = cursor.fetchone()

        if performance_data:
            total_net_percentage = performance_data[0]

            # Получаем коэффициент KPI по категории и выполнению плана
            coefficient = get_coefficient(category, total_net_percentage)

            # Исходная комиссия
            total_commission = base_commission

            # Условие на бонус Xiaomi
            if (
                re.search(r'xiaomi', store, re.IGNORECASE)
                and total_net_percentage >= 100
                and normalize_category_name(category).lower() == 'на_связи'
            ):
                bonus = base_commission * Decimal("0.25")
                total_commission += bonus
                print(f"🎯 Бонус Xiaomi добавлен: {bonus:.2f} → магазин: {store}, категория: {category}")

            # Итоговая сумма с учетом коэффициента
            total_sum = total_commission * Decimal(coefficient)

            # Вставка в итоговую таблицу
            cursor.execute("""
                INSERT INTO sales_detail_kpi (
                    store, regional_director, employee, category, sale_date, goods_name,
                    total_sales, total_commission, total_quantity, goods_price_group,
                    kpi_coefficient, total_sum
                )
                SELECT store, regional_director, employee, category, sale_date, goods_name,
                       total_sales, %s, total_quantity, goods_price_group,
                       %s, %s
                FROM sales_detail WHERE id = %s
            """, (total_commission, coefficient, total_sum, sales_id))

    conn.commit()
    print("✅ Данные успешно обновлены в sales_detail_kpi.")

# Основной запуск
if __name__ == "__main__":
    conn = create_connection()
    cursor = conn.cursor()

    create_sales_detail_kpi_table(cursor)
    update_sales_detail_kpi(cursor, conn)

    cursor.close()
    conn.close()
