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

# Маппинг коэффициентов в зависимости от категории товара
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

# Функция для корректировки имени категории перед запросом
def normalize_category_name(category):
    # Заменим пробелы на подчеркивания (если нужно)
    return category.replace(' ', '_')

# Функция для расчёта коэффициента по категории и проценту выполнения
def get_coefficient(category, percentage):
    category = normalize_category_name(category)  # Преобразуем категорию
    # Если категория не найдена в словаре, возвращаем 1 (по умолчанию)
    if category not in coefficient_mapping:
        return 1
    if percentage < 90:
        return coefficient_mapping[category][0]
    elif 90 <= percentage <= 100:
        return coefficient_mapping[category][1]
    else:
        return coefficient_mapping[category][2]

# Функция для создания таблицы sales_detail_kpi
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

# Функция для обновления данных в таблице sales_detail_kpi с учетом коэффициентов
def update_sales_detail_kpi(cursor, conn):
    cursor.execute("""
        SELECT id, store, category, total_commission
        FROM sales_detail
    """)
    sales_data = cursor.fetchall()

    # Для каждого продажного процесса из таблицы sales_detail
    for row in sales_data:
        sales_id = row[0]
        store = row[1]
        category = row[2]
        total_commission = row[3]

        # Получаем коэффициент KPI для магазина и категории
        cursor.execute("""
            SELECT total_net_percentage
            FROM ms_phone_performance
            WHERE store = %s
        """, (store,))
        performance_data = cursor.fetchone()

        if performance_data:
            total_net_percentage = performance_data[0]
            # Получаем коэффициент для категории
            coefficient = get_coefficient(category, total_net_percentage)

            # Рассчитываем итоговую сумму
            total_sum = total_commission * Decimal(coefficient)

            # Вставляем данные в таблицу sales_detail_kpi
            cursor.execute("""
                INSERT INTO sales_detail_kpi (store, regional_director, employee, category, sale_date, goods_name,
                                              total_sales, total_commission, total_quantity, goods_price_group, kpi_coefficient, total_sum)
                SELECT store, regional_director, employee, category, sale_date, goods_name,
                       total_sales, total_commission, total_quantity, goods_price_group, %s, %s
                FROM sales_detail WHERE id = %s
            """, (coefficient, total_sum, sales_id))

    conn.commit()
    print(f"✅ Данные успешно обновлены в таблице sales_detail_kpi.")

# Основной код
if __name__ == "__main__":
    # Подключение к базе данных
    conn = create_connection()
    cursor = conn.cursor()

    # Создаем таблицу sales_detail_kpi, если она не существует
    create_sales_detail_kpi_table(cursor)

    # Обновляем таблицу sales_detail_kpi с коэффициентами и итоговой суммой
    update_sales_detail_kpi(cursor, conn)

    # Закрываем соединение с базой данных
    cursor.close()
    conn.close()
####################################################8_kpi_detail_fact.py###################################################################м