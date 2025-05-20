import mysql.connector
import re
from decimal import Decimal

# Подключение к базе данных
def create_connection():
    return mysql.connector.connect(
        host="localhost",
        user="root",
        password="Ms_phone_fox62052!",
        database="ms_phone"
    )

def update_commission_and_copy_to_all_detail():
    conn = create_connection()
    cursor = conn.cursor(dictionary=True)

    # Создание таблицы all_detail, если не существует
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS all_detail (
            id INT PRIMARY KEY,
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
        );
    """)

    # Получение всех записей из sales_detail_kpi
    cursor.execute("SELECT * FROM sales_detail_kpi")
    rows = cursor.fetchall()

    # Получаем информацию о планах выполнения из ms_phone_performance
    cursor.execute("SELECT store, total_net_percentage FROM ms_phone_performance")
    performance_data = cursor.fetchall()
    performance_dict = {row['store']: row['total_net_percentage'] for row in performance_data}

    update_cursor = conn.cursor()
    updated_count = 0

    for row in rows:
        store_name = row['store']
        group_code = row['goods_price_group']
        total_sales = row['total_sales'] or Decimal('0.00')
        total_commission = row['total_commission'] or Decimal('0.00')

        # Получаем план выполнения для магазина
        total_net_percentage = performance_dict.get(store_name, 0.0)

        # Проверка на выполнение плана для xiaomi и соответствующих ценовых групп
        if re.search(r'xiaomi', store_name, re.IGNORECASE) and group_code in ['001_Х1.2_Э', '002_Х1.2_Э']:
            if total_net_percentage < 100:
                total_commission = total_sales * Decimal('0.015')   # Если выполнение < 100%, умножаем на 1.5
            else:
                total_commission = total_sales * Decimal('0.018')   # Если выполнение > 100%, умножаем на 1.8

            # Обновляем в оригинальной таблице
            update_cursor.execute("""
                UPDATE sales_detail_kpi
                SET total_commission = %s
                WHERE id = %s
            """, (total_commission, row['id']))
            updated_count += 1

        # Получаем коэффициент KPI
        kpi_coefficient = row['kpi_coefficient'] or Decimal('1.00')

        # Рассчитываем итоговую сумму
        total_sum = total_commission * kpi_coefficient

        # Копируем строку в all_detail с учётом новой комиссии и итоговой суммы
        row_copy = row.copy()
        row_copy['total_commission'] = total_commission
        row_copy['total_sum'] = total_sum

        update_cursor.execute("""
            REPLACE INTO all_detail (
                id, store, regional_director, employee, category,
                sale_date, goods_name, total_sales, total_commission,
                total_quantity, goods_price_group, kpi_coefficient, total_sum
            ) VALUES (
                %(id)s, %(store)s, %(regional_director)s, %(employee)s, %(category)s,
                %(sale_date)s, %(goods_name)s, %(total_sales)s, %(total_commission)s,
                %(total_quantity)s, %(goods_price_group)s, %(kpi_coefficient)s, %(total_sum)s
            );
        """, row_copy)

    conn.commit()
    cursor.close()
    update_cursor.close()
    conn.close()

    print(f"✅ Обновлено комиссий: {updated_count}")
    print("✅ Таблица all_detail успешно создана/обновлена.")

if __name__ == "__main__":
    update_commission_and_copy_to_all_detail()
#################################### РАБОТАЕТ !!!! XIAOMI #####################################################