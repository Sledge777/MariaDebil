import mysql.connector
from decimal import Decimal

def create_connection():
    return mysql.connector.connect(
        host="localhost",
        user="root",
        password="Ms_phone_fox62052!",
        database="ms_phone"
    )

def create_or_update_table():
    conn = create_connection()
    cursor = conn.cursor()

    create_table_query = """
    CREATE TABLE IF NOT EXISTS aggregated_sales_data (
        id INT AUTO_INCREMENT PRIMARY KEY,
        store VARCHAR(255),
        regional_director TEXT,
        employee TEXT,
        total_sales_sum DECIMAL(12, 2),
        total_commission_sum DECIMAL(12, 2),
        total_commission_kpi DECIMAL(12, 2),
        net_over DECIMAL(5, 2) DEFAULT 1,
        zp DECIMAL(12, 2) DEFAULT 0,
        total_payment INT DEFAULT 0,
        final DECIMAL(12, 2) DEFAULT 0
    );
    """
    
    cursor.execute(create_table_query)
    conn.commit()
    cursor.close()
    conn.close()

def insert_aggregated_data():
    conn = create_connection()
    cursor = conn.cursor()

    # Получаем суммы продаж и комиссий по сотруднику
    select_query = """
    SELECT 
        regional_director,
        employee,
        SUM(total_sales) AS total_sales_sum,
        SUM(total_commission) AS total_commission_sum,
        SUM(total_sum) AS total_commission_kpi
    FROM 
        all_detail
    GROUP BY 
        regional_director, employee;
    """
    cursor.execute(select_query)
    results = cursor.fetchall()

    # Получаем выполнение плана по магазинам
    cursor.execute("SELECT store, total_net_percentage FROM ms_phone_performance;")
    performance_data = cursor.fetchall()
    performance_dict = {store: total_net_percentage for store, total_net_percentage in performance_data}

    # Получаем список магазинов, где работал каждый сотрудник
    cursor.execute("SELECT DISTINCT employee, store FROM all_detail;")
    employee_store_data = cursor.fetchall()
    employee_store_map = {}
    for employee, store in employee_store_data:
        employee_store_map.setdefault(employee, []).append(store)

    # Получаем выходы на работу (один раз на сотрудника)
    cursor.execute("SELECT employee, total_payment FROM employee_total_payment;")
    payment_data = cursor.fetchall()
    payment_dict = {employee: total_payment for employee, total_payment in payment_data}

    # Подготовка запроса для вставки агрегированных данных
    insert_query = """
    INSERT INTO aggregated_sales_data (
        store, regional_director, employee, 
        total_sales_sum, total_commission_sum, total_commission_kpi, 
        net_over, zp, total_payment, final
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
    """

    for regional_director, employee, total_sales_sum, total_commission_sum, total_commission_kpi in results:
        stores = employee_store_map.get(employee, [])
        max_net = max([performance_dict.get(s, 100) for s in stores], default=100)
        net_over = Decimal('1.3') if max_net > 100 else Decimal('1.0')
        zp = total_commission_kpi * net_over
        total_payment = payment_dict.get(employee, 0)
        final = zp + total_payment

        # Формируем строку со списком магазинов, где сотрудник работал
        store_repr = " / ".join(sorted(set(stores)))

        cursor.execute(insert_query, (
            store_repr, regional_director, employee,
            total_sales_sum, total_commission_sum, total_commission_kpi,
            net_over, zp, total_payment, final
        ))

    conn.commit()
    cursor.close()
    conn.close()

# Запуск
create_or_update_table()
insert_aggregated_data()

print("Таблица и данные успешно обновлены с учётом уникальных выходов и распределённых продаж.")
