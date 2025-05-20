import mysql.connector
from mysql.connector import Error

# Ставки для разных позиций
rates = {
    '1. ДМ': 1000,
    '3. СПК': 850,
    '4. ПК': 700
}

# Функция для создания подключения к базе данных
def create_connection():
    try:
        connection = mysql.connector.connect(
            host="localhost",
            user="root",
            password="Ms_phone_fox62052!",
            database="ms_phone"
        )
        if connection.is_connected():
            print("Connection successfully established")
        return connection
    except Error as e:
        print(f"Error: {e}")
        return None

# Функция для подсчёта выходов сотрудников с детализацией по датам
def calculate_work_shifts():
    connection = create_connection()
    if connection is None:
        return  # Если подключение не удалось, выходим

    try:
        # Создание новой таблицы для детализированных выходов сотрудников по датам без поля sum_payment
        create_table_query = '''
        CREATE TABLE IF NOT EXISTS employee_daily_shifts (
            id INT AUTO_INCREMENT PRIMARY KEY,
            store TEXT,
            employee TEXT,
            work_date DATE,
            post TEXT,
            shift_cost INT
        );
        '''
        cursor = connection.cursor()
        cursor.execute(create_table_query)

        # Запрос для вставки детализированных данных по каждому выходу сотрудника
        insert_query = '''
        INSERT INTO employee_daily_shifts (store, employee, work_date, post, shift_cost)
        SELECT 
            w.store,
            w.manager AS employee,
            w.date AS work_date,
            e.post,
            IFNULL(rates.shift_cost, 0) AS shift_cost
        FROM 
            work_report_card w
        JOIN 
            employee e ON w.manager = e.full_name
        LEFT JOIN 
            (SELECT * FROM (VALUES
                ('1. ДМ', 1000),
                ('3. СПК', 850),
                ('4. ПК', 700)
            ) AS rates(post, shift_cost)) rates ON e.post = rates.post
        GROUP BY 
            w.store, w.manager, w.date, e.post;
        '''
        cursor.execute(insert_query)
        connection.commit()
        print("Daily work shifts calculated and inserted successfully.")

    except Error as e:
        print(f"Error: {e}")
    finally:
        # Закрытие курсора и соединения
        if cursor:
            cursor.close()
        if connection.is_connected():
            connection.close()
            print("Connection closed.")

# Вызов функции для подсчёта выходов с детализацией по датам
calculate_work_shifts()
