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

# Функция для подсчёта заработка сотрудников по всем магазинам
def calculate_total_payment():
    connection = create_connection()
    if connection is None:
        return  # Если подключение не удалось, выходим

    try:
        # Создание новой таблицы для заработка сотрудников по всем магазинам
        create_table_query = '''
        CREATE TABLE IF NOT EXISTS employee_total_payment (
            id INT AUTO_INCREMENT PRIMARY KEY,
            employee TEXT,
            total_payment INT
        );
        '''
        cursor = connection.cursor()
        cursor.execute(create_table_query)

        # Запрос для подсчёта общего заработка каждого сотрудника по всем магазинам
        insert_query = '''
        INSERT INTO employee_total_payment (employee, total_payment)
        SELECT 
            w.manager AS employee,
            COUNT(DISTINCT w.date) * IFNULL(rates.shift_cost, 0) AS total_payment
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
            w.manager;
        '''
        cursor.execute(insert_query)
        connection.commit()
        print("Total payment for employees across all stores calculated and inserted successfully.")

    except Error as e:
        print(f"Error: {e}")
    finally:
        # Закрытие курсора и соединения
        if cursor:
            cursor.close()
        if connection.is_connected():
            connection.close()
            print("Connection closed.")

# Вызов функции для подсчёта заработка сотрудников по всем магазинам
calculate_total_payment()
