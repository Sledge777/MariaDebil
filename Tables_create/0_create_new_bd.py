import mysql.connector

# Подключение к MariaDB
conn = mysql.connector.connect(
    host='localhost',
    user='root',
    password='Ms_phone_fox62052!'
)

cursor = conn.cursor()

# Создание базы данных
cursor.execute("CREATE DATABASE IF NOT EXISTS ms_phone")

# Закрытие подключения
cursor.close()
conn.close()

