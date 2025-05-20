import mysql.connector
import csv
from datetime import datetime

def parse_date(value):
    try:
        if value and value.strip():
            return datetime.strptime(value.strip(), '%d.%m.%Y').date()
    except Exception:
        pass
    return None
#--------------------------------------
def create_connection():
    return mysql.connector.connect(
        host="localhost",
        user="root",
        password="Ms_phone_fox62052!",
        database="ms_phone"
    )
#--------------------------------------
def create_tables(cursor):
    print("Создание таблицы employee...", end='')
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS employee (
            id INT AUTO_INCREMENT PRIMARY KEY,
            full_name TEXT,
            date_of_birth DATE,
            tax_id TEXT,
            location TEXT,
            post TEXT,
            is_dismissed BOOLEAN
        )
    """)
    print("OK")

def load_data_from_txt(file_path, cursor, conn):
    with open(file_path, 'r', encoding='utf-8-sig') as f:
        reader = csv.DictReader(f, delimiter='\t')
        if reader.fieldnames:
            reader.fieldnames[0] = reader.fieldnames[0].replace('\ufeff', '')
        print("🔍 Заголовки в файле:", reader.fieldnames)

        for row in reader:
            try:
                cursor.execute("""
                    INSERT INTO employee (
                        full_name, date_of_birth, tax_id,
                        location, post, is_dismissed
                    ) VALUES (%s, %s, %s, %s, %s, %s)
                """, (
                    row['full_name'],
                    parse_date(row['date_of_birth']),
                    row['tax_id'],
                    row['location'],
                    row['post'],
                    row['is_dismissed'].strip().lower() == 'да'
                ))
            except Exception as e:
                print("❌ Ошибка в строке:", row)
                print(e)

        conn.commit()
    #----------------------------------------------------------------------------------
    commit_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    print("✅ Данные успешно загружены в MariaDB.")
    print(f"Запись в таблицу employee выполнена в: {commit_time}")
    #----------------------------------------------------------------------------------

# Основной блок
if __name__ == "__main__":
    conn = create_connection()
    cursor = conn.cursor()

    create_tables(cursor)

    file_path = r'C:\Users\root\Desktop\Final\reports\employee.txt'
    load_data_from_txt(file_path, cursor, conn)

    cursor.close()
    conn.close()
