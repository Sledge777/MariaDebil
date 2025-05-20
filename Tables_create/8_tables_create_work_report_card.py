import mysql.connector
import csv
from datetime import datetime

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
    print("Создание таблицы work_report_card...", end='')
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS work_report_card (
            id INT AUTO_INCREMENT PRIMARY KEY,
            date DATE,
            store TEXT,
            manager TEXT,
            work_shifts INT
        )
    """)
    print("OK")

def load_data_from_txt(file_path, cursor, conn):
    with open(file_path, 'r', encoding='utf-8-sig') as f:
        reader = csv.DictReader(f, delimiter='\t')
        if reader.fieldnames:
            reader.fieldnames[0] = reader.fieldnames[0].replace('\ufeff', '')
        print("🔍 Заголовки в файле:", reader.fieldnames)

        count = 0

        for row in reader:
            try:
                if 'Итого' in row['store']:
                    continue

                work_date = None
                if row['date']:
                    try:
                        work_date = datetime.strptime(row['date'], "%d.%m.%Y").date()
                    except ValueError:
                        work_date = None

                work_shifts = None
                if row.get('Количество записей'):
                    work_shifts = int(row['Количество записей'].replace('\xa0', '').replace(' ', '').strip())
                
                cursor.execute("""
                    INSERT INTO work_report_card (
                        date, store, manager, work_shifts
                    ) VALUES (%s, %s, %s, %s)
                """, (
                    work_date,
                    row['store'],
                    row['manager'],
                    work_shifts
                ))
                count += 1
            except Exception as e:
                print("❌ Ошибка в строке:", row)
                print(e)

        conn.commit()
        commit_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        print(f"✅ Загружено {count} записей в таблицу work_report_card. Коммит выполнен в: {commit_time}")

# Основной блок
if __name__ == "__main__":
    conn = create_connection()
    cursor = conn.cursor()

    create_tables(cursor)

    file_path = r'C:\Users\root\Desktop\Final\reports\work_report_card.txt'
    load_data_from_txt(file_path, cursor, conn)

    cursor.close()
    conn.close()
