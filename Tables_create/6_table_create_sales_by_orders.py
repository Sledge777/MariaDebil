import mysql.connector
import csv
import re
from datetime import datetime

########################################################################################
def parse_decimal(value):
    if not value or value.strip() == '':
        return 0.0
    return float(value.replace('\xa0', '').replace(' ', '').replace(',', '.'))

def extract_datetime(text):
    match = re.search(r'от (\d{2}\.\d{2}\.\d{4} \d{1,2}:\d{2}:\d{2})', text)
    if match:
        try:
            return datetime.strptime(match.group(1), "%d.%m.%Y %H:%M:%S")
        except ValueError:
            return None
    return None
########################################################################################

def create_connection():
    return mysql.connector.connect(
        host="localhost",
        user="root",
        password="Ms_phone_fox62052!",
        database="ms_phone"
    )

def create_tables(cursor):
    print("Создание таблицы sales_by_orders...", end='')
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS sales_by_orders (
            id INT AUTO_INCREMENT PRIMARY KEY,
            date DATETIME,
            order_id TEXT,
            store TEXT,
            employee TEXT,
            goods_id TEXT,
            quantity DECIMAL(10, 3),
            amount DECIMAL(12, 2),
            promo_discount DECIMAL(12, 2),
            manual_discount DECIMAL(12, 2),
            bonus_discount DECIMAL(12, 2),
            goods_price_group TEXT,
            serial_number TEXT
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
            # Пропускаем строки с итогами
            if 'Итого' in row['order_id']:
                continue

            try:
                order_dt = extract_datetime(row['order_id'])
                # ВАЖНО: НЕ ДЕЛАЕМ .date(), оставляем время!

                cursor.execute("""
                    INSERT INTO sales_by_orders (
                        date, order_id, store, employee, goods_id,
                        quantity, amount, promo_discount, manual_discount, 
                        bonus_discount, goods_price_group, serial_number
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    order_dt,
                    row['order_id'],
                    row['store'],
                    row['employee'],
                    row['goods_id'],
                    parse_decimal(row['quantity']),
                    parse_decimal(row['amount']),
                    parse_decimal(row['promo_discount']),
                    parse_decimal(row['manual_discount']),
                    parse_decimal(row['bonus_discount']),
                    row['goods_price_group'],
                    row['serial_number']
                ))
                count += 1
            except Exception as e:
                print("❌ Ошибка в строке:", row)
                print(e)

        conn.commit()
        commit_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        print(f"✅ Загружено {count} записей в таблицу sales_by_orders. Коммит выполнен в: {commit_time}")

# Основной блок
if __name__ == "__main__":
    conn = create_connection()
    cursor = conn.cursor()

    create_tables(cursor)

    file_path = r'C:\Users\root\Desktop\Final\reports\sales_by_orders.txt'
    load_data_from_txt(file_path, cursor, conn)

    cursor.close()
    conn.close()
