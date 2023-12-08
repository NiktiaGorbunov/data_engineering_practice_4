import json
import msgpack
import csv
import sqlite3


def whrite_json(items: list, path_name: str):
    json_items = json.dumps(items, ensure_ascii=False)
    with open(path_name, 'w', encoding='utf-8') as f:
        f.write(json_items)

def parse_data_msgpack(file_name):
    with open(file_name, "rb") as data_file:
        byte_data = data_file.read()
        data_loaded = msgpack.unpackb(byte_data)

    return data_loaded

def parse_data_csv(file_name):
    items = []
    with open(file_name, mode='r') as f:
        reader = list(csv.reader(f, delimiter=';'))
        key = reader[0]

        for row in reader[1:]:
            if len(row) != 0:
                param = None
                if row[2] in ['True', 'False']:
                    param = bool(row[2])
                elif len(row[2]) != 0:
                    param = float(row[2])

                item = {key[0]: row[0],
                        key[1]: row[1],
                        key[2]: param,
                        }
                items.append(item)

    return items

def connect_to_db(file_name):
    connection = sqlite3.connect(file_name)
    connection.row_factory = sqlite3.Row
    return connection



{'name': 'Intel Core i5', 'price': 66989, 'quantity': 33, 'fromCity': 'Луго', 'isAvailable': True, 'views': 28918}

def create_table(db):
    cursor = db.cursor()
    cursor.execute("CREATE TABLE products (id INTEGER PRIMARY KEY AUTOINCREMENT, name, price, quantity, fromCity, isAvailable, views, count_update DEFAULT 0)")
    db.commit()

def insert_data(db, data):
    cursor = db.cursor()

    cursor.executemany("""
        INSERT or REPLACE INTO products (name, price, quantity, fromCity, isAvailable, views)
        VALUES (:name, :price, :quantity, :fromCity, :isAvailable, :views)
    """, data)
    db.commit()

def delete_by_name(db, name):
    cursor = db.cursor()
    cursor.execute("DELETE FROM products WHERE name = ?", [name])
    db.commit()

def update_price_percent_by_name(db, name, percent):
    cursor = db.cursor()
    cursor.execute("UPDATE products SET price = price * (1 + ?) WHERE name = ?", [percent, name])
    cursor.execute("UPDATE products SET count_update = count_update + 1 WHERE name = ?", [name])
    db.commit()

def update_price_abs_by_name(db, name, num):
    cursor = db.cursor()
    res = cursor.execute("UPDATE products SET price = price + ? WHERE name = ? AND (price + ?) >= 0", [num, name, num])
    if res.rowcount > 1:
        cursor.execute("UPDATE products SET count_update = count_update + 1 WHERE name = ?", [name])
    db.commit()

def update_quantity_by_name(db, name, quantity):
    cursor = db.cursor()
    res = cursor.execute("UPDATE products SET quantity = quantity + ? WHERE name = ? AND (quantity + ?) >= 0", [quantity, name, quantity])
    if res.rowcount > 1:
        cursor.execute("UPDATE products SET count_update = count_update + 1 WHERE name = ?", [name])
    db.commit()

def update_available_by_name(db, name, available):
    cursor = db.cursor()
    cursor.execute("UPDATE products SET isAvailable = ? WHERE name = ?", [available, name])
    cursor.execute("UPDATE products SET count_update = count_update + 1 WHERE name = ?", [name])
    db.commit()

def handle_update(db, update_items):
    for item in update_items:
        menu = {
            'remove': 0 , #delete_by_name(db, item['name']),
            'price_percent': update_price_percent_by_name(db, item['name'], item['param']),
            'price_abs': update_price_abs_by_name(db, item['name'], item['param']),
            'quantity_add': update_quantity_by_name(db, item['name'], item['param']),
            'quantity_sub': update_quantity_by_name(db, item['name'], item['param']),
            'available': update_available_by_name(db, item['name'], item['param']),
        }

        answer = menu[item['method']]

def get_top_by_update(db):
    cursor = db.cursor()
    res = cursor.execute("SELECT * FROM products ORDER BY count_update DESC LIMIT 10")
    items = [dict(row) for row in res]
    cursor.close()
    return items

def get_stat_by_price(db):
    cursor = db.cursor()
    res = cursor.execute("""
            SELECT 
                fromCity,
                SUM(price) as sum,
                AVG(price) as avg,
                MIN(price) as min,  
                MAX(price) as max
            FROM products
            GROUP BY fromCity
        """)

    stat = [dict(row) for row in res]
    cursor.close()
    return stat

def get_stat_by_quantity(db):
    cursor = db.cursor()
    res = cursor.execute("""
                SELECT 
                    fromCity,
                    SUM(quantity) as sum,
                    AVG(quantity) as avg,
                    MIN(quantity) as min,  
                    MAX(quantity) as max,
                    COUNT(*) AS count
                FROM products
                GROUP BY fromCity
            """)

    stat = [dict(row) for row in res]
    cursor.close()
    return stat

def get_filter_by_price_and_views(db, min_price, max_views, limit=30):
    cursor = db.cursor()
    res = cursor.execute("SELECT * FROM products WHERE price >= ? AND views <= ? ORDER BY price DESC LIMIT ?", [min_price, max_views, limit])

    stat = [dict(row) for row in res.fetchall()]
    cursor.close()
    return stat

def main():
    file_name_msgpack = 'tasks/task_4_var_53_product_data.msgpack'
    file_name_csv = 'tasks/task_4_var_53_update_data.csv'

    product = parse_data_msgpack(file_name_msgpack)
    update = parse_data_csv(file_name_csv)

    db = connect_to_db('db/task4.db')

    # заполнение таблицы данными
    # create_table(db)
    # insert_data(db, product)

    # обновление данных
    # handle_update(db, update)


    top_by_update = get_top_by_update(db)
    whrite_json(top_by_update, 'answers/task_4_top.json')

    stat_by_price = get_stat_by_price(db)
    whrite_json(stat_by_price, 'answers/task_4_stat_by_price.json')

    stat_by_quantity = get_stat_by_quantity(db)
    whrite_json(stat_by_quantity, 'answers/task_4_stat_by_quantity.json')

    price_and_views = get_filter_by_price_and_views(db, 50, 543210)
    whrite_json(price_and_views, 'answers/task_4_ price_and_views.json')

    db.close()

if __name__ == "__main__":
    main()


