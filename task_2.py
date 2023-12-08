import sqlite3
import json

def whrite_json(items: list, path_name: str):
    json_items = json.dumps(items, ensure_ascii=False)
    with open(path_name, 'w', encoding='utf-8') as f:
        f.write(json_items)

def parse_data(file_name):
    items = []
    with open(file_name, 'r', encoding="utf-8") as f:
        lines = f.readlines()
        item = dict()
        for line in lines:
            if line == '=====\n':
                items.append(item)
                item = dict()
            else:
                line = line.strip()

                splitted = line.split("::")

                if splitted[0] == 'price':
                    item[splitted[0]] = int(splitted[1])
                else:
                    item[splitted[0]] = splitted[1]

    return items

def connect_to_db(file_name):
    connection = sqlite3.connect(file_name)
    connection.row_factory = sqlite3.Row
    return connection

def create_table(db):
    cursor = db.cursor()
    cursor.execute("CREATE TABLE market (title_id, price, place, date)")
    db.commit()

def insert_data(db, data):
    cursor = db.cursor()

    cursor.executemany("""
        INSERT INTO market (title_id, price, place, date)
        VALUES (
            (SELECT id FROM book WHERE title = :title), :price, :place, :date
            )
    """, data)
    db.commit()

# Отсортированные цены на книгу
def get_price_books_top_rating(db, name):
    cursor = db.cursor()
    res = cursor.execute("SELECT price FROM market WHERE title_id = (SELECT id FROM book WHERE title=?) ORDER by price;", [name])
    items = []
    for row in res.fetchall():
        item = dict(row)
        items.append(item)

    cursor.close()
    return items

# рейтинг 10 самых доргих книг
def get_rating_top_price(db):
    cursor = db.cursor()
    res = cursor.execute("SELECT book.title, book.rating, market.price FROM book, market WHERE book.id = market.title_id ORdER BY market.price DESC LIMIT 10")
    items = []
    for row in res.fetchall():
        item = dict(row)
        items.append(item)

    cursor.close()
    return items

# средний прайс на online площадках на каждую книгу
def get_avg_price(db):
    cursor = db.cursor()
    res = cursor.execute("""SELECT title, (SELECT ROUND(AVG(price), 2) FROM market WHERE place='online' and book.id = title_id) AS avg_price
FROM book
ORDER BY avg_price DESC;""")
    items = []
    for row in res.fetchall():
        item = dict(row)
        items.append(item)

    cursor.close()
    return items

def main():
    file_name = 'tasks/task_2_var_53_subitem.text'

    data = parse_data(file_name)

    db = connect_to_db('db/task1.db')

    # заполнение таблицы данными
    # create_table(db)
    # insert_data(db, data)

    name = 'Фауст'
    price_books = get_price_books_top_rating(db, name)
    whrite_json(price_books, f'answers/task_2_price_{name}.json')

    rating_top_price = get_rating_top_price(db)
    whrite_json(rating_top_price, 'answers/task_2_top.json')

    avg_price = get_avg_price(db)
    whrite_json(avg_price, 'answers/task_2_avg.json')


    db.close()

if __name__ == "__main__":
    main()