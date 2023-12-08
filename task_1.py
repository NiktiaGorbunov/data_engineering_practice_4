import msgpack
import sqlite3
import json

def whrite_json(items: list, path_name: str):
    json_items = json.dumps(items, ensure_ascii=False)
    with open(path_name, 'w', encoding='utf-8') as f:
        f.write(json_items)

def parse_data(file_name):
    with open(file_name, "rb") as data_file:
        byte_data = data_file.read()
        data_loaded = msgpack.unpackb(byte_data)

    return data_loaded

def connect_to_db(file_name):
    connection = sqlite3.connect(file_name)
    connection.row_factory = sqlite3.Row
    return connection

def create_table(db):
    cursor = db.cursor()
    cursor.execute("CREATE TABLE book (id INTEGER PRIMARY KEY AUTOINCREMENT, title, author, genre, pages, published_year, isbn, rating, views)")
    db.commit()

def insert_data(db, data):
    cursor = db.cursor()

    cursor.executemany("""
        INSERT or REPLACE INTO book (title, author, genre, pages, published_year, isbn, rating, views)
        VALUES (:title, :author, :genre, :pages, :published_year, :isbn, :rating, :views)
    """, data)
    db.commit()

def get_top_by_views(db, limit):
    cursor = db.cursor()
    res = cursor.execute("SELECT * FROM book ORDER BY views DESC LIMIT ?", [limit])
    items = []
    for row in res.fetchall():
        items.append(dict(row))

    cursor.close()
    return items

def get_stat_by_pages(db):
    cursor = db.cursor()
    res = cursor.execute("""
        SELECT 
            SUM(pages) as sum,
            AVG(pages) as avg,
            MIN(pages) as min,  
            MAX(pages) as max
        FROM book
    """)

    stat = dict(res.fetchone())
    cursor.close()
    return stat

def get_freq_by_century(db):
    cursor = db.cursor()
    res = cursor.execute("""
        SELECT 
            CAST (count(*) as REAL) / (SELECT COUNT(*) FROM book) as count, (FLOOR(published_year/100)+1) as century
        FROM book
        GROUP BY (FLOOR(published_year/100)+1)
    """)
    items = list()
    for row in res.fetchall():
        items.append(dict(row))

    cursor.close()
    return items

def get_filter_by_year(db, min_year, limit=10):
    cursor = db.cursor()
    res = cursor.execute("""
        SELECT *
        FROM book
        WHERE published_year > ?
        ORDER BY views DESC
        LIMIT ?
    """, [min_year, limit])

    items = list()
    for row in res.fetchall():
        items.append(dict(row))

    cursor.close()
    return items

def main():
    file_name = 'tasks/task_1_var_53_item.msgpack'

    data = parse_data(file_name)
    db = connect_to_db('db/task1.db')

    # заполнение таблицы данными
    # create_table(db)
    # insert_data(db, data)

    top_by_views = get_top_by_views(db, 10)
    whrite_json(top_by_views, 'answers/task_1_top.json')

    stat_by_pages = list()
    stat_by_pages.append(get_stat_by_pages(db))
    whrite_json(stat_by_pages, 'answers/task_1_stat.json')

    freq_by_century = get_freq_by_century(db)
    whrite_json(freq_by_century, 'answers/task_1_freq.json')

    filter_by_year = get_filter_by_year(db, 1950)
    whrite_json(filter_by_year, 'answers/task_1_filter.json')

    db.close()

if __name__ == "__main__":
    main()