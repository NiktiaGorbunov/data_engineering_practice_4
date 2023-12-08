import pickle
import sqlite3
import json

def whrite_json(items: list, path_name: str):
    json_items = json.dumps(items, ensure_ascii=False)
    with open(path_name, 'w', encoding='utf-8') as f:
        f.write(json_items)


def parse_data_pkl(file_name):
    objects = []
    with open(file_name, "rb") as openfile:
        while True:
            try:
                objects.append(pickle.load(openfile))
            except EOFError:
                break

    for object in objects[0]:
        object['duration_ms'] = int(object['duration_ms'])
        object['year'] = int(object['year'])
        object['tempo'] = float(object['tempo'])
        object['acousticness'] = float(object['acousticness'])
        object['energy'] = float(object['energy'])
        object['popularity'] = int(object['popularity'])

    return objects[0]

def parse_data_text(file_name):
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

                if splitted[0] == 'duration_ms':
                    item[splitted[0]] = int(splitted[1])
                elif splitted[0] == 'year':
                    item[splitted[0]] = int(splitted[1])
                elif splitted[0] == 'tempo':
                    item[splitted[0]] = float(splitted[1])
                elif splitted[0] == 'instrumentalness':
                    item[splitted[0]] = float(splitted[1])
                elif splitted[0] == 'loudness':
                    item[splitted[0]] = float(splitted[1])
                else:
                    item[splitted[0]] = splitted[1]

    return items

def connect_to_db(file_name):
    connection = sqlite3.connect(file_name)
    connection.row_factory = sqlite3.Row
    return connection


def create_table(db):
    cursor = db.cursor()
    cursor.execute("CREATE TABLE songs (id INTEGER PRIMARY KEY AUTOINCREMENT, artist, song, duration_ms, year, tempo, genre)")
    db.commit()

def insert_data(db, data):
    cursor = db.cursor()

    cursor.executemany("""
        INSERT or REPLACE INTO songs (artist, song, duration_ms, year, tempo, genre)
        VALUES (:artist, :song, :duration_ms, :year, :tempo, :genre)
    """, data)
    db.commit()

def get_top_by_tempo(db, limit):
    cursor = db.cursor()
    res = cursor.execute("SELECT * FROM songs ORDER BY tempo DESC LIMIT ?", [limit])
    items = []
    for row in res.fetchall():
        items.append(dict(row))

    cursor.close()
    return items

def get_stat_by_tempo(db):
    cursor = db.cursor()
    res = cursor.execute("""
        SELECT 
            SUM(tempo) as sum,
            AVG(tempo) as avg,
            MIN(tempo) as min,  
            MAX(tempo) as max
        FROM songs
    """)

    stat = dict(res.fetchone())
    cursor.close()
    return stat

def get_freq_by_artist(db):
    cursor = db.cursor()
    res = cursor.execute("""
        SELECT artist,
            CAST (count(*) as REAL) / (SELECT COUNT(*) FROM songs) as freq
        FROM songs
        GROUP BY artist
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
        FROM songs
        WHERE year > ?
        ORDER BY duration_ms DESC
        LIMIT ?
    """, [min_year, limit])

    items = list()
    for row in res.fetchall():
        items.append(dict(row))

    cursor.close()
    return items

def main():
    file_name_pkl = 'tasks/task_3_var_53_part_1.pkl'
    file_name_text = 'tasks/task_3_var_53_part_2.text'

    data_part_1 = parse_data_pkl(file_name_pkl)
    data_part_2 = parse_data_text(file_name_text)

    data = data_part_1 + data_part_2

    db = connect_to_db('db/task3.db')

    # заполнение таблицы данными
    # create_table(db)
    insert_data(db, data)

    top_by_tempo = get_top_by_tempo(db, 10)
    whrite_json(top_by_tempo, 'answers/task_3_top.json')

    stat_by_tempo = list()
    stat_by_tempo.append(get_stat_by_tempo(db))
    whrite_json(stat_by_tempo, 'answers/task_3_stat.json')

    freq_by_artist = get_freq_by_artist(db)
    whrite_json(freq_by_artist, 'answers/task_3_freq.json')

    filter_by_year = get_filter_by_year(db, 2010, 48)
    whrite_json(filter_by_year, 'answers/task_3_filter.json')

    db.close()

if __name__ == "__main__":
    main()

