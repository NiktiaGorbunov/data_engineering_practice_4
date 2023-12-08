import json
import msgpack
import csv
import sqlite3
from bs4 import BeautifulSoup
import xml


def whrite_json(items: list, path_name: str):
    json_items = json.dumps(items, ensure_ascii=False)
    with open(path_name, 'w', encoding='utf-8') as f:
        f.write(json_items)

def parse_data_json(file_name):
    with open(file_name, encoding='utf-8') as file:
        data = json.load(file)

        countries = list()

        for item in data:

            countri = {
                "name": item["name"],
                "iso2": item["iso2"],
                "iso3": item["iso3"],
                "phone_code": item["phone_code"],
                "capital": item["capital"],
                "region": item["region"],
                "subregion": item["subregion"],
            }

            countries.append(countri)
    return countries



def parse_data_csv(file_name):
    items = []
    with open(file_name, mode='r', encoding='utf-8') as f:
        reader = list(csv.reader(f, delimiter=','))
        key = reader[0]
        for row in reader[1:]:

            item = {key[1]: row[1],
                    key[2]: int(row[2]),
                    key[3]: row[3],
                    key[4]: int(row[4]),
                    key[5]: row[5],
                    key[6]: float(row[6]),
                    key[7]: float(row[7]),
                    }
            items.append(item)

    return items


def parse_data_xml(file_name):
    states = list()
    with open(file_name, encoding='utf-8') as file:
        xml = file.read()
        site = BeautifulSoup(xml, 'xml')
        blocks = site.find_all('state')
        for block in blocks:
            item = dict()
            item['id'] = int(block.find('id').get_text().strip())
            item['name'] = block.find('name').get_text().strip()
            item['country_id'] = int(block.find('country_id').get_text().strip())
            item['country_code'] = block.find('country_code').get_text().strip()
            item['state_code'] = block.find('state_code').get_text().strip()
            item['latitude'] = float(block.find('latitude').get_text().strip()) if block.find('latitude').get_text().strip() != '' else None
            item['longitude'] = float(block.find('longitude').get_text().strip()) if block.find('latitude').get_text().strip() != '' else None
            states.append(item)

    return states

def connect_to_db(file_name):
    connection = sqlite3.connect(file_name)
    connection.row_factory = sqlite3.Row
    return connection


def create_tables(db):
    cursor = db.cursor()
    cursor.execute("CREATE TABLE cities (id INTEGER PRIMARY KEY AUTOINCREMENT, name,state_id,state_code,country_id,country_code,latitude,longitude)")
    cursor.execute("CREATE TABLE countries (id INTEGER PRIMARY KEY AUTOINCREMENT, name, iso2, iso3, phone_code, capital, region, subregion)")
    cursor.execute("CREATE TABLE states (id, name, country_id, country_code, state_code, latitude, longitude)")
    db.commit()

def insert_data_cities(db, cities):
    cursor = db.cursor()
    cursor.executemany("""
        INSERT or REPLACE INTO cities (name,state_id,state_code,country_id,country_code,latitude,longitude)
        VALUES (:name, :state_id, :state_code, :country_id, :country_code, :latitude, :longitude)
    """, cities)
    db.commit()

def insert_data_countries(db, countries):
    cursor = db.cursor()
    cursor.executemany("""
            INSERT or REPLACE INTO countries (name, iso2, iso3, phone_code, capital, region, subregion)
            VALUES (:name, :iso2, :iso3, :phone_code, :capital, :region, :subregion)
        """, countries)
    db.commit()

def insert_data_states(db,states):
    cursor = db.cursor()
    cursor.executemany("""
            INSERT or REPLACE INTO states (id, name, country_id, country_code, state_code, latitude, longitude)
            VALUES (:id, :name, :country_id, :country_code, :state_code, :latitude, :longitude)
        """, states)
    db.commit()

# координаты столиц всех стран
def get_coordinates_capitals(db):
    cursor = db.cursor()
    res = cursor.execute("SELECT cities.name as capital, countries.name as countrie, cities.latitude, cities.longitude FROM cities JOIN countries ON cities.name = countries.capital")
    items = [dict(row) for row in res]
    cursor.close()
    return items

# Кол-во областей в регионе
def get_count_states(db, state='Europe'):
    cursor = db.cursor()
    res = cursor.execute("SELECT COUNT(name) FROm states WHERE states.country_code in (SELECT iso2 FROM countries WHERE region = ?)", [state])
    items = [dict(row) for row in res]
    cursor.close()
    return items


# топ 5 северных городов
def get_top_nord_cities(db, limit=10):
    cursor = db.cursor()
    res = cursor.execute("SELECT cities.name, countries.name, cities.latitude, cities.longitude FROM cities, countries WHERE cities.country_code = countries.iso2 ORDER BY cities.latitude DESC LIMIT ?", [limit])
    items = [dict(row) for row in res]
    cursor.close()
    return items


# кол-во городов в странах
def get_count_cities(db):
    cursor = db.cursor()
    res = cursor.execute("SELECT COUNT(*) as count, countries.name FROM cities JOIN countries ON cities.country_code = countries.iso2  GROUP BY countries.name ORDER BY count DESC")
    items = [dict(row) for row in res]
    cursor.close()
    return items


# обновить код номера телефона у страны
def update_phone_number(db,new_code, capital):
    cursor = db.cursor()
    cursor.execute("UPDATE countries SET phone_code = ? WHERE capital = ?", [new_code, capital])
    db.commit()


# Удалить город
def delete_cities_by_name(db, name):
    cursor = db.cursor()
    cursor.execute("DELETE FROM cities WHERE name = ?", [name])
    db.commit()

def main():
    file_name_cities = 'task_5/cities.csv'
    file_name_countries = 'task_5/countries.json'
    file_name_states = 'task_5/states.xml'

    # cities = parse_data_csv(file_name_cities)
    # countries = parse_data_json(file_name_countries)
    # states = parse_data_xml(file_name_states)

    db = connect_to_db('db/task5.db')

    # заполнение таблицы данными
    # create_tables(db)
    # insert_data_cities(db, cities)
    # insert_data_countries(db, countries)
    #insert_data_states(db, states)

    coordinates_capitals = get_coordinates_capitals(db)
    whrite_json(coordinates_capitals, 'task_5/task_5_coordinates_capitals.json')
    count_states = get_count_states(db, 'Europe')
    whrite_json(count_states, 'task_5/task_5_count_states.json')
    top_nord_cities = get_top_nord_cities(db, 10)
    whrite_json(top_nord_cities, 'task_5/task_5_top_nord_cities.json')
    count_cities = get_count_cities(db)
    whrite_json(count_cities, 'task_5/task_5_count_cities.json')

    update_phone_number(db, 99999, 'Minsk')
    delete_cities_by_name(db, 'Gardez')

    db.close()

if __name__ == "__main__":
    main()


