import requests
import psycopg2


db_params = {
    "host": "localhost",
    "database": "dellTest",
    "user": "postgres",
    "password": "admin",
}


# URL of JSON file
json_url = "https://opendata.ecdc.europa.eu/covid19/hospitalicuadmissionrates/json/"


def connect_to_database():
    return psycopg2.connect(**db_params)


def create_table(conn):
    cursor = conn.cursor()
    create_table_query = '''
        CREATE TABLE IF NOT EXISTS hospital_icu_admission (
            country text,
            indicator text,
            date date,
            year_week text,
            value numeric,
            source text,
            url text,
            CONSTRAINT pk_hospital_icu_admission PRIMARY KEY (country, indicator, year_week)
        );
    '''
    cursor.execute(create_table_query)
    conn.commit()
    cursor.close()


def download_json_data(url):
    response = requests.get(url)
    if response.status_code == 200:
        return response.json()
    else:
        raise Exception("Error al obtener datos JSON desde la URL")


def insert_data(conn, data):
    cursor = conn.cursor()
    insert_query = '''
        INSERT INTO hospital_icu_admission (country, indicator, date, year_week, value, source, url)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (country, indicator, year_week) DO NOTHING;
    '''

    for record in data:
        cursor.execute(insert_query, (
            record['country'],
            record['indicator'],
            record['date'],
            record['year_week'],
            record['value'],
            record['source'],
            record.get('url', None)
        ))

    conn.commit()
    cursor.close()


def main():
    conn = connect_to_database()
    create_table(conn)
    json_data = download_json_data(json_url)
    insert_data(conn, json_data)
    conn.close()
    print("Proceso completado.")


if __name__ == "__main__":
    main()
