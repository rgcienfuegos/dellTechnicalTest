import psycopg2
import requests
import pandas as pd
from io import StringIO

# Function to connect to the PostgreSQL database


def connect_to_database():
    conn = psycopg2.connect(
        database="dellTest",
        user="postgres",
        password="admin",
        host="localhost",
        port="5432"
    )
    return conn

# Function to create the 'covid_data' table in the database


def create_table(conn):
    cursor = conn.cursor()
    create_table_query = '''
        CREATE TABLE IF NOT EXISTS covid_data (
            country text,
            country_code VARCHAR(3),
            continent VARCHAR(255),
            population INT,
            indicator VARCHAR(255),
            year_week VARCHAR(10),
            source VARCHAR(255),
            note VARCHAR(255),
            weekly_count INT,
            rate_14_day NUMERIC,
            cumulative_count INT
        );
    '''
    cursor.execute(create_table_query)
    conn.commit()
    cursor.close()  # Close the cursor

# Function to create the 'countries_data' table in the database


def create_countries_data_table(conn):
    cursor = conn.cursor()
    create_table_query = '''
        CREATE TABLE IF NOT EXISTS countries_data (
            Country text,
            Region text,
            Population bigint,
            Area_sq_mi bigint,
            Pop_Density_per_sq_mi numeric,
            Coastline_ratio numeric,
            Net_migration numeric,
            Infant_mortality numeric,
            GDP_per_capita numeric,
            Literacy numeric,
            Phones_per_1000 numeric,
            Arable numeric,
            Crops numeric,
            Other numeric,
            Climate numeric,
            Birthrate numeric,
            Deathrate numeric,
            Agriculture numeric,
            Industry numeric,
            Service numeric
        );
    '''
    cursor.execute(create_table_query)
    conn.commit()
    cursor.close()

# Function to check if a record already exists in the database


def record_exists(conn, year_week, country):
    cursor = conn.cursor()
    select_query = '''
        SELECT COUNT(*) FROM covid_data
        WHERE year_week = %s AND country = %s;
    '''
    cursor.execute(select_query, (year_week, country))
    result = cursor.fetchone()
    cursor.close()  # Cerrar el cursor
    return result[0] > 0

# Function to insert data into the 'covid_data' table


def insert_data(conn, data):
    cursor = conn.cursor()
    insert_query = '''
        INSERT INTO covid_data (country, country_code, continent, population, indicator, year_week, source, note, weekly_count, rate_14_day, cumulative_count)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
    '''

    for row in data:
        cursor.execute(insert_query, (
            row['country'],
            row.get('country_code', None),
            row['continent'],
            row['population'],
            row['indicator'],
            row['year_week'],
            row['source'],
            row['note'],
            row.get('weekly_count', None),
            row.get('rate_14_day', None),
            row.get('cumulative_count', None)
        ))

    conn.commit()
    cursor.close()  # Cerrar el cursor

# Function to insert data into the 'countries_data' table


def load_data_to_postgresql(conn, data):
    try:
        cursor = conn.cursor()

        # Consulta para obtener los países existentes en la base de datos
        cursor.execute("SELECT country FROM countries_data")
        existing_countries = set([row[0] for row in cursor.fetchall()])

        # Inserta los datos en la tabla PostgreSQL
        for _, row in data.iterrows():
            country = row['Country']
            if country not in existing_countries:
                cursor.execute(
                    "INSERT INTO countries_data VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
                    tuple(row)
                )
        # Commit and close the connection
        conn.commit()
        cursor.close()
    except Exception as e:
        print(f"Error al cargar los datos en PostgreSQL: {str(e)}")

# Function to fetch JSON data from a URL


def get_json_data(url):
    response = requests.get(url)
    if response.status_code == 200:
        return response.json()
    else:
        raise Exception("Error al obtener datos JSON desde la URL")

# Function to fetch CSV data from a URL


def get_csv_data(url):
    response = requests.get(url)
    if response.status_code == 200:
        return StringIO(response.text)
    else:
        raise Exception("Error al descargar el archivo CSV desde la URL")


# Function to convert commas to points in decimal numeric values


def convert_commas_to_points(value):
    if isinstance(value, str):
        return value.replace(',', '.')
    return value

# Function to close the database connection


def close_connection(conn):
    conn.close()


# URL of the dataset
data_url = "https://opendata.ecdc.europa.eu/covid19/nationalcasedeath/json/"
# csv_url = "https://storage.googleapis.com/kagglesdsdata/datasets/23752/30346/countries%20of%20the%20world.csv?X-Goog-Algorithm=GOOG4-RSA-SHA256&X-Goog-Credential=gcp-kaggle-com%40kaggle-161607.iam.gserviceaccount.com%2F20230924%2Fauto%2Fstorage%2Fgoog4_request&X-Goog-Date=20230924T022457Z&X-Goog-Expires=259200&X-Goog-SignedHeaders=host&X-Goog-Signature=12455b306dcaca8a4332a2dc9dd1d02e7ef3334f10421922da0017a37d4548ce95b91d335c423493f07632c1152b7844abf78372a4b05e56e7c8410c34768939e19baa2eaf982ad9644ce31177843b1205f2bccfb94865490bdda6e2c3011b844df01806c357f61394d9a24d2a48813ef1ba6d180488211d0bf78f63ff287cf1e9400972a0c38764d466360f2e7426cfb6b6562ace46b147dd894e62b304d205290a7cebd252cb6fd3ebb1608a922586980098803a4972cf90d5e8eef5b6d50cb79dc1402687844214cd2a226d686568ef3732b07e3f1464e07935573194c5d85bed9a214d826fe4653f32d6a141d7f69a0882d8247c2aeeb0ed54bbff30cd95"
csv_data = "countriesoftheworld.csv"

# Database Connection
conn = connect_to_database()

# Create 'data_covid' and 'countries_data' tables
create_table(conn)
create_countries_data_table(conn)

# Get JSON data from the URL
json_data = get_json_data(data_url)
# csv_data = get_csv_data(csv_url)

# Insert CSV Countries into the database
df = pd.read_csv(csv_data)

# Apply the conversion function to relevant columns
columns_to_convert = ["Pop. Density (per sq. mi.)", "Coastline (coast/area ratio)", "Net migration",
                      "Infant mortality (per 1000 births)", "Literacy (%)", "Phones (per 1000)",
                      "Arable (%)", "Crops (%)", "Other (%)", "Birthrate", "Deathrate",
                      "Agriculture", "Industry", "Service", "Climate"]
for column in columns_to_convert:
    df[column] = df[column].apply(convert_commas_to_points)

# Load data into the database
load_data_to_postgresql(conn, df)

# Insert only records that do not exist in the database
for row in json_data:
    if not record_exists(conn, row['year_week'], row['country']):
        insert_data(conn, [row])

# Close the connection
close_connection(conn)
