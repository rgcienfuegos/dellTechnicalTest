import pandas as pd
import matplotlib.pyplot as plt
from sqlalchemy import create_engine

# Database connection parameters
db_params = {
    'database': 'dellTest',
    'user': 'postgres',
    'password': 'admin',
    'host': 'localhost',
    'port': '5432'
}

# Create a database connection using SQLAlchemy
engine = create_engine(
    f'postgresql://{db_params["user"]}:{db_params["password"]}@{db_params["host"]}:{db_params["port"]}/{db_params["database"]}')

try:
    # Query to retrieve data
    query = """
   SELECT
    TRIM(c.country) AS country,
    c.gdp_per_capita,
    cd.cumulative_count AS covid_cases,
    hi.value AS icu_admissions,
    (hi.value / cd.cumulative_count) AS icu_admissions_ratio,
    TO_CHAR(TO_DATE(hi.year_week, 'IYYY-"W"IW'), 'YYYY-IW') AS formatted_year_week
FROM
    countries_data c
INNER JOIN
    covid_data cd ON TRIM(c.country) = TRIM(cd.country)
INNER JOIN
    hospital_icu_admission hi ON TRIM(c.country) = TRIM(hi.country) AND cd.year_week = TO_CHAR(TO_DATE(hi.year_week, 'IYYY-"W"IW'), 'YYYY-IW')
WHERE
    hi.indicator = 'Daily ICU occupancy';
    """

    # Fetch data into a Pandas DataFrame
    data = pd.read_sql(query, engine)
    # Create the scatter plots
    plt.figure(figsize=(15, 5))

    # GDP per Capita vs. COVID-19 Cases
    plt.subplot(131)
    plt.scatter(data['gdp_per_capita'], data['covid_cases'], alpha=0.5)
    plt.title('GDP per Capita vs. COVID-19 Cases')
    plt.xlabel('GDP per Capita')
    plt.ylabel('COVID-19 Cases')

    # GDP per Capita vs. ICU Admissions
    plt.subplot(132)
    plt.scatter(data['gdp_per_capita'], data['icu_admissions'], alpha=0.5)
    plt.title('GDP per Capita vs. ICU Admissions')
    plt.xlabel('GDP per Capita')
    plt.ylabel('ICU Admissions')

    # COVID-19 Cases vs. ICU Admissions
    plt.subplot(133)
    plt.scatter(data['covid_cases'], data['icu_admissions'], alpha=0.5)
    plt.title('COVID-19 Cases vs. ICU Admissions')
    plt.xlabel('COVID-19 Cases')
    plt.ylabel('ICU Admissions')

    # Additional Graph: Week with Most COVID-19 Cases per Country
    plt.figure(figsize=(10, 5))
    week_most_cases = data.groupby('country')['covid_cases'].idxmax()
    most_cases_data = data.loc[week_most_cases]
    plt.bar(most_cases_data['country'], most_cases_data['covid_cases'])
    plt.title('Week with Most COVID-19 Cases per Country')
    plt.xlabel('Country')
    plt.ylabel('COVID-19 Cases')
    plt.xticks(rotation=90)
    plt.tight_layout()

    # Additional Graph: Week with Most ICU Admissions per Country
    plt.figure(figsize=(10, 5))
    week_most_icu = data.groupby('country')['icu_admissions'].idxmax()
    most_icu_data = data.loc[week_most_icu]
    plt.bar(most_icu_data['country'], most_icu_data['icu_admissions'])
    plt.title('Week with Most ICU Admissions per Country')
    plt.xlabel('Country')
    plt.ylabel('ICU Admissions')
    plt.xticks(rotation=90)
    plt.tight_layout()

    plt.xticks(rotation=90)  # Rotate x-axis labels for better readability
    plt.tight_layout()
    plt.show()

except Exception as e:
    print(f"Error: {e}")

finally:
    # Close the SQLAlchemy engine
    engine.dispose()
