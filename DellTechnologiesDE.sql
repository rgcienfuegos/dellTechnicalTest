
-- View: public.combined_data

-- DROP VIEW public.combined_data;

CREATE OR REPLACE VIEW public.combined_data
 AS
 SELECT cd.country,
    cd.region,
    cd.population,
    cd.area_sq_mi,
    cd.pop_density_per_sq_mi,
    cd.coastline_ratio,
    cd.net_migration,
    cd.infant_mortality,
    cd.gdp_per_capita,
    cd.literacy,
    cd.phones_per_1000,
    cd.arable,
    cd.crops,
    cd.other,
    cd.climate,
    cd.birthrate,
    cd.deathrate,
    cd.agriculture,
    cd.industry,
    cd.service,
    covid.weekly_count AS latest_number_of_cases,
    covid.rate_14_day AS "Cumulative_number_for_14_days_of_COVID-19_cases_per_100000",
    covid.year_week AS date_information_extracted
   FROM countries_data cd
     JOIN covid_data covid ON TRIM(BOTH FROM cd.country) = TRIM(BOTH FROM covid.country);

ALTER TABLE public.combined_data
    OWNER TO postgres;



-- 1- What is the country with the highest number of Covid-19 cases per 100 000 Habitants at 31/07/2020?



SELECT
    covid_data.country,
    (cumulative_count / countries_data.population::numeric) * 100000 AS cases_per_100000
FROM covid_data
JOIN countries_data ON TRIM(BOTH FROM covid_data.country) = TRIM(BOTH FROM countries_data.country)
WHERE EXTRACT('year' FROM TO_DATE("year_week", 'IYYY-IW')) = EXTRACT('year' FROM TO_DATE('31-07-2020', 'DD-MM-YYYY'))
   AND EXTRACT('week' FROM TO_DATE("year_week", 'IYYY-IW')) = EXTRACT('week' FROM TO_DATE('31-07-2020', 'DD-MM-YYYY'))
ORDER BY cases_per_100000 DESC
LIMIT 1;

-- 2-What is the top 10 countries with the lowest number of Covid-19 cases per 100 000 Habitants at 31/07/2020?
-- Comentario: Selecciona los 10 países con el menor número de casos por 100,000 habitantes el 31/07/2020

SELECT
    covid_data.country,
    (cumulative_count / countries_data.population::numeric) * 100000 AS cases_per_100000
FROM covid_data
JOIN countries_data ON TRIM(BOTH FROM covid_data.country) = TRIM(BOTH FROM countries_data.country)
WHERE EXTRACT('year' FROM TO_DATE("year_week", 'IYYY-IW')) = EXTRACT('year' FROM TO_DATE('31-07-2020', 'DD-MM-YYYY'))
   AND EXTRACT('week' FROM TO_DATE("year_week", 'IYYY-IW')) = EXTRACT('week' FROM TO_DATE('31-07-2020', 'DD-MM-YYYY'))
ORDER BY cases_per_100000 ASC
LIMIT 10;

-- 3-What is the top 10 countries with the highest number of cases among the top 20 richest countries (by GDP per capita)?

-- WorldWide Version

WITH Top20RichestCountries AS (
    SELECT country
    FROM countries_data
    ORDER BY gdp_per_capita DESC
    LIMIT 20
)

SELECT
    covid.country,
    covid.cumulative_count AS latest_cases
FROM covid_data covid
JOIN (
    SELECT
        country,
        MAX(year_week) AS latest_date
    FROM covid_data
	 WHERE cumulative_count IS NOT NULL
    GROUP BY country
) latest_dates ON covid.country = latest_dates.country AND covid.year_week = latest_dates.latest_date
JOIN Top20RichestCountries richest ON TRIM(BOTH FROM covid.country) = TRIM(BOTH FROM richest.country)
ORDER BY latest_cases DESC
LIMIT 10;




-- EU Version

WITH Top20RichestCountries AS (
    SELECT DISTINCT country,gdp_per_capita
    FROM combined_data
    ORDER BY gdp_per_capita DESC
    LIMIT 20
)

SELECT
    covid.country,
    covid.cumulative_count AS latest_cases
FROM covid_data covid
JOIN (
    SELECT
        country,
        MAX(year_week) AS latest_date
    FROM covid_data
    WHERE cumulative_count IS NOT NULL
    GROUP BY country
) latest_dates ON covid.country = latest_dates.country AND covid.year_week = latest_dates.latest_date
JOIN Top20RichestCountries richest ON TRIM(BOTH FROM covid.country) = TRIM(BOTH FROM richest.country)
ORDER BY latest_cases DESC
LIMIT 10;

--4- List all the regions with the number of cases per million of inhabitants and display information on population density, for 31/07/2020.
 
SELECT
    cd.region,
    SUM(covid.cumulative_count) / (SUM(cd.population)::numeric / 1000000) AS cases_per_million,
    MAX(cd.pop_density_per_sq_mi) AS population_density
FROM covid_data covid
JOIN countries_data cd ON TRIM(BOTH FROM covid.country) = TRIM(BOTH FROM cd.country)
WHERE EXTRACT('year' FROM TO_DATE("year_week", 'IYYY-IW')) = EXTRACT('year' FROM TO_DATE('31-07-2020', 'DD-MM-YYYY'))
   AND EXTRACT('week' FROM TO_DATE("year_week", 'IYYY-IW')) = EXTRACT('week' FROM TO_DATE('31-07-2020', 'DD-MM-YYYY'))
GROUP BY cd.region
ORDER BY cases_per_million DESC;
	
--5 

-- Find duplicated records in the combined_data view

--countries_data
SELECT country, region, COUNT(*)
FROM countries_data
GROUP BY country, region
HAVING COUNT(*) > 1;


--covid_data
SELECT country, year_week, COUNT(*)
FROM covid_data
GROUP BY country, year_week
HAVING COUNT(*) > 1;


--combined data
SELECT
    "country",
    "date_information_extracted",
    COUNT(*) AS "duplicate_count"
FROM
    combined_data
GROUP BY
    "country",
    "date_information_extracted"
HAVING
    COUNT(*) > 1
ORDER BY
    "duplicate_count" DESC;

