/*
Covid-19 Data Exploration Analysis (by Postgre SQL)

<Skills used> 
Joins, CTE's, Temp Tables, Windows Functions, Aggregate Functions, 
Creating Views, Creating Table, Converting Data Types

<Data Source>
https://covid.ourworldindata.org/data/owid-covid-data.csv

*/


--Using  Aggregate Functions to track covid-19 across countries (Inflection Rate/Death Rate/ Number Of Death) 

SELECT location, 
	   continent, 
	   population, 
       SUM(COALESCE(new_cases,0)) AS total_cases, 
	   SUM(COALESCE(new_deaths,0)) AS total_deaths, 
	   ROUND(SUM(COALESCE(new_cases,0))/population*100,2) AS Infection_rate,
	   ROUND(COALESCE(SUM(new_deaths)/SUM(new_cases)*100,0),2) AS death_rate 
FROM covid_death
WHERE continent IS NOT NULL 
GROUP BY 1,2,3
ORDER BY 5 DESC;

--Using Aggregate Functions to track covid-19 across continents (Inflection Rate/Death Rate/ Number Of Death)

SELECT location, 
	   population, 
       SUM(COALESCE(new_cases,0)) AS total_cases, 
	   SUM(COALESCE(new_deaths,0)) AS total_deaths, 
	   ROUND(SUM(COALESCE(new_cases,0))/population*100,2) AS Infection_rate,
	   ROUND(COALESCE(SUM(new_deaths)/SUM(new_cases)*100,0),2) AS death_rate 
FROM covid_death
WHERE continent IS NULL 
GROUP BY 1,2
ORDER BY 4 DESC;

--Using JOIN & Window Function to track covid-19 (Accumulated Vaccinations) across countries

SELECT C.location,
	   C.date, 
	   C.population AS country_population,
	   COALESCE(V.new_vaccinations,0) AS new_vaccinations,
	   SUM(COALESCE(V.new_vaccinations,0)) OVER(PARTITION BY C.location ORDER BY C.location, C.date) AS acc_vaccinations
FROM covid_death C
JOIN covid_vaccine V
ON C.location=V.location AND C.date=V.date
WHERE C.continent IS NOT NULL AND;


--Using Temp Table to track covid-19 (Vaccination Rate) across countries

SELECT *, ROUND(acc_vaccinations/population*100,2) AS acc_vaccinated_rate
FROM 
	(SELECT C.location,
		    C.date, 
	        C.population AS country_population,
		    COALESCE(V.new_vaccinations,0) AS new_vaccinations,
		    SUM(COALESCE(V.new_vaccinations,0)) OVER(PARTITION BY C.location ORDER BY C.location, C.date) AS acc_vaccinations
	FROM covid_death C
	JOIN covid_vaccine V
	ON C.location=V.location AND C.date=V.date
	WHERE C.continent IS NOT NULL) Temp
WHERE acc_vaccinations!=0;

--Using CTE to track covid-19 (Vaccination Rate) across countries

with VaccinationStat(location, date, country_population, vew_vaccinations, acc_vaccinations)
as (SELECT C.location,
		    C.date, 
	        C.population AS country_population,
		    COALESCE(V.new_vaccinations,0) AS new_vaccinations,
		    SUM(COALESCE(V.new_vaccinations,0)) OVER(PARTITION BY C.location ORDER BY C.location, C.date) AS acc_vaccinations
	FROM covid_death C
	JOIN covid_vaccine V
	ON C.location=V.location AND C.date=V.date
	WHERE C.continent IS NOT NULL
)
SELECT *, ROUND(acc_vaccinations/country_population*100,2) AS acc_vaccinated_rate
FROM VaccinationStat
WHERE acc_vaccinations!=0;

--Create View for Tracking Coronavirus Vaccinations Around the World

Create view TrackVaccination as 
SELECT C.location,
		    C.date, 
	        C.population AS country_population,
		    COALESCE(V.new_vaccinations,0) AS new_vaccinations,
		    SUM(COALESCE(V.new_vaccinations,0)) OVER(PARTITION BY C.location ORDER BY C.location, C.date) AS acc_vaccinations
	FROM covid_death C
	JOIN covid_vaccine V
	ON C.location=V.location AND C.date=V.date
	WHERE C.continent IS NOT NULL;

--Create Table for Tracking Coronavirus Vaccinations Around the World

Create Table if not exists VaccinationTable(
	location VARCHAR(50),
	date Date,
	country_population int,
	daily_vaccinations int,
	acc_vaccinations int
);

INSERT INTO VaccinationTable(
	SELECT C.location,
		    CAST(C.date as Date), 
	        C.population AS country_population,
		    COALESCE(V.new_vaccinations,0) AS new_vaccinations,
		    SUM(COALESCE(V.new_vaccinations,0)) OVER(PARTITION BY C.location ORDER BY C.location, C.date) AS acc_vaccinations
	FROM covid_death C
	JOIN covid_vaccine V
	ON C.location=V.location AND C.date=V.date
	WHERE C.continent IS NOT NULL
);


COPY VaccinationTable TO 'C:\temp\vaccinationtable.csv' DELIMITER ',' CSV HEADER;

























