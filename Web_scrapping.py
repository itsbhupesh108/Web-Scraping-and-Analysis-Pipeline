import requests
from bs4 import BeautifulSoup
import pandas as pd
import pymysql


db_connection = None
cursor = None

try:
    db_connection = pymysql.connect(
        host="localhost",
        user="root",
        password="Bhupesh108",
        database="covid19"
    )
    cursor = db_connection.cursor()
    print("Database connection established successfully")

    print("Creating table covid_data_1 ")
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS covid_data_1 (
            id INT AUTO_INCREMENT PRIMARY KEY,
            country VARCHAR(100),
            total_cases INT,
            total_deaths INT,
            total_recovered INT,
            active_cases INT
        )
    """)


    def scrape_covid_data():
        print("Starting web scraping...")
        url = "https://www.worldometers.info/coronavirus/"
        response = requests.get(url)

        if response.status_code != 200:
            print(f"Failed to retrieve data: {response.status_code}")
            return [], []   #Return empty data and headers if the request fails

        print("Webpage retrieved successfully, Parsing")
        soup = BeautifulSoup(response.content, "html.parser")
        table = soup.find("table", id="main_table_countries_today")

        if not table:
            print("Failed to find the table.")
            return [], []

        
        headers = [header.text.strip() for header in table.find_all("th")] #Extract headers
        data = []

        print("Extracting table data...")
        for row in table.find_all("tr")[1:]:
            cells = row.find_all("td")
            if len(cells) > 1:
                country_data = [cell.text.strip().replace(",", "") for cell in cells]
                data.append(country_data)

        print(f"Scraped {len(data)} rows of data.")
        print("Data sample:", data[:5])
        return data, headers 

    data, headers = scrape_covid_data()  # Get both data and headers

    
    if data:
        print("Converting scraped data to DataFrame...")
        covid_df = pd.DataFrame(data)

        # Rename the columns
        covid_df.columns = ['Index', 'Country', 'Total_Cases', 'New_Cases', 'Total_Deaths', 'New_Deaths',
                            'Total_Recovered', 'New_Recovered', 'Active_Cases', 'Serious_Cases',
                            'Tot_Cases/1M', 'Deaths/1M', 'Total_Test', 'Tests/1M', 'Population',
                            'Continent', 'Active_Cases/1M', 'New_Cases/1M', 'New_Deaths/1M',
                            'New_Recovered/1M', 'Critical_Cases', 'Cases_Per_Million']

        # Drop unnecessary columns
        print("Cleaning DataFrame...")
        covid_df = covid_df[['Country', 'Total_Cases', 'Total_Deaths', 'Total_Recovered', 'Active_Cases']]

        print("Converting columns to numeric types")
        try:
            covid_df['Total_Cases'] = pd.to_numeric(covid_df['Total_Cases'], errors='coerce')
            covid_df['Total_Deaths'] = pd.to_numeric(covid_df['Total_Deaths'], errors='coerce')
            covid_df['Total_Recovered'] = pd.to_numeric(covid_df['Total_Recovered'], errors='coerce')
            covid_df['Active_Cases'] = pd.to_numeric(covid_df['Active_Cases'], errors='coerce')
        except Exception as e:
            print(f"Error converting data to numeric: {e}")

        print(covid_df.head())

        def insert_data_into_db(covid_df):
            print("Inserting data into the database...")
            for _, row in covid_df.iterrows():
                try:
                    cursor.execute("""
                        INSERT INTO covid_data_1 (country, total_cases, total_deaths, total_recovered, active_cases)
                        VALUES (%s, %s, %s, %s, %s)
                    """, (
                        row['Country'],
                        int(row['Total_Cases']) if pd.notnull(row['Total_Cases']) else None,
                        int(row['Total_Deaths']) if pd.notnull(row['Total_Deaths']) else None,
                        int(row['Total_Recovered']) if pd.notnull(row['Total_Recovered']) else None,
                        int(row['Active_Cases']) if pd.notnull(row['Active_Cases']) else None
                    ))
                except Exception as e:
                    print(f"Error inserting row into database: {e}")

            db_connection.commit()
            print("COVID-19 data inserted into MySQL database.")


        insert_data_into_db(covid_df)

    else:
        print("No data to insert into the database.")

except mysql.connector.Error as err:
    print(f"Database error: {err}")
except Exception as e:
    print(f"Unexpected error: {e}")
finally:

    print("Closing database connection...")     # Close Database Connection
    if cursor:
        cursor.close()
    if db_connection:
        db_connection.close()
    print("Database connection closed.")
