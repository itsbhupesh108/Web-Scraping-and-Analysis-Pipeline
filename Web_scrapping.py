import requests
from bs4 import BeautifulSoup
import pandas as pd
import mysql.connector

# Database Connection Parameters
db_connection = None
cursor = None

try:
    # Establish Database Connection
    db_connection = mysql.connector.connect(
        host="localhost",
        user="root",  # Replace with your username
        password="Bhupesh108",  # Replace with your password
        database="Covid19"  # Replace with your database name
    )
    cursor = db_connection.cursor()

    # Step 1: Create Table in MySQL (if it doesn’t already exist)
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
    print("Table check/creation complete.")


    # Step 2: Define Function to Scrape Data from Worldometer
    def scrape_covid_data():
        url = "https://www.worldometers.info/coronavirus/"
        response = requests.get(url)

        # Check if the request was successful
        if response.status_code != 200:
            print(f"Failed to retrieve data: {response.status_code}")
            return [], []  # Return empty data and headers if the request fails

        soup = BeautifulSoup(response.content, "html.parser")
        table = soup.find("table", id="main_table_countries_today")

        if not table:
            print("Failed to find the table.")
            return [], []  # Return empty data and headers if the table is not found

        # Extract headers
        headers = [header.text.strip() for header in table.find_all("th")]
        data = []

        for row in table.find_all("tr")[1:]:
            cells = row.find_all("td")
            if len(cells) > 1:
                country_data = [cell.text.strip().replace(",", "") for cell in cells]
                data.append(country_data)

        print("Data sample:", data[:5])  # Print the first 5 rows of scraped data
        print(f"Scraped {len(data)} rows of data.")

        return data, headers  # Return both data and headers


    # Step 3: Call the Scraping Function
    data, headers = scrape_covid_data()  # Get both data and headers

    # Step 4: Convert to DataFrame and Clean the Data
    if data:
        # Convert to DataFrame with relevant columns only
        covid_df = pd.DataFrame(data)

        # Rename the columns based on your understanding of the data
        covid_df.columns = ['Index', 'Country', 'Total_Cases', 'New_Cases', 'Total_Deaths', 'New_Deaths',
                            'Total_Recovered', 'New_Recovered', 'Active_Cases', 'Serious_Cases',
                            'Tot_Cases/1M', 'Deaths/1M', 'Total_Test', 'Tests/1M', 'Population',
                            'Continent', 'Active_Cases/1M', 'New_Cases/1M', 'New_Deaths/1M',
                            'New_Recovered/1M', 'Critical_Cases', 'Cases_Per_Million']

        # Drop unnecessary columns (like the index or empty columns)
        covid_df = covid_df[['Country', 'Total_Cases', 'Total_Deaths', 'Total_Recovered', 'Active_Cases']]

        # Convert the relevant columns to numeric types
        covid_df['Total_Cases'] = pd.to_numeric(covid_df['Total_Cases'], errors='coerce')
        covid_df['Total_Deaths'] = pd.to_numeric(covid_df['Total_Deaths'], errors='coerce')
        covid_df['Total_Recovered'] = pd.to_numeric(covid_df['Total_Recovered'], errors='coerce')
        covid_df['Active_Cases'] = pd.to_numeric(covid_df['Active_Cases'], errors='coerce')

        # Display the cleaned DataFrame
        print(covid_df.head())


        # Step 5: Insert Data into MySQL Database
        def insert_data_into_db(covid_df):
            for _, row in covid_df.iterrows():
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

            # Commit the transaction
            db_connection.commit()
            print("COVID-19 data inserted into MySQL database.")


        # Insert the cleaned data into the database
        insert_data_into_db(covid_df)

    else:
        print("No data to insert into the database.")

except mysql.connector.Error as err:
    print(f"Database error: {err}")
except Exception as e:
    print(f"Error: {e}")
finally:
    # Close Database Connection
    if cursor:
        cursor.close()
    if db_connection:
        db_connection.close()
    print("Database connection closed.")
