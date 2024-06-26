import requests
from bs4 import BeautifulSoup
import psycopg2
from psycopg2 import sql, OperationalError
import csv


url = 'https://y20india.in/literacy-rates-in-india/'

def scrape_table(url):
    try:
        response = requests.get(url)
        response.raise_for_status()
    except requests.RequestException as e:
        raise Exception(f"Failed to load page {url}: {e}")

    soup = BeautifulSoup(response.text, 'html.parser')

    table = soup.find('table')
    if not table:
        raise Exception("No table found")

    headers = [th.text.strip() for th in table.find('tr').find_all('th')]

    rows = []
    for tr in table.find_all('tr')[1:]:
        cells = [td.text.strip() for td in tr.find_all('td')]
        rows.append(cells)

    return headers, rows

def store_data_in_postgresql(headers, rows):
    try:
        con = psycopg2.connect(
            dbname='scraping_db',
            user='postgres',
            password='password',
            host='127.0.0.1',
            port='5432'
        )
    except OperationalError as e:
        raise Exception(f"Failed to connect to the database: {e}")

    cursor = con.cursor()

    try:
        create_table_query = sql.SQL('''
            CREATE TABLE IF NOT EXISTS scraped_data (
                {}
            )
        ''').format(
            sql.SQL(', ').join(
                sql.Identifier(header) + sql.SQL(' TEXT') for header in headers
            )
        )
        cursor.execute(create_table_query)
        con.commit()

        insert_query = sql.SQL('''
            INSERT INTO scraped_data ({})
            VALUES ({})
        ''').format(
            sql.SQL(', ').join(map(sql.Identifier, headers)),
            sql.SQL(', ').join(sql.Placeholder() * len(headers))
        )

        for row in rows:
            cursor.execute(insert_query, row)

        con.commit()
    except Exception as e:
        con.rollback()
        raise Exception(f"Failed to store data: {e}")
    finally:
        cursor.close()
        con.close()

hostname = "localhost"
database = "scraping_db"
username = "postgres"
pwd = "password"
port_id = 5432

def connect():
    try:
        connection = psycopg2.connect(
            host=hostname,
            dbname=database,
            user=username,
            password=pwd,
            port=port_id
        )
        return connection
    except Exception as error:
        print("Error connecting to database:", error)
        return None

def create_cursor(connection):
    if connection:
        try:
            return connection.cursor()
        except Exception as error:
            print("Error creating cursor:", error)
            return None
    else:
        print("Connection not established. Cannot create cursor.")
        return None

def close_connection(connection):
    if connection:
        try:
            connection.close()
        except Exception as error:
            print("Error closing database connection:", error)

def insert(data):
    connection = connect()
    cursor = create_cursor(connection)

    if connection and cursor:
        try:
            check_query = 'SELECT * FROM scraped_data WHERE "State/UT" = %s'
            cursor.execute(check_query, (data[1],))
            existing_record = cursor.fetchone()

            if not existing_record:
                insert_data = '''INSERT INTO scraped_data 
                    ("State/UT", "Census 2011 Average", "Census 2011 Male" , "Census 2011 Female",
                                  "NSO Survey 2017 Average" , "NSO Survey 2017 Male" , "NSO Survey 2017 Female"  
                            )
                    VALUES (%s, %s, %s, %s, %s, %s, %s)'''
                insert_values = data
                cursor.execute(insert_data, insert_values)
                connection.commit()
                print("Data added successfully")
            else:
                print(f"Record with State '{data[1]}' already exists. Skipping insertion.")

        except Exception as error:
            print("Got an Error:", error)
        finally:
            close_connection(connection)

def select_all():
    connection = connect()
    cursor = create_cursor(connection)

    if connection and cursor:
        try:
            select_query = "SELECT * FROM scraped_data"
            cursor.execute(select_query)
            rows = cursor.fetchall()

            if rows:
                for row in rows:
                    print(row)
            else:
                print("No data found in scraped_data table.")

        except Exception as error:
            print("Got an Error:", error)
        finally:
            close_connection(connection)

def update(data):
    connection = connect()
    cursor = create_cursor(connection)

    if connection and cursor:
        try:
            update_query = '''UPDATE scraped_data
                              SET "Census 2011 Average" = %s, "Census 2011 Male" = %s, "Census 2011 Female" = %s,
                                  "NSO Survey 2017 Average" = %s, "NSO Survey 2017 Male" = %s, "NSO Survey 2017 Female" = %s 
                              WHERE "State/UT" = %s'''
            update_values = data[1:] + [data[0]]  
            print("Executing update query:", update_query)
            print("Update values:", update_values)
            cursor.execute(update_query, update_values)
            connection.commit()
            print("Data updated successfully")

        except Exception as error:
            print("Error updating data:", error)
        finally:
            close_connection(connection)

def delete(state):
    connection = connect()
    cursor = create_cursor(connection)

    if connection and cursor:
        try:
            delete_data = 'DELETE FROM scraped_data WHERE "State/UT" = %s'
            cursor.execute(delete_data, (state,))
            connection.commit()
            print("Data deleted successfully")

        except Exception as error:
            print("Got an Error:", error)
        finally:
            close_connection(connection)
#new_data = ["HELLO", "120", "20", "130", "110", "125", "115"]
#insert(new_data)

#select_all()

def export_to_csv(file_path):
    connection = connect()
    cursor = create_cursor(connection)

    if connection and cursor:
        try:
            select_query = "SELECT * FROM scraped_data"
            cursor.execute(select_query)
            rows = cursor.fetchall()

            if not rows:
                print("No data found in scraped_data table.")
                return

            cursor.execute('SELECT column_name FROM information_schema.columns WHERE table_name = %s', ('scraped_data',))
            headers = [row[0] for row in cursor.fetchall()]

            with open(file_path, 'w', newline='') as csvfile:
                csvwriter = csv.writer(csvfile)
                csvwriter.writerow(headers)
                csvwriter.writerows(rows)
            print(f"Data exported successfully to {file_path}")

        except Exception as e:
            print(f"Failed to export data: {e}")
        finally:
            close_connection(connection)
    else:
        print("Failed to establish a connection to the database.")

if __name__ == '__main__':
    try:
        headers, rows = scrape_table(url)
        store_data_in_postgresql(headers, rows)
        export_to_csv('scraped_data.csv')
    except Exception as e:
        print(f"An error occurred: {e}")


#select_all()
updated_data = ["HELLO", "135", "25", "140", "122", "130", "120"]
update(updated_data)