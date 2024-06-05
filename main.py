import requests
from bs4 import BeautifulSoup
import psycopg2
from psycopg2 import sql, OperationalError
import csv
import os
import psycopg2
from psycopg2 import sql

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
        conn = psycopg2.connect(
            dbname='scraping_db',
            user='postgres',
            password='password',
            host='127.0.0.1',
            port='5432'
        )
    except OperationalError as e:
        raise Exception(f"Failed to connect to the database: {e}")

    cursor = conn.cursor()

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
        conn.commit()

        insert_query = sql.SQL('''
            INSERT INTO scraped_data ({})
            VALUES ({})
        ''').format(
            sql.SQL(', ').join(map(sql.Identifier, headers)),
            sql.SQL(', ').join(sql.Placeholder() * len(headers))
        )

        for row in rows:
            cursor.execute(insert_query, row)

        conn.commit()
    except Exception as e:
        conn.rollback()
        raise Exception(f"Failed to store data: {e}")
    finally:
        cursor.close()
        conn.close()



# Database connection details (modify these with your credentials)
hostname = "localhost"
database = "scraping_db"
username = "postgres"
pwd = "password"
port_id = 5432

def connect():
    """
    Connects to the PostgreSQL database.
    Returns a psycopg2 connection object or None on failure.
    """
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
    """
    Creates a cursor object from the provided connection.
    Returns a psycopg2 cursor object or None on failure.
    """
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
    """
    Closes the database connection if it's open.
    """
    if connection:
        try:
            connection.close()
        except Exception as error:
            print("Error closing database connection:", error)

def insert(data):
    """
    Inserts a new record into the 'scraped_data' table.
    Checks for duplicate Index_Name before insertion.

    Args:
        data (list): A list containing values to be inserted in the same
                     order as the table columns.
    """
    connection = connect()
    cursor = create_cursor(connection)

    if connection and cursor:
        try:
            # Check if record exists (prevents duplicate Index_Name)
            check_query = 'SELECT * FROM scraped_data WHERE "State/UT" = %s'
            cursor.execute(check_query, (data[1],))
            existing_record = cursor.fetchone()

            if not existing_record:
                # Insert data if record doesn't exist
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

# def select_all():
#     """
#     Fetches all data from the 'scraped_data' table.
#     Prints retrieved data or a message if no data is found.
#     """
#     connection = connect()
#     cursor = create_cursor(connection)

#     if connection and cursor:
#         try:
#             # Fetch all data from scraped_data table
#             select_query = "SELECT * FROM scraped_data"
#             cursor.execute(select_query)
#             rows = cursor.fetchall()

#             # Print retrieved data
#             if rows:
#                 for row in rows:
#                     print(row)
#             else:
#                 print("No data found in scraped_data table.")

#         except Exception as error:
#             print("Got an Error:", error)
#         finally:
#             close_connection(connection)
def select_all():
    """
    Fetches all data from the 'scraped_data' table.
    Returns the rows of data or an empty list if no data is found.
    """
    connection = connect()
    cursor = create_cursor(connection)
    rows = []

    if connection and cursor:
        try:
            # Fetch all data from scraped_data table
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

    return rows

def update(data):
    """
    Updates existing data based on the provided State.

    Args:
        data (list): A list containing values to be updated in the same
                     order as the table columns. The first element should
                     be the State of the record to update.
    """
    connection = connect()
    cursor = create_cursor(connection)

    if connection and cursor:
        try:
            # Update data based on existing State
            update_query = '''UPDATE scraped_data
                              SET "Census 2011 Average" = %s, "Census 2011 Male" = %s, "Census 2011 Female" = %s,
                                  "NSO Survey 2017 Average" = %s, "NSO Survey 2017 Male" = %s, "NSO Survey 2017 Female" = %s 
                              WHERE "State/UT" = %s'''
            update_values = data[1:] + [data[0]]  # Extract the index and append it at the end for the WHERE clause
            cursor.execute(update_query, update_values)
            connection.commit()
            print("Data updated successfully")

        except Exception as error:
            print("Got an Error:", error)
        finally:
            close_connection(connection)

def delete(state):
    """
    Deletes a record from the 'scraped_data' table based on the provided State.

    Args:
        state (str): The State value of the record to delete.
    """
    connection = connect()
    cursor = create_cursor(connection)

    if connection and cursor:
        try:
            # Delete data based on State
            delete_data = 'DELETE FROM scraped_data WHERE "State/UT" = %s'
            cursor.execute(delete_data, (state,))
            connection.commit()
            print("Data deleted successfully")

        except Exception as error:
            print("Got an Error:", error)
        finally:
            close_connection(connection)

# Example data (replace with your actual data)
new_data = ["HELLO", "120", "20", "130", "110", "125", "115"]
insert(new_data)

# select_all()

# Example data (replace with actual values)
# updated_data = ["India", "135", "25", "140", "122", "130", "120"]
# update(updated_data)
# select_all()

# delete("India")
# select_all()
def export_to_csv(file_path):
    rows = select_all()

    if not rows:
        print("No data to export.")
        return

    try:
        conn = psycopg2.connect(
            dbname='scraping_db',
            user='postgres',
            password='password',
            host='127.0.0.1',
            port='5432'
        )
        cursor = conn.cursor()
        cursor.execute('SELECT column_name FROM information_schema.columns WHERE table_name = %s', ('scraped_data',))
        headers = [row[0] for row in cursor.fetchall()]
    except Exception as e:
        raise Exception(f"Failed to fetch headers: {e}")
    finally:
        cursor.close()
        conn.close()

    try:
        with open(file_path, 'w', newline='') as csvfile:
            csvwriter = csv.writer(csvfile)
            csvwriter.writerow(headers)
            csvwriter.writerows(rows)
    except Exception as e:
        raise Exception(f"Failed to write CSV file: {e}")
# def export_to_csv(file_path):
#     rows = select_all()

#     try:
#         conn = psycopg2.connect(
#             dbname='scraping_db',
#             user='postgres',
#             password='password',
#             host='127.0.0.1',
#             port='5432'
#         )
#         cursor = conn.cursor()
#         cursor.execute('SELECT column_name FROM information_schema.columns WHERE table_name = %s', ('scraped_data',))
#         headers = [row[0] for row in cursor.fetchall()]
#     except Exception as e:
#         raise Exception(f"Failed to fetch headers: {e}")
#     finally:
#         cursor.close()
#         conn.close()

#     try:
#         with open(file_path, 'w', newline='') as csvfile:
#             csvwriter = csv.writer(csvfile)
#             csvwriter.writerow(headers)
#             csvwriter.writerows(rows)
#     except Exception as e:
#         raise Exception(f"Failed to write CSV file: {e}")

if __name__ == '__main__':
    try:
        headers, rows = scrape_table(url)
        store_data_in_postgresql(headers, rows)
        export_to_csv('scraped_data.csv')
    except Exception as e:
        print(f"An error occurred: {e}")

select_all()

# Example data (replace with actual values)
updated_data = ["India", "135", "25", "140", "122", "130", "120"]
update(updated_data)
select_all()

# delete("India")
# select_all()



















# def create_record(headers, values):
#     try:
#         conn = psycopg2.connect(
#             dbname='scraping_db',
#             user='postgres',
#             password='password',
#             host='127.0.0.1',
#             port='5432'
#         )
#         cursor = conn.cursor()

#         insert_query = sql.SQL('''
#             INSERT INTO scraped_data ({})
#             VALUES ({})
#         ''').format(
#             sql.SQL(', ').join(map(sql.Identifier, headers)),
#             sql.SQL(', ').join(sql.Placeholder() * len(headers))
#         )

#         cursor.execute(insert_query, values)
#         conn.commit()
#     except Exception as e:
#         conn.rollback()
#         raise Exception(f"Failed to create record: {e}")
#     finally:
#         cursor.close()
#         conn.close()

# def read_records():
#     try:
#         conn = psycopg2.connect(
#             dbname='scraping_db',
#             user='postgres',
#             password='password',
#             host='127.0.0.1',
#             port='5432'
#         )
#         cursor = conn.cursor()

#         cursor.execute('SELECT * FROM scraped_data')
#         rows = cursor.fetchall()
#         conn.commit()
#     except Exception as e:
#         raise Exception(f"Failed to read records: {e}")
#     finally:
#         cursor.close()
#         conn.close()

#     return rows

# def update_record(condition, update_values):
#     try:
#         conn = psycopg2.connect(
#             dbname='scraping_db',
#             user='postgres',
#             password='password',
#             host='127.0.0.1',
#             port='5432'
#         )
#         cursor = conn.cursor()

#         set_clause = sql.SQL(', ').join(
#             sql.Composed([sql.Identifier(col), sql.SQL(' = '), sql.Placeholder()]) for col in update_values.keys()
#         )
#         where_clause = sql.SQL(' AND ').join(
#             sql.Composed([sql.Identifier(col), sql.SQL(' = '), sql.Placeholder()]) for col in condition.keys()
#         )

#         update_query = sql.SQL('''
#             UPDATE scraped_data
#             SET {}
#             WHERE {}
#         ''').format(set_clause, where_clause)

#         cursor.execute(update_query, list(update_values.values()) + list(condition.values()))
#         conn.commit()
#     except Exception as e:
#         conn.rollback()
#         raise Exception(f"Failed to update record: {e}")
#     finally:
#         cursor.close()
#         conn.close()

# def delete_record(condition):
#     try:
#         conn = psycopg2.connect(
#             dbname='scraping_db',
#             user='postgres',
#             password='password',
#             host='127.0.0.1',
#             port='5432'
#         )
#         cursor = conn.cursor()

#         where_clause = sql.SQL(' AND ').join(
#             sql.Composed([sql.Identifier(col), sql.SQL(' = '), sql.Placeholder()]) for col in condition.keys()
#         )

#         delete_query = sql.SQL('''
#             DELETE FROM scraped_data
#             WHERE {}
#         ''').format(where_clause)

#         cursor.execute(delete_query, list(condition.values()))
#         conn.commit()
#     except Exception as e:
#         conn.rollback()
#         raise Exception(f"Failed to delete record: {e}")
#     finally:
#         cursor.close()
#         conn.close()
