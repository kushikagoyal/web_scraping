import requests
from bs4 import BeautifulSoup
import psycopg2
from psycopg2 import sql, OperationalError
import csv
import os

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

def create_record(headers, values):
    try:
        conn = psycopg2.connect(
            dbname='scraping_db',
            user='postgres',
            password='password',
            host='127.0.0.1',
            port='5432'
        )
        cursor = conn.cursor()

        insert_query = sql.SQL('''
            INSERT INTO scraped_data ({})
            VALUES ({})
        ''').format(
            sql.SQL(', ').join(map(sql.Identifier, headers)),
            sql.SQL(', ').join(sql.Placeholder() * len(headers))
        )

        cursor.execute(insert_query, values)
        conn.commit()
    except Exception as e:
        conn.rollback()
        raise Exception(f"Failed to create record: {e}")
    finally:
        cursor.close()
        conn.close()

def read_records():
    try:
        conn = psycopg2.connect(
            dbname='scraping_db',
            user='postgres',
            password='password',
            host='127.0.0.1',
            port='5432'
        )
        cursor = conn.cursor()

        cursor.execute('SELECT * FROM scraped_data')
        rows = cursor.fetchall()
        conn.commit()
    except Exception as e:
        raise Exception(f"Failed to read records: {e}")
    finally:
        cursor.close()
        conn.close()

    return rows

def update_record(condition, update_values):
    try:
        conn = psycopg2.connect(
            dbname='scraping_db',
            user='postgres',
            password='password',
            host='127.0.0.1',
            port='5432'
        )
        cursor = conn.cursor()

        set_clause = sql.SQL(', ').join(
            sql.Composed([sql.Identifier(col), sql.SQL(' = '), sql.Placeholder()]) for col in update_values.keys()
        )
        where_clause = sql.SQL(' AND ').join(
            sql.Composed([sql.Identifier(col), sql.SQL(' = '), sql.Placeholder()]) for col in condition.keys()
        )

        update_query = sql.SQL('''
            UPDATE scraped_data
            SET {}
            WHERE {}
        ''').format(set_clause, where_clause)

        cursor.execute(update_query, list(update_values.values()) + list(condition.values()))
        conn.commit()
    except Exception as e:
        conn.rollback()
        raise Exception(f"Failed to update record: {e}")
    finally:
        cursor.close()
        conn.close()

def delete_record(condition):
    try:
        conn = psycopg2.connect(
            dbname='scraping_db',
            user='postgres',
            password='password',
            host='127.0.0.1',
            port='5432'
        )
        cursor = conn.cursor()

        where_clause = sql.SQL(' AND ').join(
            sql.Composed([sql.Identifier(col), sql.SQL(' = '), sql.Placeholder()]) for col in condition.keys()
        )

        delete_query = sql.SQL('''
            DELETE FROM scraped_data
            WHERE {}
        ''').format(where_clause)

        cursor.execute(delete_query, list(condition.values()))
        conn.commit()
    except Exception as e:
        conn.rollback()
        raise Exception(f"Failed to delete record: {e}")
    finally:
        cursor.close()
        conn.close()

def export_to_csv(file_path):
    rows = read_records()

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

if __name__ == '__main__':
    try:
        headers, rows = scrape_table(url)
        store_data_in_postgresql(headers, rows)
        export_to_csv('scraped_data.csv')
    except Exception as e:
        print(f"An error occurred: {e}")
