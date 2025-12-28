#!/usr/bin/env python3
import mysql.connector
import time
import sys


def reproduce():
    print("Connecting to Calm server...")
    try:
        conn = mysql.connector.connect(
            host="127.0.0.1", port=3307, user="root", password="", database="public"
        )
        cursor = conn.cursor()

        # Check if table exists, if not create it
        print("Checking if taxi_trips exists...")
        try:
            cursor.execute("SELECT 1 FROM taxi_trips LIMIT 1")
            cursor.fetchall()
            print("Table taxi_trips exists.")
        except Exception as e:
            print(f"Table might not exist or error: {e}")
            print("Creating taxi_trips table...")
            # Create a table with enough columns to trigger the index 3 issue
            # The user mentioned index 3, so we need at least 4 columns.
            create_sql = """
            CREATE TABLE taxi_trips (
                vendor_id INT,
                pickup_datetime TIMESTAMP,
                dropoff_datetime TIMESTAMP,
                passenger_count INT,
                trip_distance DOUBLE,
                rate_code_id INT,
                store_and_fwd_flag KEYWORD,
                pickup_location_id INT,
                dropoff_location_id INT,
                payment_type INT,
                fare_amount DOUBLE,
                extra DOUBLE,
                mta_tax DOUBLE,
                tip_amount DOUBLE,
                tolls_amount DOUBLE,
                improvement_surcharge DOUBLE,
                total_amount DOUBLE,
                congestion_surcharge DOUBLE
            )
            """
            # Note: Calm uses GraphQL for DDL usually, but maybe SQL DDL is supported or I should use GraphQL.
            # The instructions say "GraphQL is the ONLY DDL interface".
            # So I cannot use SQL CREATE TABLE.
            print(
                "Cannot create table via SQL. Assuming it exists or using GraphQL if needed."
            )
            # If it doesn't exist, I'll try to run the query anyway to see what happens,
            # but likely I need to use the existing environment or create it via GraphQL.

        print("Running: select count(*) from taxi_trips")
        cursor.execute("select count(*) from taxi_trips")
        result = cursor.fetchall()
        print(f"Result: {result}")

        cursor.close()
        conn.close()

    except mysql.connector.Error as err:
        print(f"Error: {err}")
    except Exception as e:
        print(f"Unexpected error: {e}")


if __name__ == "__main__":
    reproduce()
