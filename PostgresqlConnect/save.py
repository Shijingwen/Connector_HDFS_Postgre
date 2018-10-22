def insert_table(self):
    # Connect to an existing database
    conn = psycopg2.connect(self.connString)

    # Open a cursor to perform database operations
    cur = conn.cursor()

    # Pass data to fill a query placeholders and let Psycopg perform
    # the correct conversion (no more SQL injections!)
    cur.execute("INSERT INTO test (num, data) VALUES (%s, %s)", (100, "abc'def"))

    # Make the changes to the database persistent
    conn.commit()

    # Close communication with the database
    cur.close()
    conn.close()
