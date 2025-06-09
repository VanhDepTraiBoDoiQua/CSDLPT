import psycopg2
from psycopg2 import sql

RROBIN_TABLE_PREFIX = 'rrobin_part'

# Open connection to the database
def getopenconnection(user='postgres', password='12345678', dbname='postgres', host='localhost'):
    conn = psycopg2.connect(
        dbname=dbname,
        user=user,
        password=password,
        host=host
    )
    print(f"Connected to {dbname}")
    return conn

# Create a new database if it doesn't exist
def create_db(dbname):
    conn = getopenconnection()
    conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM pg_catalog.pg_database WHERE datname = %s", (dbname,))
    if cur.fetchone()[0] == 0:
        cur.execute(sql.SQL("CREATE DATABASE {dbname}").format(dbname=sql.Identifier(dbname)))
        print(f"Created database {dbname}")
    else:
        print(f"Database {dbname} already exists")
    cur.close()
    conn.close()

# Drop all tables in the public schema
def deleteAllPublicTables(openconnection):
    cur = openconnection.cursor()
    cur.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")
    for (tablename,) in cur:
        cur.execute(f"DROP TABLE IF EXISTS {tablename} CASCADE")
    cur.close()
    openconnection.commit()

# Load ratings data into a table
def loadratings(ratingstablename, ratingsfilepath, openconnection):
    cur = openconnection.cursor()
    createQuery = sql.SQL(
        """
        CREATE TABLE IF NOT EXISTS {table} (
            UserId INT,
            extra1 CHAR,
            MovieId INT,
            extra2 CHAR,
            Rating FLOAT,
            extra3 CHAR,
            timestamp BIGINT
        );
        """
    ).format(table=sql.Identifier(ratingstablename))
    cur.execute(createQuery)
    print(f"Created table {ratingstablename}")
    with open(ratingsfilepath, 'r') as f:
        cur.copy_from(f, ratingstablename, sep=':')
    alterQuery = sql.SQL(
        """
        ALTER TABLE {table}
        DROP COLUMN extra1,
        DROP COLUMN extra2,
        DROP COLUMN extra3,
        DROP COLUMN timestamp;
        """
    ).format(table=sql.Identifier(ratingstablename))
    cur.execute(alterQuery)
    print(f"Data inserted into {ratingstablename} successfully")
    cur.close()
    openconnection.commit()

# Count existing round robin partitions
def get_partitions_count(prefix, openconnection):
    cur = openconnection.cursor()
    cur.execute(
        "SELECT COUNT(table_name) FROM information_schema.tables "
        "WHERE table_schema = 'public' AND table_name LIKE %s;",
        (prefix + '%',)
    )
    count = cur.fetchone()[0]
    cur.close()
    openconnection.commit()
    return count

# Round Robin Partitioning
def roundrobinpartition(ratingstablename, numberofpartitions, openconnection):
    cur = openconnection.cursor()
    # Metadata table for next partition index
    cur.execute("CREATE TABLE IF NOT EXISTS roundrobin_metadata(next_partition INT);")
    cur.execute("DELETE FROM roundrobin_metadata;")
    cur.execute("INSERT INTO roundrobin_metadata VALUES (0);")
    # Create partition tables
    for i in range(numberofpartitions):
        table_name = RROBIN_TABLE_PREFIX + str(i)
        cur.execute(
            f"CREATE TABLE IF NOT EXISTS {table_name} (userid INTEGER, movieid INTEGER, rating FLOAT);"
        )
    # Distribute existing rows
    for i in range(numberofpartitions):
        part_name = f"{RROBIN_TABLE_PREFIX}{i}"
        insert_query = sql.SQL(
            "INSERT INTO {part} (userid, movieid, rating) "
            "SELECT userid, movieid, rating FROM ("
            "  SELECT userid, movieid, rating, ROW_NUMBER() OVER () AS rn "
            "  FROM {main}"
            ") AS tmp "
            "WHERE (rn - 1) %% %s = %s;"
        ).format(
            part=sql.Identifier(part_name),
            main=sql.Identifier(ratingstablename)
        )
        cur.execute(insert_query, (numberofpartitions, i))
    cur.close()
    openconnection.commit()

# Insert a single row in round robin fashion
def roundrobininsert(ratingstablename, userid, itemid, rating, openconnection):
    cur = openconnection.cursor()
    no_of_partitions = get_partitions_count(RROBIN_TABLE_PREFIX, openconnection)
    if no_of_partitions == 0:
        cur.close()
        openconnection.commit()
        return
    # Ensure metadata exists
    cur.execute("CREATE TABLE IF NOT EXISTS roundrobin_metadata(next_partition INT);")
    cur.execute("SELECT next_partition FROM roundrobin_metadata LIMIT 1;")
    row = cur.fetchone()
    if row is None:
        next_part = 0
        cur.execute("INSERT INTO roundrobin_metadata VALUES (1);")
    else:
        next_part = row[0]
        cur.execute(
            "UPDATE roundrobin_metadata SET next_partition = %s;",
            ((next_part + 1) % no_of_partitions,)
        )
    # Insert into main and partition table
    target_table = RROBIN_TABLE_PREFIX + str(next_part)
    cur.execute(
        sql.SQL("INSERT INTO {main_table} (userid, movieid, rating) VALUES (%s, %s, %s)")
        .format(main_table=sql.Identifier(ratingstablename)),
        (userid, itemid, rating)
    )
    cur.execute(
        sql.SQL("INSERT INTO {part_table} (userid, movieid, rating) VALUES (%s, %s, %s)")
        .format(part_table=sql.Identifier(target_table)),
        (userid, itemid, rating)
    )
    cur.close()
    openconnection.commit()
