import psycopg2
from psycopg2 import sql

RROBIN_TABLE_PREFIX = 'rrobin_part'
META_TABLE = 'roundrobin_metadata'
SEQ_NAME = 'rr_seq' 

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

def get_partitions_count(prefix, openconnection):
    """
    Đếm số bảng partition round robin đang tồn tại.
    """
    cur = openconnection.cursor()
    cur.execute(
        "SELECT COUNT(table_name) FROM information_schema.tables "
        "WHERE table_schema = 'public' AND table_name LIKE %s;",
        (prefix + '%',)
    )
    count = cur.fetchone()[0]
    cur.close()
    return count

def roundrobinpartition(ratingstablename, numberofpartitions, openconnection):
    """
    Chia dữ liệu bảng rating thành nhiều partition theo round robin.
    """
    with openconnection.cursor() as cur:
        # Xóa các bảng partition cũ (nếu có)
        cur.execute(sql.SQL("""
            SELECT tablename
            FROM   pg_tables
            WHERE  schemaname = 'public'
            AND tablename LIKE %s
        """), (RROBIN_TABLE_PREFIX + '%',))
        old_parts = [t[0] for t in cur.fetchall()]
        for t in old_parts:
            cur.execute(sql.SQL("DROP TABLE IF EXISTS {} CASCADE").format(sql.Identifier(t)))
        # Tạo các bảng partition mới
        for i in range(numberofpartitions):
            cur.execute(sql.SQL("""
                CREATE UNLOGGED TABLE {part} (
                    userid  INTEGER,
                    movieid INTEGER,
                    rating  FLOAT
                );
            """).format(part=sql.Identifier(f"{RROBIN_TABLE_PREFIX}{i}")))
        # Đánh số dòng và insert vào từng partition
        base_query = sql.SQL("""
            WITH numbered AS (
                SELECT userid, movieid, rating,
                       ROW_NUMBER() OVER () - 1 AS rn   -- bắt đầu từ 0
                FROM   {ratings}
            )
        """).format(ratings=sql.Identifier(ratingstablename))
        for i in range(numberofpartitions):
            cur.execute(base_query + sql.SQL("""
                INSERT INTO {part} (userid, movieid, rating)
                SELECT userid, movieid, rating
                FROM   numbered
                WHERE  rn % {n} = {i};
            """).format(
                part = sql.Identifier(f"{RROBIN_TABLE_PREFIX}{i}"),
                n    = sql.Literal(numberofpartitions),
                i    = sql.Literal(i)
            ))
        # Reset lại sequence và metadata
        cur.execute("""
            DROP SEQUENCE IF EXISTS rr_seq CASCADE;
            CREATE SEQUENCE rr_seq;
        """)
        cur.execute("""
            DROP TABLE IF EXISTS roundrobin_metadata;
            CREATE TABLE roundrobin_metadata (
                num_partitions  INTEGER NOT NULL,
                CONSTRAINT rr_meta_pk CHECK (num_partitions > 0)
            );
        """)
        cur.execute("INSERT INTO roundrobin_metadata VALUES (%s);", (numberofpartitions,))
        openconnection.commit()

def roundrobininsert(ratingstablename, userid, itemid, rating, openconnection):
    """
    Thêm 1 dòng vào bảng rating và partition tương ứng theo round robin.
    """
    with openconnection.cursor() as cur:
        # Lấy số lượng partition
        cur.execute(sql.SQL("SELECT num_partitions FROM {m};").format(m=sql.Identifier(META_TABLE)))
        row = cur.fetchone()
        if row is None:
            raise RuntimeError("Thiếu thông tin metadata round robin")
        n_partitions = row[0]
        # Lấy slot tiếp theo (atomic)
        cur.execute(sql.SQL("SELECT nextval({seq});").format(seq=sql.Literal(SEQ_NAME)))
        slot = cur.fetchone()[0]
        target_index = (slot - 1) % n_partitions
        target_table = f"{RROBIN_TABLE_PREFIX}{target_index}"
        # Insert vào bảng chính và bảng partition
        insert_sql = sql.SQL("INSERT INTO {tbl} (userid, movieid, rating) VALUES (%s, %s, %s)")
        for tbl in (ratingstablename, target_table):
            cur.execute(insert_sql.format(tbl=sql.Identifier(tbl)), (userid, itemid, rating))
        openconnection.commit()