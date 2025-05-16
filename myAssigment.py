import psycopg2
from psycopg2 import sql

# Tạo kết nối đến db
def getopenconnection(user='postgres', password='1234', dbname='postgres', host='localhost'):
    conn = psycopg2.connect(
        dbname=dbname,
        user=user,
        password=password,
        host=host
    )
    print(f"Connected to {dbname}")
    return conn


# Tạo db mới
def create_db(dbname):
    # Tạo kết nối đến db mặc định
    conn = getopenconnection()

    # Thiết lập auto commit
    conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)

    # Tạo một con trỏ để thực hiện truy vấn
    cur = conn.cursor()

    # Kiểm tra xem đã tồn tại db cần tạo hay chưa
    cur.execute("SELECT COUNT(*) FROM pg_catalog.pg_database WHERE datname = %s", (dbname,))
    count = cur.fetchone()[0]

    if count == 0:
        # Tạo db mới
        createQuery = sql.SQL("CREATE DATABASE {dbname}").format(
            dbname=sql.Identifier(dbname)
        )
        cur.execute(createQuery)
        print(f"Created database {dbname}")

    else:
        print(f"Database {dbname} already exists")

    # Đóng kết nối
    cur.close()
    conn.close()


# Xoá tất cả bảng public của db
def deleteAllPublicTables(openconnection):
    cur = openconnection.cursor()

    cur.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")
    l = []
    for row in cur:
        l.append(row[0])
    for tablename in l:
        cur.execute("drop table if exists {0} CASCADE".format(tablename))

    cur.close()
    openconnection.commit()


def loadratings(ratingstablename, ratingsfilepath, openconnection):
    cur = openconnection.cursor()

    # Tạo bảng ratings
    # Sử dụng sql.Identifier để tránh tấn công SQL Injection
    createQuery = sql.SQL("""
        CREATE TABLE IF NOT EXISTS {table} (
            UserId INT,
            extra1 CHAR,
            MovieId INT,
            extra2 CHAR,
            Rating FLOAT,
            extra3 CHAR,
            timestamp BIGINT
        );
    """).format(table=sql.Identifier(ratingstablename))
    cur.execute(createQuery)
    print(f"Created table {ratingstablename}")

    # Đọc dữ liệu từ file và copy vào bảng
    with open(ratingsfilepath, 'r') as f:
        cur.copy_from(f, ratingstablename, sep=':')

    # Xoá các cột thừa
    alterQuery = sql.SQL("""
        ALTER TABLE {table}
        DROP COLUMN extra1,
        DROP COLUMN extra2,
        DROP COLUMN extra3,
        DROP COLUMN timestamp;
    """).format(table=sql.Identifier(ratingstablename))
    cur.execute(alterQuery)
    print(f"Data inserted into {ratingstablename} successfully")

    cur.close()
    openconnection.commit()
