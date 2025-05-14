import psycopg2


# Tạo kết nối đến db
def getopenconnection(user='postgres', password='1234', dbname='postgres'):
    query = f"dbname='{dbname}' user='{user}' host='localhost' password='{password}'"
    conn = psycopg2.connect(query)
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
    countQuery = f"SELECT COUNT(*) FROM pg_catalog.pg_database WHERE datname='{dbname}'"
    cur.execute(countQuery)
    count = cur.fetchone()[0]

    if count == 0:
        # Tạo db mới
        createQuery = f"CREATE DATABASE {dbname}"
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


def loadratings(ratingstablename, ratingsfilepath, openconnection):
    cur = openconnection.cursor()

    # Tạo bảng ratings
    createQuery = f"""
        CREATE TABLE IF NOT EXISTS {ratingstablename} (
            UserId int,
            extra1 char,
            MovieId int,
            extra2 char,
            Rating float,
            extra3 char,
            timestamp bigint
        );
    """
    cur.execute(createQuery)
    print(f"Created table {ratingstablename}")

    # Đọc dữ liệu từ file và copy vào bảng
    cur.copy_from(open(ratingsfilepath), ratingstablename, sep=':')

    # Xoá các cột thừa
    alterQuery = f"""
        ALTER TABLE {ratingstablename}
        DROP COLUMN extra1,
        DROP COLUMN extra2,
        DROP COLUMN extra3,
        DROP COLUMN timestamp;
    """
    cur.execute(alterQuery)
    print(f"Data inserted into {ratingstablename} successfully")

    cur.close()
