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

# Đếm số lượng bảng có tên theo đúng quy tắc phân mảnh
def get_partitions_count(prefix, openconnection):
    o = openconnection
    cur = o.cursor()
    # Truy vấn số lượng bảng có tên bắt đầu bằng prefix
    cur.execute("SELECT COUNT(table_name) FROM information_schema.tables WHERE table_schema = 'public' AND table_name LIKE '{0}%';".format(prefix))
    count = int(cur.fetchone()[0])  # Đếm số lượng bảng và lấy kết quả
    cur.close()
    openconnection.commit()
    return count

# Hàm Range_Partition()
def rangePartition(ratingstablename, numberofpartitions, openconnection):
    o = openconnection
    cur = o.cursor()

    # Tính phạm vi phân mảnh
    partition_range = 5.0 / numberofpartitions

    # Tạo các phân mảnh
    for i in range(numberofpartitions):
        min_rating = i * partition_range
        max_rating = min_rating + partition_range
        partition_table_name = "range_ratings_part" + str(i)

        # Tạo bảng phân mảnh
        cur.execute("CREATE TABLE " + partition_table_name + " (userid INTEGER, movieid INTEGER, rating FLOAT);")

        # Chèn dữ liệu vào các bảng phân mảnh
        if i == 0:
            cur.execute(
                "INSERT INTO " + partition_table_name + " (userid, movieid, rating) SELECT userid, movieid, rating FROM " + ratingstablename +
                " WHERE rating >= " + str(min_rating) + " AND rating <= " + str(max_rating) + ";")
        else:
            cur.execute(
                "INSERT INTO " + partition_table_name + " (userid, movieid, rating) SELECT userid, movieid, rating FROM " + ratingstablename +
                " WHERE rating >" + str(min_rating) + " AND rating <= " + str(max_rating) + ";")

    cur.close()
    openconnection.commit()

# Hàm Range_Insert()
def rangeInsert(ratingstablename, userid, itemid, rating, openconnection):
    o = openconnection
    cur = o.cursor()

    # Lấy số lượng phân mảnh từ hàm get_partitions_count
    no_of_partitions = get_partitions_count("range_ratings_part", openconnection)
    partition_range = 5.0 / no_of_partitions

    # Xác định phân mảnh và chèn dữ liệu vào đó
    for i in range(no_of_partitions):
        min_rating = i * partition_range
        max_rating = min_rating + partition_range

        if i == 0:
            if rating >= min_rating and rating <= max_rating:
                partition_table = "range_ratings_part" + str(i)
                cur.execute("INSERT INTO " + ratingstablename + " (userid, movieid, rating) VALUES (%s, %s, %s)", (userid, itemid, rating))
                cur.execute("INSERT INTO " + partition_table + " (userid, movieid, rating) VALUES (%s, %s, %s)", (userid, itemid, rating))
                break
        else:
            if rating > min_rating and rating <= max_rating:
                partition_table = "range_ratings_part" + str(i)
                cur.execute("INSERT INTO " + ratingstablename + " (userid, movieid, rating) VALUES (%s, %s, %s)", (userid, itemid, rating))
                cur.execute("INSERT INTO " + partition_table + " (userid, movieid, rating) VALUES (%s, %s, %s)", (userid, itemid, rating))
                break

    cur.close()
    openconnection.commit()

