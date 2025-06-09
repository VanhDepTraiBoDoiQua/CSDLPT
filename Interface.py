import psycopg2
from psycopg2 import sql


# Tạo kết nối đến db
def getopenconnection(user='postgres', password='123456', dbname='postgres', host='localhost'):
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

#---------------------- Range Partitioning ---------------------#

def rangepartition(ratingstablename, numberofpartitions, openconnection):

    # Số phân vùng phải là số nguyên dương lớn hơn 0
    if numberofpartitions <= 0:
        return

    try:
        connection = openconnection
        cursor = connection.cursor()
        
        RANGE_TABLE_PREFIX = 'range_part'

        # Xóa các bảng phân vùng cũ (nếu có)
        cursor.execute("""
            SELECT tablename
            FROM pg_tables 
            WHERE tablename LIKE 'range_part%';
        """)
        old_tables = cursor.fetchall()
        for table_tuple in old_tables:
            table_name = table_tuple[0]
            cursor.execute(f"DROP TABLE IF EXISTS {table_name};")
        
        # Bảng metadata được tạo để lưu thông tin phân vùng
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS metadata (
                partition_type TEXT,
                num_partitions INTEGER,
                range_boundaries TEXT
            );
        """)
        
        cursor.execute("DELETE FROM metadata WHERE partition_type = 'range';")
        
        # Tính các biên phân vùng
        max_rating = 5.0
        min_rating = 0.0
        delta = (max_rating - min_rating) / numberofpartitions
        boundaries = [min_rating + i * delta for i in range(numberofpartitions + 1)]
        
        # Làm tròn các giá trị biên
        boundaries = [round(b * 100 + 0.5) / 100 for b in boundaries]
        boundaries_str = ",".join(str(b) for b in boundaries)

        # Lưu thông tin phân vùng vào bảng metadata
        cursor.execute("""
            INSERT INTO metadata (partition_type, num_partitions, range_boundaries)
            VALUES (%s, %s, %s);
        """, ('range', numberofpartitions, boundaries_str))
        
        # Tạo và phân phối dữ liệu vào các bảng phân vùng
        for i in range(numberofpartitions):
            table_name = f"{RANGE_TABLE_PREFIX}{i}"
            
            # Tạo bảng phân vùng nếu chưa tồn tại
            cursor.execute(f"""
                CREATE TABLE IF NOT EXISTS {table_name} (
                    userid INTEGER,
                    movieid INTEGER,
                    rating FLOAT
                );
            """)
            
            # Xóa dữ liệu cũ trong bảng phân vùng (nếu có)
            cursor.execute(f"DELETE FROM {table_name};")
            
            # Phân phối dữ liệu cho các phân vùng:
            # Phân vùng đầu tiên
            if i == 0:
                cursor.execute(f"""
                    INSERT INTO {table_name} (userid, movieid, rating)
                    SELECT userid, movieid, rating 
                    FROM {ratingstablename}
                    WHERE rating >= %s AND rating <= %s;
                """, (boundaries[i], boundaries[i + 1]))
            # Các phân vùng còn lại: 
            else:
                cursor.execute(f"""
                    INSERT INTO {table_name} (userid, movieid, rating)
                    SELECT userid, movieid, rating
                    FROM {ratingstablename}
                    WHERE rating > %s AND rating <= %s;
                """, (boundaries[i], boundaries[i + 1]))
        
        openconnection.commit()
        cursor.close()   
    except Exception as e:
        openconnection.rollback()
        print("Range Partitioning failed")
        print(e)
        raise e



def rangeinsert(ratingstablename, userid, itemid, rating, openconnection):

    # Kiểm tra điều kiện rating hợp lệ
    if not (0 <= rating <= 5):
        raise Exception("Rating must be between 0 and 5.")
    
    # Kiểm tra UserID và MovieID hợp lệ
    if not (isinstance(userid, int) and isinstance(itemid, int) and userid > 0 and itemid > 0):
        raise Exception("UserID and MovieID must be positive integers.")
    
    try:
        connection = openconnection
        cursor = connection.cursor()

        RANGE_TABLE_PREFIX = 'range_part'

        # Kiểm tra nếu dữ liệu đã tồn tại trong bảng gốc
        cursor.execute(f"""
            SELECT 1 FROM {ratingstablename}
            WHERE userid = %s AND movieid = %s AND rating = %s;
        """, (userid, itemid, rating))

        if cursor.fetchone():
            print("Dữ liệu đã tồn tại trong Ratings")
            return
        
        # Chèn dữ liệu vào bảng chính
        cursor.execute(f"""
            INSERT INTO {ratingstablename} (userid, movieid, rating)
            VALUES (%s, %s, %s);
        """, (userid, itemid, rating))
        
        # Lấy thông tin phân vùng từ metadata
        cursor.execute("SELECT num_partitions FROM metadata WHERE partition_type = 'range';")
        num_partitions = cursor.fetchone()[0]
        
        # Tính chỉ số phân vùng dựa trên rating
        partition_size = round(5.0 / num_partitions, 2)
        partition_index = int(rating / partition_size)
        if rating % partition_size == 0 and rating != 0:
            partition_index -= 1
        
        # Chèn vào phân mảnh tương ứng
        table_name = f"{RANGE_TABLE_PREFIX}{partition_index}"
        cursor.execute(f"""
            INSERT INTO {table_name} (userid, movieid, rating)
            VALUES (%s, %s, %s);
        """, (userid, itemid, rating))

        openconnection.commit()
        cursor.close()   
    except Exception as e:
        openconnection.rollback()
        print("Insert Range failed")
        print(e)
        raise e

# --------------------- Round Robin Partitioning ---------------------
def roundrobinpartition(ratingstablename, numberofpartitions, openconnection):
    o = openconnection
    cur = o.cursor()

    # create metadata table to keep track of next partition index
    cur.execute(
        "CREATE TABLE IF NOT EXISTS roundrobin_metadata(next_partition INT);"
    )
    cur.execute("DELETE FROM roundrobin_metadata;")
    cur.execute("INSERT INTO roundrobin_metadata VALUES (0);")

    # create partition tables
    for i in range(numberofpartitions):
        table_name = "rrobin_part" + str(i)
        cur.execute(
            "CREATE TABLE " + table_name + " (userid INTEGER, movieid INTEGER, rating FLOAT);"
        )

    # distribute existing rows in round robin fashion
    for i in range(numberofpartitions):
        insert_query = f"""
            INSERT INTO rrobin_part{i} (userid, movieid, rating)
            SELECT userid, movieid, rating
            FROM (
                SELECT userid, movieid, rating,
                       ROW_NUMBER() OVER () AS rn
                FROM {ratingstablename}
            ) AS tmp
            WHERE (rn-1) % {numberofpartitions} = {i};
        """
        cur.execute(insert_query)

    cur.close()
    openconnection.commit()


def roundrobininsert(ratingstablename, userid, itemid, rating, openconnection):
    o = openconnection
    cur = o.cursor()

    no_of_partitions = get_partitions_count("rrobin_part", openconnection)
    if no_of_partitions == 0:
        # nothing to insert into
        cur.close()
        openconnection.commit()
        return

    # ensure metadata table exists
    cur.execute(
        "CREATE TABLE IF NOT EXISTS roundrobin_metadata(next_partition INT);"
    )
    cur.execute("SELECT next_partition FROM roundrobin_metadata LIMIT 1;")
    row = cur.fetchone()
    if row is None:
        next_part = 0
        cur.execute("INSERT INTO roundrobin_metadata VALUES (1);")
    else:
        next_part = row[0]
        cur.execute(
            "UPDATE roundrobin_metadata SET next_partition = %s;",
            ((next_part + 1) % no_of_partitions,),
        )

    target_table = "rrobin_part" + str(next_part)
    cur.execute(
        "INSERT INTO " + ratingstablename + " (userid, movieid, rating) VALUES (%s, %s, %s)",
        (userid, itemid, rating),
    )
    cur.execute(
        "INSERT INTO " + target_table + " (userid, movieid, rating) VALUES (%s, %s, %s)",
        (userid, itemid, rating),
    )

    cur.close()
    openconnection.commit()


