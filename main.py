import config
import myAssigment

if __name__ == '__main__':
    try:
        # Tạo db mới
        myAssigment.create_db(config.DB_NAME)

        # Tạo kết nối đến db vừa tạo
        conn = myAssigment.getopenconnection(config.USER, config.PASSWORD, config.DB_NAME, config.HOST)

        # Reset db
        myAssigment.deleteAllPublicTables(conn)

        # Load ratings
        myAssigment.loadratings("ratings", config.RATINGS_FILE_PATH, conn)

        # TODO: Các hàm phân mảnh

        # Phân mảnh theo Range - Partition_Range()
        print("Doing the Range Partitions")
        myAssigment.rangePartition('ratings', 5, conn)

        # Chèn dữ liệu vào phân mảnh Range - Partition_Insert()
        print("Inserting data into Range Partitions")
        myAssigment.rangeInsert('ratings', 100, 2, 3.5, conn)

        # Ngắt kết nối
        conn.close()

    except Exception as err:
        print(err)
