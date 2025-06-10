import Interface
import config

if __name__ == '__main__':
    try:
        # Tạo db mới
        Interface.create_db(config.DB_NAME)

        # Tạo kết nối đến db vừa tạo
        conn = Interface.getopenconnection(config.USER, config.PASSWORD, config.DB_NAME, config.HOST)

        # Reset db
        Interface.deleteAllPublicTables(conn)

        # Load ratings
        Interface.loadratings("ratings", config.RATINGS_FILE_PATH, conn)

        # Phân mảnh theo khoảng
        Interface.rangepartition("ratings", 5, conn)

        # Chèn dữ liệu theo khoảng
        # Interface.rangeinsert("ratings", 100, 2, 3, conn)

        # Phân mảnh vòng tròn
        Interface.roundrobinpartition("ratings", 5, conn)

        # Chèn dữ liệu vòng tròn
        # Interface.roundrobininsert("ratings", 100, 2, 3, conn)

        # Ngắt kết nối
        conn.close()

    except Exception as err:
        print(err)
