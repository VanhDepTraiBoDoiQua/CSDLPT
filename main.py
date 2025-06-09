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

        # TODO: Các hàm phân mảnh

        # Ngắt kết nối
        conn.close()

    except Exception as err:
        print(err)
