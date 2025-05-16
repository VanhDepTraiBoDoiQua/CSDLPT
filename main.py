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

        # Ngắt kết nối
        conn.close()

    except Exception as err:
        print(err)
