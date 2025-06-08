#
# Tester for the assignement1
#

# TODO: Change these as per your code
RATINGS_TABLE = 'ratings'
RANGE_TABLE_PREFIX = 'range_part'
RROBIN_TABLE_PREFIX = 'rrobin_part'
USER_ID_COLNAME = 'userid'
MOVIE_ID_COLNAME = 'movieid'
RATING_COLNAME = 'rating'
INPUT_FILE_PATH = 'ratings.dat'
ACTUAL_ROWS_IN_INPUT_FILE = 10000054  # Number of lines in the input file

import psycopg2
import traceback
import testHelper
import Interface as MyAssignment

def get_range_partition_index(rating, number_of_partitions):
    """
    Tính toán index của phân mảnh dựa trên rating và số phân mảnh
    """
    delta = 5.0 / number_of_partitions
    index = int(rating / delta)
    if rating % delta == 0 and index != 0:
        index = index - 1
    return str(index)

if __name__ == '__main__':
    try:
        # Nhập tên database từ bàn phím
        DATABASE_NAME = input("Nhập tên cơ sở dữ liệu: ").strip()
        if not DATABASE_NAME:
            print("Tên cơ sở dữ liệu không được để trống!")
            exit(1)
            
        print("\nBắt đầu tạo và kiểm tra cơ sở dữ liệu...")
        
        # Tạo database với tên đã nhập
        testHelper.createdb(DATABASE_NAME)

        # Kết nối đến database đã tạo
        with testHelper.getopenconnection(dbname=DATABASE_NAME) as conn:
            conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)

            testHelper.deleteAllPublicTables(conn)
            print("\nĐã xóa các bảng cũ (nếu có)")

            [result, e] = testHelper.testloadratings(MyAssignment, RATINGS_TABLE, INPUT_FILE_PATH, conn, ACTUAL_ROWS_IN_INPUT_FILE)
            if result :
                print("Hàm loadratings đã chạy thành công!")
            else:
                print("Hàm loadratings thất bại!")

            # Nhập số phân mảnh từ bàn phím
            while True:
                try:
                    number_of_partitions = int(input("\nNhập số phân mảnh cần tạo: "))
                    if number_of_partitions <= 0:
                        print("Số phân mảnh phải lớn hơn 0!")
                        continue
                    break
                except ValueError:
                    print("Vui lòng nhập một số nguyên!")

            [result, e] = testHelper.testrangepartition(MyAssignment, RATINGS_TABLE, number_of_partitions, conn, 0, ACTUAL_ROWS_IN_INPUT_FILE)
            if result :
                print("Hàm rangepartition đã chạy thành công!")
            else:
                print("Hàm rangepartition thất bại!")

            # Test rangeinsert với partition index được tính toán dựa trên rating
            test_rating = 3.0  # Rating để test
            partition_index = get_range_partition_index(test_rating, number_of_partitions)
            [result, e] = testHelper.testrangeinsert(MyAssignment, RATINGS_TABLE, 100, 2, test_rating, conn, partition_index)
            if result:
                print("Hàm rangeinsert đã chạy thành công!")
            else:
                print("Hàm rangeinsert thất bại!")

            # Thực hiện roundrobin partition trực tiếp trên bảng ratings hiện tại
            [result, e] = testHelper.testroundrobinpartition(MyAssignment, RATINGS_TABLE, number_of_partitions, conn, 0, ACTUAL_ROWS_IN_INPUT_FILE)
            if result :
                print("Hàm roundrobinpartition đã chạy thành công!")
            else:
                print("Hàm roundrobinpartition thất bại!")

            # Test roundrobininsert với partition index phù hợp
            partition_index = '0' if number_of_partitions == 1 else '1'
            [result, e] = testHelper.testroundrobininsert(MyAssignment, RATINGS_TABLE, 9999999, 1, 3, conn, partition_index)
            if result :
                print("Hàm roundrobininsert đã chạy thành công!")
            else:
                print("Hàm roundrobininsert thất bại!")

            choice = input('\nNhấn Enter để xóa tất cả các bảng? ')
            if choice == '':
                testHelper.deleteAllPublicTables(conn)
                print("Đã xóa tất cả các bảng")
            if not conn.close:
                conn.close()

    except Exception as detail:
        print("\nCó lỗi xảy ra:")
        traceback.print_exc()