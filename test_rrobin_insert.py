import Interface
import psycopg2

def test_rrobin_insert():
    # Kết nối đến database
    conn = psycopg2.connect(
        database="csdlpt",  # Thay đổi tên database của bạn ở đây
        user="postgres",
        password="1234",
        host="localhost",
        port="5432"
    )
    
    try:
        # Test case 1: Insert dòng đầu tiên (sẽ vào part0)
        print("Test case 1: Insert first row")
        Interface.roundrobininsert("ratings", 1, 1, 3.5, conn)
        
        # Test case 2: Insert dòng thứ hai (sẽ vào part1)
        print("Test case 2: Insert second row")
        Interface.roundrobininsert("ratings", 2, 2, 4.0, conn)
        
        # Test case 3: Insert dòng thứ ba (sẽ vào part2)
        print("Test case 3: Insert third row")
        Interface.roundrobininsert("ratings", 3, 3, 2.5, conn)
        
        # Test case 4: Insert dòng thứ tư (sẽ vào part3)
        print("Test case 4: Insert fourth row")
        Interface.roundrobininsert("ratings", 4, 4, 3.0, conn)
        
        # Test case 5: Insert dòng thứ năm (sẽ vào part0)
        print("Test case 5: Insert fifth row")
        Interface.roundrobininsert("ratings", 5, 5, 4.5, conn)
        
        print("All test cases completed!")
        
    except Exception as e:
        print(f"Error occurred: {e}")
    finally:
        conn.close()

if __name__ == "__main__":
    test_rrobin_insert() 