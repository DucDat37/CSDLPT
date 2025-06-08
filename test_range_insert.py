import Interface
import psycopg2

def test_range_insert():
    # Kết nối đến database
    conn = psycopg2.connect(
        database="csdlpt",  # Thay đổi tên database của bạn ở đây
        user="postgres",
        password="1234",
        host="localhost",
        port="5432"
    )
    
    try:
        # Test case 1: Insert vào phân mảnh 0 (rating < 2.0)
        print("Test case 1: Insert rating 1.5")
        Interface.rangeinsert("ratings", 1, 1, 1.5, conn)
        
        # Test case 2: Insert vào phân mảnh 1 (2.0 <= rating < 3.0)
        print("Test case 2: Insert rating 2.5")
        Interface.rangeinsert("ratings", 2, 2, 2.5, conn)
        
        # Test case 3: Insert vào phân mảnh 2 (3.0 <= rating < 4.0)
        print("Test case 3: Insert rating 3.5")
        Interface.rangeinsert("ratings", 3, 3, 3.5, conn)
        
        # Test case 4: Insert vào phân mảnh 3 (rating >= 4.0)
        print("Test case 4: Insert rating 4.5")
        Interface.rangeinsert("ratings", 4, 4, 4.5, conn)
        
        print("All test cases completed!")
        
    except Exception as e:
        print(f"Error occurred: {e}")
    finally:
        conn.close()

if __name__ == "__main__":
    test_range_insert() 