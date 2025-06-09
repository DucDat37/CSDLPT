#!/usr/bin/python2.7
#
# Interface for the assignement
#

import psycopg2
import time

# Các hằng số định nghĩa prefix và tên cột
RANGE_TABLE_PREFIX = 'range_part'  # Prefix cho tên bảng phân vùng theo khoảng giá trị
RROBIN_TABLE_PREFIX = 'rrobin_part'  # Prefix cho tên bảng phân vùng theo round-robin
USER_ID_COLNAME = 'userid'  # Tên cột chứa ID của user
MOVIE_ID_COLNAME = 'movieid'  # Tên cột chứa ID của movie
RATING_COLNAME = 'rating'  # Tên cột chứa giá trị rating

def getopenconnection(dbname='postgres'):
    """
    Hàm tạo kết nối đến PostgreSQL database
    
    Parameters:
    -----------
    dbname : str, optional
        Tên database muốn kết nối đến. Mặc định là 'postgres'
        
    Returns:
    --------
    connection : psycopg2.extensions.connection
        Đối tượng kết nối đến database
        
    Raises:
    -------
    Exception
        Nếu không thể kết nối đến database
        
    Notes:
    -----
    - Hàm này sử dụng thư viện psycopg2 để kết nối đến PostgreSQL
    - Các thông số kết nối mặc định:
        + user: postgres
        + password: postgres
        + host: localhost
        + port: 5432
    - Nếu không truyền tên database, sẽ kết nối đến database mặc định 'postgres'
    - Database 'postgres' thường được sử dụng để tạo/xóa các database khác
    """
    try:
        # Tạo kết nối đến database với các thông số mặc định
        connection = psycopg2.connect(
            database=dbname,  # Tên database
            user="postgres",  # Tên người dùng
            password="1234",  # Mật khẩu
            host="localhost", # Địa chỉ host
            port="5432"      # Cổng kết nối
        )
        return connection
    except Exception as e:
        # In thông báo lỗi nếu không thể kết nối
        print("Error: Could not connect to the database")
        print(e)
        raise e

def loadratings(ratingstablename, ratingsfilepath, openconnection):
    """
    Function to load data in @ratingsfilepath file to a table called @ratingstablename.
    """
    try:
        start_time = time.time()
        
        # Tạo bảng với cấu trúc phù hợp cho file input
        cur = openconnection.cursor()
        cur.execute(f"""
        CREATE TABLE {ratingstablename} (
            {USER_ID_COLNAME} INTEGER,
            extra1 CHAR,
            {MOVIE_ID_COLNAME} INTEGER,
            extra2 CHAR,
            {RATING_COLNAME} FLOAT,
            extra3 CHAR,
            timestamp BIGINT
        )
        """)
        
        # Copy trực tiếp từ file vào bảng
        with open(ratingsfilepath, 'r') as f:
            cur.copy_from(f, ratingstablename, sep=':')
        
        # Xóa các cột không cần thiết
        cur.execute(f"""
        ALTER TABLE {ratingstablename} 
        DROP COLUMN extra1,
        DROP COLUMN extra2,
        DROP COLUMN extra3,
        DROP COLUMN timestamp
        """)
        
        # Thêm primary key
        cur.execute(f"""
        ALTER TABLE {ratingstablename} 
        ADD PRIMARY KEY ({USER_ID_COLNAME}, {MOVIE_ID_COLNAME})
        """)
        
        # Commit và đóng cursor
        openconnection.commit()
        cur.close()
        
        # Tính và in thời gian thực thi
        end_time = time.time()
        execution_time = end_time - start_time
        print(f"Thời gian thực thi hàm loadratings: {execution_time:.2f} giây")
        
    except Exception as e:
        openconnection.rollback()
        print("Error: Could not load ratings from file")
        print(e)
        raise e

def rangepartition(ratingstablename, numberofpartitions, openconnection):
    """
    Function to partition the ratings table into numberofpartitions partitions based on ranges of ratings.
    Optimized for speed.
    """
    try:
        start_time = time.time()
        
        cur = openconnection.cursor()
        
        # Tắt các tính năng không cần thiết để tăng tốc
        cur.execute("SET session_replication_role = 'replica'")  # Tắt triggers
        cur.execute("SET synchronous_commit = OFF")  # Tắt synchronous commit
        
        # Tính khoảng giá trị cho mỗi phân mảnh
        delta = 5.0 / numberofpartitions
        
        # Tạo các bảng phân mảnh với UNLOGGED để tăng tốc
        for i in range(numberofpartitions):
            min_range = i * delta
            max_range = min_range + delta
            table_name = RANGE_TABLE_PREFIX + str(i)
            
            # Tạo bảng UNLOGGED để tăng tốc độ insert
            cur.execute(f"""
            CREATE UNLOGGED TABLE {table_name} (
                userid INTEGER,
                movieid INTEGER,
                rating FLOAT,
                PRIMARY KEY (userid, movieid)
            ) WITH (
                autovacuum_enabled = false
            )
            """)
            
            # Insert dữ liệu với điều kiện đơn giản
            if i == 0:
                cur.execute(f"""
                INSERT INTO {table_name} (userid, movieid, rating)
                SELECT userid, movieid, rating
                FROM {ratingstablename}
                WHERE rating <= {max_range}
                """)
            else:
                cur.execute(f"""
                INSERT INTO {table_name} (userid, movieid, rating)
                SELECT userid, movieid, rating
                FROM {ratingstablename}
                WHERE rating > {min_range} AND rating <= {max_range}
                """)
            
            # Commit sau mỗi phân vùng để tránh transaction quá lớn
            openconnection.commit()
        
        # Bật lại các tính năng
        cur.execute("SET session_replication_role = 'origin'")
        cur.execute("SET synchronous_commit = ON")
        
        # Commit và đóng cursor
        openconnection.commit()
        cur.close()
        
        # Tính và in thời gian thực thi
        end_time = time.time()
        execution_time = end_time - start_time
        print(f"Thời gian thực thi hàm rangepartition: {execution_time:.2f} giây")
        
    except Exception as e:
        openconnection.rollback()
        print("Error: Could not create range partitions")
        print(e)
        raise e
    finally:
        # Đảm bảo các cài đặt được reset về mặc định
        try:
            cur = openconnection.cursor()
            cur.execute("SET session_replication_role = 'origin'")
            cur.execute("SET synchronous_commit = ON")
            cur.close()
        except:
            pass

def roundrobinpartition(ratingstablename, numberofpartitions, openconnection):
    """
    Function to create partitions of main table using round robin approach.
    Sử dụng truy vấn SQL để phân mảnh dữ liệu theo round robin
    
    Thuật toán phân vùng theo round-robin:
    1. Nguyên lý hoạt động:
       - Phân phối dữ liệu luân phiên vào các bảng con
       - Mỗi bản ghi được gán một số thứ tự (row_num)
       - Bảng con được chọn dựa trên phép chia lấy dư: row_num % numberofpartitions
    
    2. Tạo các bảng con:
       - Mỗi bảng con có prefix 'rrobin_part'
       - Cấu trúc bảng: userid, movieid, rating
       - Tên bảng: rrobin_part0, rrobin_part1, ...
    
    3. Phân phối dữ liệu:
       - Bản ghi 0 -> rrobin_part0
       - Bản ghi 1 -> rrobin_part1
       - Bản ghi 2 -> rrobin_part2
       - ...
       - Bản ghi n -> rrobin_part(n % numberofpartitions)
    
    4. Ưu điểm:
       - Phân phối dữ liệu đều giữa các bảng con
       - Không phụ thuộc vào giá trị của dữ liệu
       - Dễ dàng thêm/xóa phân vùng
       - Hiệu quả cho các truy vấn song song
    
    5. Nhược điểm:
       - Không tối ưu cho các truy vấn có điều kiện trên rating
       - Cần quét tất cả các bảng con khi tìm kiếm theo giá trị
       - Khó khăn trong việc tìm kiếm theo khoảng giá trị
    """
    try:
        start_time = time.time()
        
        con = openconnection
        cur = con.cursor()
        RROBIN_TABLE_PREFIX = 'rrobin_part'

        # Tạo các bảng phân mảnh
        for i in range(numberofpartitions):
            table_name = RROBIN_TABLE_PREFIX + str(i)
            cur.execute(f"""
                CREATE TABLE {table_name} (
                    userid INTEGER,
                    movieid INTEGER,
                    rating FLOAT
                )
            """)
        
        # Phân phối dữ liệu theo round robin
        query = f"""
        DO $$
        DECLARE
            target_partition text;
            row_data record;
        BEGIN
            FOR row_data IN 
                SELECT 
                    userid,
                    movieid,
                    rating,
                    ROW_NUMBER() OVER (ORDER BY userid, movieid) - 1 as row_num
                FROM {ratingstablename}
            LOOP
                -- Tính toán tên bảng phân mảnh
                target_partition := '{RROBIN_TABLE_PREFIX}' || (row_data.row_num % {numberofpartitions})::text;
                
                -- Insert dữ liệu vào phân mảnh tương ứng
                EXECUTE format('INSERT INTO %I (userid, movieid, rating) VALUES ($1, $2, $3)', target_partition)
                USING row_data.userid, row_data.movieid, row_data.rating;
            END LOOP;
        END $$;
        """
        
        cur.execute(query)
        
        # Commit và đóng cursor
        openconnection.commit()
        cur.close()
        
        # Tính và in thời gian thực thi
        end_time = time.time()
        execution_time = end_time - start_time
        print(f"Thời gian thực thi hàm roundrobinpartition: {execution_time:.2f} giây")
        
    except Exception as e:
        openconnection.rollback()
        print("Error: Could not create round robin partitions")
        print(e)
        raise e

def roundrobininsert(ratingstablename, userid, itemid, rating, openconnection):
    """
    Function to insert a new row into the main table and specific partition based on round robin
    approach.
    
    Parameters:
    -----------
    ratingstablename : str
        Tên bảng ratings
    userid : int
        ID của user
    itemid : int
        ID của movie
    rating : float
        Giá trị rating
    openconnection : psycopg2.extensions.connection
        Kết nối đến database
        
    Notes:
    -----
    - Insert bản ghi vào bảng chính
    - Xác định bảng con cần insert dựa trên số lượng bản ghi
    - Insert vào bảng con tương ứng
    """
    con = openconnection
    cur = con.cursor()
    RROBIN_TABLE_PREFIX = 'rrobin_part'
    
    try:
        # Insert vào bảng ratings trước
        cur.execute(f"INSERT INTO {ratingstablename} (userid, movieid, rating) VALUES (%s, %s, %s)",
                   (userid, itemid, rating))
        
        # Đếm tổng số dòng trong bảng ratings sau khi insert
        cur.execute(f"SELECT COUNT(*) FROM {ratingstablename}")
        total_rows = cur.fetchone()[0]
        
        # Tính toán số phân mảnh
        numberofpartitions = count_partitions(RROBIN_TABLE_PREFIX, openconnection)
        
        # Tính toán index của phân mảnh cần insert (trừ 1 vì đã insert vào bảng chính)
        partition_index = (total_rows - 1) % numberofpartitions
        
        # Tạo truy vấn SQL để insert dữ liệu vào phân mảnh
        query = f"""
        DO $$
        DECLARE
            target_partition text;
        BEGIN
            -- Tính toán tên bảng phân mảnh
            target_partition := '{RROBIN_TABLE_PREFIX}' || '{partition_index}';
            
            -- Insert dữ liệu vào phân mảnh tương ứng
            EXECUTE format('INSERT INTO %I (userid, movieid, rating) VALUES ($1, $2, $3)', target_partition)
            USING {userid}, {itemid}, {rating};
        END $$;
        """
        
        cur.execute(query)
        con.commit()
        
    except Exception as e:
        con.rollback()
        raise e
    finally:
        cur.close()

def rangeinsert(ratingstablename, userid, itemid, rating, openconnection):
    """
    Function to insert a new row into the main table and specific partition based on range rating.
    Sử dụng truy vấn SQL để xác định phân mảnh và insert dữ liệu
    
    Parameters:
    -----------
    ratingstablename : str
        Tên bảng ratings
    userid : int
        ID của user
    itemid : int
        ID của movie
    rating : float
        Giá trị rating
    openconnection : psycopg2.extensions.connection
        Kết nối đến database
        
    Notes:
    -----
    - Xác định bảng con dựa trên giá trị rating
    - Insert vào bảng con tương ứng
    """
    con = openconnection
    cur = con.cursor()
    RANGE_TABLE_PREFIX = 'range_part'
    numberofpartitions = count_partitions(RANGE_TABLE_PREFIX, openconnection)
    delta = 5.0 / numberofpartitions
    
    # Tạo truy vấn SQL để xác định phân mảnh và insert dữ liệu
    query = f"""
    DO $$
    DECLARE
        target_partition text;
    BEGIN
        WITH partition_ranges AS (
            SELECT 
                generate_series(0, {numberofpartitions-1}) as partition_index,
                generate_series(0, {numberofpartitions-1}) * {delta} as min_range,
                (generate_series(0, {numberofpartitions-1}) + 1) * {delta} as max_range
        )
        SELECT '{RANGE_TABLE_PREFIX}' || partition_index::text INTO target_partition
        FROM partition_ranges
        WHERE 
            CASE 
                WHEN partition_index = 0 THEN {rating} >= min_range AND {rating} <= max_range
                ELSE {rating} > min_range AND {rating} <= max_range
            END
        LIMIT 1;

        EXECUTE format('INSERT INTO %I (userid, movieid, rating) VALUES ($1, $2, $3)', target_partition)
        USING {userid}, {itemid}, {rating};
    END $$;
    """
    
    cur.execute(query)
    cur.close()
    con.commit()

def create_db(dbname):
    """
    We create a DB by connecting to the default user and database of Postgres
    The function first checks if an existing database exists for a given name, else creates it.
    
    Parameters:
    -----------
    dbname : str
        Tên database cần tạo
        
    Notes:
    -----
    - Kết nối đến database mặc định
    - Tạo database mới với tên được chỉ định
    - Đóng kết nối
    """
    # Connect to the default database
    con = getopenconnection(dbname='postgres')
    con.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    cur = con.cursor()
    
    # Check if an existing database with the same name exists
    cur.execute('SELECT COUNT(*) FROM pg_catalog.pg_database WHERE datname=\'%s\'' % (dbname,))
    count = cur.fetchone()[0]
    if count == 0:
        cur.execute('CREATE DATABASE %s' % (dbname,))  # Create the database
        print('Đã tạo cơ sở dữ liệu mới: {0}'.format(dbname))
    else:
        print('Cơ sở dữ liệu {0} đã tồn tại'.format(dbname))

    # Clean up
    cur.close()
    con.close()

def count_partitions(prefix, openconnection):
    """
    Function to count the number of tables which have the @prefix in their name somewhere.
    
    Parameters:
    -----------
    prefix : str
        Prefix của tên bảng cần đếm
    openconnection : psycopg2.extensions.connection
        Kết nối đến database
        
    Returns:
    --------
    int
        Số lượng bảng con có prefix được chỉ định
        
    Notes:
    -----
    - Truy vấn pg_stat_user_tables để đếm số bảng
    - Chỉ đếm các bảng có tên bắt đầu bằng prefix
    """
    con = openconnection
    cur = con.cursor()
    cur.execute("select count(*) from pg_stat_user_tables where relname like " + "'" + prefix + "%';")
    count = cur.fetchone()[0]
    cur.close()
    
    return count
