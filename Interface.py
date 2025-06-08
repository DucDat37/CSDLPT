#!/usr/bin/python2.7
#
# Interface for the assignement
#

import psycopg2

# Các hằng số cho tên bảng và cột
RANGE_TABLE_PREFIX = 'range_part'
RROBIN_TABLE_PREFIX = 'rrobin_part'
USER_ID_COLNAME = 'userid'
MOVIE_ID_COLNAME = 'movieid'
RATING_COLNAME = 'rating'

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
    Hàm đọc dữ liệu từ file và load vào bảng ratings trong database
    
    Parameters:
    -----------
    ratingstablename : str
        Tên bảng ratings trong database
    ratingsfilepath : str
        Đường dẫn đến file chứa dữ liệu ratings
    openconnection : psycopg2.extensions.connection
        Kết nối đến database
        
    Returns:
    --------
    None
    
    Raises:
    -------
    Exception
        - Nếu không thể mở file
        - Nếu file không đúng định dạng
        - Nếu không thể insert dữ liệu vào database
        
    Notes:
    -----
    - File dữ liệu phải có định dạng: userid::movieid::rating::timestamp
    - Mỗi dòng trong file là một bản ghi ratings
    - Hàm sẽ tạo bảng ratings nếu chưa tồn tại
    - Bảng ratings có cấu trúc:
        + userid: integer (khóa chính)
        + movieid: integer (khóa chính)
        + rating: float
    - Hàm sử dụng COPY để load dữ liệu nhanh hơn so với INSERT
    """
    try:
        # Tạo cursor để thực thi các câu lệnh SQL
        cur = openconnection.cursor()
        
        # Tạo bảng ratings nếu chưa tồn tại
        create_table_query = f"""
        CREATE TABLE IF NOT EXISTS {ratingstablename} (
            {USER_ID_COLNAME} INTEGER,
            {MOVIE_ID_COLNAME} INTEGER,
            {RATING_COLNAME} FLOAT,
            PRIMARY KEY ({USER_ID_COLNAME}, {MOVIE_ID_COLNAME})
        )
        """
        cur.execute(create_table_query)
        
        # Mở file dữ liệu
        with open(ratingsfilepath, 'r') as ratingsfile:
            # Đọc từng dòng trong file
            for line in ratingsfile:
                # Tách các trường dữ liệu theo dấu ::
                userid, movieid, rating, timestamp = line.strip().split('::')
                
                # Insert dữ liệu vào bảng ratings
                insert_query = f"""
                INSERT INTO {ratingstablename} ({USER_ID_COLNAME}, {MOVIE_ID_COLNAME}, {RATING_COLNAME})
                VALUES (%s, %s, %s)
                ON CONFLICT ({USER_ID_COLNAME}, {MOVIE_ID_COLNAME}) DO NOTHING
                """
                cur.execute(insert_query, (int(userid), int(movieid), float(rating)))
        
        # Commit các thay đổi vào database
        openconnection.commit()
        
    except Exception as e:
        # Rollback nếu có lỗi xảy ra
        openconnection.rollback()
        print("Error: Could not load ratings from file")
        print(e)
        raise e
    finally:
        # Đóng cursor
        cur.close()

def rangepartition(ratingstablename, numberofpartitions, openconnection):
    """
    Function to create partitions of main table based on range of ratings.
    Sử dụng truy vấn SQL để phân mảnh dựa trên khoảng giá trị của rating
    """
    con = openconnection
    cur = con.cursor()
    
    # Tính khoảng giá trị cho mỗi phân mảnh
    delta = 5.0 / numberofpartitions
    
    # Tạo các bảng phân mảnh
    for i in range(numberofpartitions):
        min_range = i * delta
        max_range = min_range + delta
        table_name = RANGE_TABLE_PREFIX + str(i)
        
        # Tạo bảng phân mảnh
        cur.execute(f"""
            CREATE TABLE {table_name} (
                userid INTEGER,
                movieid INTEGER,
                rating FLOAT
            )
        """)
        
        # Chèn dữ liệu vào bảng phân mảnh dựa trên khoảng giá trị
        if i == 0:
            # Phân mảnh đầu tiên: [min_range, max_range]
            cur.execute(f"""
                INSERT INTO {table_name} (userid, movieid, rating)
                SELECT userid, movieid, rating
                FROM {ratingstablename}
                WHERE rating >= {min_range} AND rating <= {max_range}
            """)
        else:
            # Các phân mảnh còn lại: (min_range, max_range]
            cur.execute(f"""
                INSERT INTO {table_name} (userid, movieid, rating)
                SELECT userid, movieid, rating
                FROM {ratingstablename}
                WHERE rating > {min_range} AND rating <= {max_range}
            """)
    
    cur.close()
    con.commit()

def roundrobinpartition(ratingstablename, numberofpartitions, openconnection):
    """
    Function to create partitions of main table using round robin approach.
    Sử dụng truy vấn SQL để phân mảnh dữ liệu theo round robin
    """
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
    cur.close()
    con.commit()

def roundrobininsert(ratingstablename, userid, itemid, rating, openconnection):
    """
    Function to insert a new row into the main table and specific partition based on round robin
    approach.
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
    :param dbname: Name of the database to create
    :return: None
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
    """
    con = openconnection
    cur = con.cursor()
    cur.execute("select count(*) from pg_stat_user_tables where relname like " + "'" + prefix + "%';")
    count = cur.fetchone()[0]
    cur.close()

    return count
