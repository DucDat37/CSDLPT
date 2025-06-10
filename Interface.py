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
    Hàm tải dữ liệu ratings từ file vào database PostgreSQL.
    
    Hàm này thực hiện các bước sau:
    1. Tạo bảng tạm thời với cấu trúc đầy đủ để phù hợp với định dạng file input
    2. Copy dữ liệu trực tiếp từ file vào bảng sử dụng psycopg2.copy_from
    3. Dọn dẹp bảng bằng cách xóa các cột phụ không cần thiết
    4. Thêm primary key composite cho cặp (userid, movieid)
    
    Parameters:
    -----------
    ratingstablename : str
        Tên bảng sẽ được tạo để lưu trữ dữ liệu ratings
    ratingsfilepath : str
        Đường dẫn đến file chứa dữ liệu ratings
        File input phải có định dạng: userid:extra1:movieid:extra2:rating:extra3:timestamp
        với dấu ':' làm separator
    openconnection : psycopg2.extensions.connection
        Kết nối đến database PostgreSQL
        
    Returns:
    --------
    None
    
    Raises:
    -------
    Exception
        - Nếu không thể tạo bảng
        - Nếu không thể đọc file
        - Nếu không thể copy dữ liệu
        - Nếu không thể thêm primary key
        
    Notes:
    -----
    - File input phải có định dạng cố định với 7 cột phân cách bằng dấu ':'
    - Các cột phụ (extra1, extra2, extra3, timestamp) sẽ bị xóa sau khi import
    - Bảng kết quả chỉ chứa 3 cột: userid, movieid, rating
    - Primary key được đặt trên cặp (userid, movieid) để đảm bảo tính duy nhất
    - Hàm sử dụng transaction để đảm bảo tính toàn vẹn dữ liệu
    - Nếu có lỗi xảy ra, toàn bộ transaction sẽ được rollback
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
    Hàm phân vùng dữ liệu ratings theo khoảng giá trị (range partitioning).
    
    Hàm này thực hiện phân vùng dữ liệu ratings thành các bảng nhỏ hơn dựa trên giá trị rating,
    với mỗi phân vùng chứa các bản ghi có rating nằm trong một khoảng giá trị cụ thể.
    
    Cách phân vùng:
    - Khoảng giá trị rating tổng thể là [0, 5]
    - Khoảng giá trị được chia đều thành numberofpartitions phần
    - Phân vùng đầu tiên bao gồm cả giá trị min: [min_range, max_range]
    - Các phân vùng còn lại không bao gồm giá trị min: (min_range, max_range]
    
    Ví dụ với numberofpartitions = 5:
    - range_part0: ratings từ 0.0 đến 1.0
    - range_part1: ratings từ 1.0 đến 2.0
    - range_part2: ratings từ 2.0 đến 3.0
    - range_part3: ratings từ 3.0 đến 4.0
    - range_part4: ratings từ 4.0 đến 5.0
    
    Parameters:
    -----------
    ratingstablename : str
        Tên bảng chứa dữ liệu ratings gốc cần được phân vùng
    numberofpartitions : int
        Số lượng phân vùng cần tạo
        Lưu ý: số này phải > 0 và nên được chọn phù hợp với phân phối dữ liệu
    openconnection : psycopg2.extensions.connection
        Kết nối đến database PostgreSQL
        
    Returns:
    --------
    None
    
    Raises:
    -------
    Exception
        - Nếu không thể tạo các bảng phân vùng
        - Nếu không thể chèn dữ liệu vào các phân vùng
        - Nếu numberofpartitions <= 0
        - Nếu bảng ratingstablename không tồn tại
        
    Notes:
    -----
    - Mỗi phân vùng được tạo thành một bảng riêng biệt với tên format: range_part{index}
    - Cấu trúc mỗi bảng phân vùng giống nhau: (userid, movieid, rating)
    - Phân vùng theo range giúp tối ưu hiệu suất truy vấn khi cần lấy dữ liệu trong một khoảng rating cụ thể
    - Hàm sử dụng transaction để đảm bảo tính toàn vẹn dữ liệu
    - Nếu có lỗi xảy ra, toàn bộ transaction sẽ được rollback
    - Thời gian thực thi của hàm được tính và in ra
    """
   
    try:
        # Bắt đầu tính thời gian thực thi
        start_time = time.time()
        
        con = openconnection
        cur = con.cursor()
        
        # Bước 1: Tính toán khoảng giá trị cho mỗi phân vùng
        # Ví dụ: nếu numberofpartitions = 5, delta = 1.0
        # Tạo ra các khoảng: [0,1], (1,2], (2,3], (3,4], (4,5]
        delta = 5.0 / numberofpartitions
        
        # Bước 2: Tạo và phân phối dữ liệu cho từng phân vùng
        for i in range(numberofpartitions):
            # Tính toán ranh giới của phân vùng hiện tại
            min_range = i * delta  # Giá trị nhỏ nhất của phân vùng
            max_range = min_range + delta  # Giá trị lớn nhất của phân vùng
            table_name = RANGE_TABLE_PREFIX + str(i)  # Tên bảng phân vùng: range_part0, range_part1,...
            
            # Bước 2.1: Tạo bảng phân vùng với cấu trúc cố định
            # Mỗi bảng có 3 cột: userid, movieid, rating
            cur.execute(f"""
                CREATE TABLE {table_name} (
                    userid INTEGER,
                    movieid INTEGER,
                    rating FLOAT
                )
            """)
            
            # Bước 2.2: Phân phối dữ liệu vào phân vùng dựa trên giá trị rating
            if i == 0:
                # Phân vùng đầu tiên (i=0):
                # - Bao gồm cả giá trị min_range
                # - Ví dụ: [0.0, 1.0] nếu delta = 1.0
                cur.execute(f"""
                    INSERT INTO {table_name} (userid, movieid, rating)
                    SELECT userid, movieid, rating
                    FROM {ratingstablename}
                    WHERE rating >= {min_range} AND rating <= {max_range}
                """)
            else:
                # Các phân vùng còn lại (i > 0):
                # - Không bao gồm giá trị min_range
                # - Ví dụ: (1.0, 2.0], (2.0, 3.0],...
                cur.execute(f"""
                    INSERT INTO {table_name} (userid, movieid, rating)
                    SELECT userid, movieid, rating
                    FROM {ratingstablename}
                    WHERE rating > {min_range} AND rating <= {max_range}
                """)
        
        # Bước 3: Hoàn tất transaction
        # Commit các thay đổi vào database
        openconnection.commit()
        cur.close()
        
        # Bước 4: Tính và in thời gian thực thi
        end_time = time.time()
        execution_time = end_time - start_time
        print(f"Thời gian thực thi hàm rangepartition: {execution_time:.2f} giây")
        
    except Exception as e:
        # Bước 5: Xử lý lỗi
        # Nếu có bất kỳ lỗi nào xảy ra trong quá trình thực thi:
        # 1. Rollback toàn bộ transaction để đảm bảo tính toàn vẹn dữ liệu
        # 2. In thông báo lỗi
        # 3. Ném ngoại lệ để thông báo cho người gọi hàm
        openconnection.rollback()
        print("Error: Could not create range partitions")
        print(e)
        raise e

def roundrobinpartition(ratingstablename, numberofpartitions, openconnection):
    """
    Hàm phân vùng dữ liệu ratings theo phương pháp Round Robin.
    
    Hàm này thực hiện phân vùng dữ liệu ratings thành các bảng nhỏ hơn bằng cách phân phối
    các bản ghi luân phiên (round robin) vào các phân vùng. Mỗi bản ghi sẽ được gán vào
    một phân vùng theo thứ tự tuần tự, và sau khi gán đến phân vùng cuối cùng, quá trình
    sẽ quay lại phân vùng đầu tiên.
    
    Cách phân vùng:
    - Dữ liệu được đọc tuần tự từ bảng gốc
    - Mỗi bản ghi được gán một số thứ tự (row_num) bắt đầu từ 0
    - Phân vùng được chọn dựa trên công thức: row_num % numberofpartitions
    - Điều này đảm bảo dữ liệu được phân phối đều giữa các phân vùng
    
    Ví dụ với numberofpartitions = 3:
    - Bản ghi 0 -> range_part0 (0 % 3 = 0)
    - Bản ghi 1 -> range_part1 (1 % 3 = 1)
    - Bản ghi 2 -> range_part2 (2 % 3 = 2)
    - Bản ghi 3 -> range_part0 (3 % 3 = 0)
    - Bản ghi 4 -> range_part1 (4 % 3 = 1)
    - và cứ tiếp tục như vậy...
    
    Parameters:
    -----------
    ratingstablename : str
        Tên bảng chứa dữ liệu ratings gốc cần được phân vùng
    numberofpartitions : int
        Số lượng phân vùng cần tạo
        Lưu ý: số này phải > 0 và nên được chọn để đảm bảo kích thước phân vùng phù hợp
    openconnection : psycopg2.extensions.connection
        Kết nối đến database PostgreSQL
        
    Returns:
    --------
    None
    
    Raises:
    -------
    Exception
        - Nếu không thể tạo các bảng phân vùng
        - Nếu không thể chèn dữ liệu vào các phân vùng
        - Nếu numberofpartitions <= 0
        - Nếu bảng ratingstablename không tồn tại
        
    Notes:
    -----
    - Mỗi phân vùng được tạo thành một bảng riêng biệt với tên format: rrobin_part{index}
    - Cấu trúc mỗi bảng phân vùng giống nhau: (userid, movieid, rating)
    - Phân vùng Round Robin đảm bảo dữ liệu được phân phối đều giữa các phân vùng
    - Phương pháp này phù hợp khi:
        + Cần cân bằng tải giữa các phân vùng
        + Không có yêu cầu cụ thể về việc nhóm dữ liệu theo giá trị
        + Cần truy vấn ngẫu nhiên trên toàn bộ dữ liệu
    - Hàm sử dụng transaction để đảm bảo tính toàn vẹn dữ liệu
    - Nếu có lỗi xảy ra, toàn bộ transaction sẽ được rollback
    - Thời gian thực thi của hàm được tính và in ra
    - Hàm sử dụng PL/pgSQL để thực hiện phân phối dữ liệu hiệu quả
    """
   
    try:
        # Bắt đầu tính thời gian thực thi
        start_time = time.time()
        
        con = openconnection
        cur = con.cursor()
        RROBIN_TABLE_PREFIX = 'rrobin_part'

        # Bước 1: Tạo các bảng phân vùng
        # Mỗi phân vùng sẽ là một bảng riêng biệt với cấu trúc giống nhau
        # Ví dụ: rrobin_part0, rrobin_part1, rrobin_part2,...
        for i in range(numberofpartitions):
            table_name = RROBIN_TABLE_PREFIX + str(i)
            # Tạo bảng với 3 cột: userid, movieid, rating
            cur.execute(f"""
                CREATE TABLE {table_name} (
                    userid INTEGER,
                    movieid INTEGER,
                    rating FLOAT
                )
            """)
        
        # Bước 2: Phân phối dữ liệu theo Round Robin
        # Sử dụng PL/pgSQL để thực hiện phân phối dữ liệu hiệu quả
        # Cách hoạt động:
        # 1. Đọc dữ liệu từ bảng gốc và gán số thứ tự cho mỗi bản ghi
        # 2. Tính toán phân vùng đích dựa trên công thức: row_num % numberofpartitions
        # 3. Chèn dữ liệu vào phân vùng tương ứng
        query = f"""
        DO $$
        DECLARE
            target_partition text;  -- Tên bảng phân vùng đích
            row_data record;        -- Bản ghi hiện tại đang xử lý
        BEGIN
            -- Duyệt qua từng bản ghi trong bảng gốc
            FOR row_data IN 
                SELECT 
                    userid,
                    movieid,
                    rating,
                    -- Gán số thứ tự cho mỗi bản ghi, bắt đầu từ 0
                    ROW_NUMBER() OVER (ORDER BY userid, movieid) - 1 as row_num
                FROM {ratingstablename}
            LOOP
                -- Bước 2.1: Tính toán tên bảng phân vùng đích
                -- Công thức: rrobin_part + (row_num % numberofpartitions)
                -- Ví dụ: nếu row_num = 5 và numberofpartitions = 3
                -- target_partition = rrobin_part2 (vì 5 % 3 = 2)
                target_partition := '{RROBIN_TABLE_PREFIX}' || (row_data.row_num % {numberofpartitions})::text;
                
                -- Bước 2.2: Chèn dữ liệu vào phân vùng tương ứng
                -- Sử dụng EXECUTE format để tạo câu lệnh SQL động
                -- %I được sử dụng để escape tên bảng một cách an toàn
                EXECUTE format('INSERT INTO %I (userid, movieid, rating) VALUES ($1, $2, $3)', target_partition)
                USING row_data.userid, row_data.movieid, row_data.rating;
            END LOOP;
        END $$;
        """
        
        # Thực thi câu lệnh PL/pgSQL để phân phối dữ liệu
        cur.execute(query)
        
        # Bước 3: Hoàn tất transaction
        # Commit các thay đổi vào database
        openconnection.commit()
        cur.close()
        
        # Bước 4: Tính và in thời gian thực thi
        end_time = time.time()
        execution_time = end_time - start_time
        print(f"Thời gian thực thi hàm roundrobinpartition: {execution_time:.2f} giây")
        
    except Exception as e:
        # Bước 5: Xử lý lỗi
        # Nếu có bất kỳ lỗi nào xảy ra trong quá trình thực thi:
        # 1. Rollback toàn bộ transaction để đảm bảo tính toàn vẹn dữ liệu
        # 2. In thông báo lỗi
        # 3. Ném ngoại lệ để thông báo cho người gọi hàm
        openconnection.rollback()
        print("Error: Could not create round robin partitions")
        print(e)
        raise e

def roundrobininsert(ratingstablename, userid, itemid, rating, openconnection):
    """
    Hàm chèn một bản ghi rating mới vào hệ thống phân vùng Round Robin.
    
    Hàm này thực hiện việc chèn một bản ghi rating mới vào cả bảng gốc và phân vùng tương ứng
    theo phương pháp Round Robin. Quá trình chèn được thực hiện theo các bước:
    1. Chèn dữ liệu vào bảng gốc (ratingstablename)
    2. Xác định phân vùng đích dựa trên tổng số bản ghi sau khi chèn
    3. Chèn dữ liệu vào phân vùng tương ứng
    
    Cách xác định phân vùng:
    - Đếm tổng số bản ghi trong bảng gốc sau khi chèn
    - Phân vùng được chọn = (tổng số bản ghi - 1) % số phân vùng
    - Ví dụ: nếu có 5 phân vùng và tổng số bản ghi là 7
      -> Phân vùng đích = (7-1) % 5 = 1 (rrobin_part1)
    
    Parameters:
    -----------
    ratingstablename : str
        Tên bảng chứa dữ liệu ratings gốc
    userid : int
        ID của người dùng
    itemid : int
        ID của phim
    rating : float
        Giá trị rating (từ 0 đến 5)
    openconnection : psycopg2.extensions.connection
        Kết nối đến database PostgreSQL
        
    Returns:
    --------
    None
    
    Raises:
    -------
    Exception
        - Nếu không thể chèn dữ liệu vào bảng gốc
        - Nếu không thể chèn dữ liệu vào phân vùng
        - Nếu bảng gốc hoặc phân vùng không tồn tại
        - Nếu dữ liệu đầu vào không hợp lệ (ví dụ: rating ngoài khoảng [0,5])
        
    Notes:
    -----
    - Hàm đảm bảo dữ liệu được chèn vào cả bảng gốc và phân vùng tương ứng
    - Phân vùng được chọn dựa trên vị trí của bản ghi mới trong chuỗi Round Robin
    - Hàm sử dụng transaction để đảm bảo tính toàn vẹn dữ liệu:
        + Nếu chèn vào bảng gốc thành công nhưng chèn vào phân vùng thất bại -> rollback
        + Nếu chèn vào phân vùng thành công nhưng commit thất bại -> rollback
    - Hàm giả định rằng các phân vùng đã được tạo trước đó bằng hàm roundrobinpartition
    - Số lượng phân vùng được xác định tự động bằng cách đếm các bảng có prefix 'rrobin_part'
    - Hàm sử dụng PL/pgSQL để thực hiện chèn dữ liệu vào phân vùng một cách an toàn
    - Cần đảm bảo rằng userid và itemid không trùng lặp trong bảng gốc (primary key)
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
    Hàm chèn một bản ghi rating mới vào hệ thống phân vùng theo khoảng giá trị (Range Partitioning).
    
    Hàm này thực hiện việc chèn một bản ghi rating mới vào cả bảng gốc và phân vùng tương ứng
    dựa trên giá trị rating. Phân vùng được chọn dựa trên khoảng giá trị mà rating thuộc vào.
    
    Cách xác định phân vùng:
    - Khoảng giá trị tổng thể của rating là [0, 5]
    - Khoảng này được chia đều thành N phần (N = số phân vùng)
    - Phân vùng đầu tiên: [min_range, max_range]
    - Các phân vùng còn lại: (min_range, max_range]
    - Ví dụ với 5 phân vùng:
        + range_part0: [0.0, 1.0]
        + range_part1: (1.0, 2.0]
        + range_part2: (2.0, 3.0]
        + range_part3: (3.0, 4.0]
        + range_part4: (4.0, 5.0]
    
    Parameters:
    -----------
    ratingstablename : str
        Tên bảng chứa dữ liệu ratings gốc
    userid : int
        ID của người dùng
    itemid : int
        ID của phim
    rating : float
        Giá trị rating (từ 0 đến 5)
        Lưu ý: giá trị này quyết định phân vùng sẽ chèn vào
    openconnection : psycopg2.extensions.connection
        Kết nối đến database PostgreSQL
        
    Returns:
    --------
    None
    
    Raises:
    -------
    Exception
        - Nếu không thể chèn dữ liệu vào bảng gốc
        - Nếu không thể chèn dữ liệu vào phân vùng
        - Nếu bảng gốc hoặc phân vùng không tồn tại
        - Nếu rating ngoài khoảng [0,5]
        - Nếu không tìm thấy phân vùng phù hợp cho giá trị rating
        
    Notes:
    -----
    - Hàm đảm bảo dữ liệu được chèn vào cả bảng gốc và phân vùng tương ứng
    - Phân vùng được chọn dựa trên giá trị rating của bản ghi
    - Hàm sử dụng transaction để đảm bảo tính toàn vẹn dữ liệu:
        + Nếu chèn vào bảng gốc thành công nhưng chèn vào phân vùng thất bại -> rollback
        + Nếu chèn vào phân vùng thành công nhưng commit thất bại -> rollback
    - Hàm giả định rằng các phân vùng đã được tạo trước đó bằng hàm rangepartition
    - Số lượng phân vùng được xác định tự động bằng cách đếm các bảng có prefix 'range_part'
    - Hàm sử dụng PL/pgSQL để thực hiện chèn dữ liệu vào phân vùng một cách an toàn
    - Cần đảm bảo rằng userid và itemid không trùng lặp trong bảng gốc (primary key)
    - Việc chèn dữ liệu vào phân vùng phù hợp giúp duy trì tính nhất quán của hệ thống phân vùng
    - Hàm tự động xử lý các trường hợp đặc biệt như rating = 0 (phân vùng đầu tiên)
    """
   
    con = openconnection
    cur = con.cursor()
    RANGE_TABLE_PREFIX = 'range_part'
    # Bước 1: Xác định số lượng phân vùng hiện có
    numberofpartitions = count_partitions(RANGE_TABLE_PREFIX, openconnection)
    # Bước 2: Tính độ rộng của mỗi phân vùng
    # Ví dụ: với 5 phân vùng, delta = 1.0
    # Tạo ra các khoảng: [0,1], (1,2], (2,3], (3,4], (4,5]
    delta = 5.0 / numberofpartitions
    
    # Bước 3: Tạo và thực thi câu lệnh PL/pgSQL để chèn dữ liệu
    # Sử dụng PL/pgSQL để thực hiện chèn dữ liệu một cách an toàn và hiệu quả
    query = f"""
    DO $$
    DECLARE
        target_partition text;  -- Biến lưu tên phân vùng đích
    BEGIN
        -- Bước 3.1: Tạo bảng tạm chứa thông tin về các phân vùng
        -- Mỗi dòng chứa: index phân vùng, giá trị min, giá trị max
        WITH partition_ranges AS (
            SELECT 
                generate_series(0, {numberofpartitions-1}) as partition_index,  -- Tạo dãy số từ 0 đến numberofpartitions-1
                generate_series(0, {numberofpartitions-1}) * {delta} as min_range,  -- Tính giá trị min của mỗi phân vùng
                (generate_series(0, {numberofpartitions-1}) + 1) * {delta} as max_range  -- Tính giá trị max của mỗi phân vùng
        )
        -- Bước 3.2: Tìm phân vùng phù hợp cho giá trị rating
        -- Sử dụng CASE để xử lý đặc biệt cho phân vùng đầu tiên
        SELECT '{RANGE_TABLE_PREFIX}' || partition_index::text INTO target_partition
        FROM partition_ranges
        WHERE 
            CASE 
                -- Phân vùng đầu tiên (index = 0): bao gồm cả giá trị min
                -- Ví dụ: [0.0, 1.0] nếu delta = 1.0
                WHEN partition_index = 0 THEN {rating} >= min_range AND {rating} <= max_range
                -- Các phân vùng còn lại: không bao gồm giá trị min
                -- Ví dụ: (1.0, 2.0], (2.0, 3.0],...
                ELSE {rating} > min_range AND {rating} <= max_range
            END
        LIMIT 1;  -- Chỉ lấy phân vùng đầu tiên thỏa mãn điều kiện

        -- Bước 3.3: Chèn dữ liệu vào phân vùng đã xác định
        -- Sử dụng EXECUTE format để tạo câu lệnh SQL động
        -- %I được sử dụng để escape tên bảng một cách an toàn
        EXECUTE format('INSERT INTO %I (userid, movieid, rating) VALUES ($1, $2, $3)', target_partition)
        USING {userid}, {itemid}, {rating};
    END $$;
    """
    
    # Bước 4: Thực thi câu lệnh PL/pgSQL
    cur.execute(query)
    cur.close()
    # Bước 5: Commit transaction để lưu thay đổi
    con.commit()

def create_db(dbname):
   
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
   
    con = openconnection
    cur = con.cursor()
    cur.execute("select count(*) from pg_stat_user_tables where relname like " + "'" + prefix + "%';")
    count = cur.fetchone()[0]
    cur.close()
    
    return count
