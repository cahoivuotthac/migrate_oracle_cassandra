from connect_2_clusters import connect_to_cluster
from datetime import datetime
import os

def clear_screen():
    os.system('cls' if os.name == 'nt' else 'clear')

def execute_query(session, query, params=None):
    """Execute a query and display results"""
    try:
        if params:
            rows = session.execute(query, params)
        else:
            rows = session.execute(query)
        
        if not rows:
            print("No results found")
            return
        
        # Print column headers
        first_row = rows.one()
        if first_row:
            headers = first_row._fields
            print("\n" + " | ".join(headers))
            print("-" * (len(" | ".join(headers)) + 10))
            
            # Print the first row
            print(" | ".join(str(getattr(first_row, field)) for field in headers))
            
            # Print remaining rows
            for row in rows:
                print(" | ".join(str(getattr(row, field)) for field in headers))
    
    except Exception as e:
        print(f"Error executing query: {e}")

def get_date_input(prompt):
    """Get a valid date input from user"""
    while True:
        date_str = input(prompt + " (YYYY-MM-DD): ")
        try:
            # Validate the date format
            datetime.strptime(date_str, '%Y-%m-%d')
            return date_str
        except ValueError:
            print("Invalid date format. Please use YYYY-MM-DD.")

def choose_database():
    """Let user choose which database to connect to"""
    while True:
        clear_screen()
        print("\n===== CHỌN CƠ SỞ DỮ LIỆU =====")
        print("1. Local Cassandra (127.0.0.1)")
        print("2. Remote Cassandra (26.103.246.194)")
        print("0. Thoát")
        
        choice = input("\nChọn cơ sở dữ liệu: ")
        
        if choice == "1":
            return '127.0.0.1'
        elif choice == "2":
            return '26.103.246.194'
        elif choice == "0":
            return None
        else:
            print("Lựa chọn không hợp lệ! Vui lòng thử lại.")
            input("\nNhấn Enter để tiếp tục...")

def chi_tiet_hoa_don_menu(session):
    while True:
        clear_screen()
        print("\n=== Chi tiết hóa đơn theo mã khách hàng ===")
        
        ma_kh = input("Nhập mã khách hàng (ví dụ: KH001): ")
        
        print("\nChọn loại truy vấn:")
        print("1. Tất cả hóa đơn của khách hàng")
        print("2. Hóa đơn trong khoảng thời gian")
        print("0. Quay lại")
        
        choice = input("Lựa chọn: ")
        
        if choice == "1":
            query = """
            SELECT * FROM chi_tiet_hoa_don_theo_ma_kh
            WHERE ma_khach_hang = %s
            """
            execute_query(session, query, [int(ma_kh)])
            input("\nNhấn Enter để tiếp tục...")
        
        elif choice == "2":
            start_date = get_date_input("Ngày bắt đầu")
            end_date = get_date_input("Ngày kết thúc")
            
            query = """
            SELECT * FROM chi_tiet_hoa_don_theo_ma_kh
            WHERE ma_khach_hang = %s
            AND ngay_tao >= %s AND ngay_tao <= %s
            """
            execute_query(session, query, [int(ma_kh), start_date, end_date])
            input("\nNhấn Enter để tiếp tục...")
        
        elif choice == "0":
            break
        
        else:
            print("Lựa chọn không hợp lệ! Vui lòng thử lại.")
            input("\nNhấn Enter để tiếp tục...")

def doanh_thu_menu(session):
    while True:
        clear_screen()
        print("\n=== Doanh thu mỗi ngày theo mã chi nhánh ===")
        
        ma_cn = int(input("Nhập mã chi nhánh (ví dụ: 1): "))
        
        print("\nChọn loại truy vấn:")
        print("1. Doanh thu tất cả các ngày")
        print("2. Doanh thu theo ngày cụ thể")
        print("0. Quay lại")
        
        choice = input("Lựa chọn: ")
        
        if choice == "1":
            query = """
            SELECT * FROM doanh_thu_moi_ngay_theo_ma_cn
            WHERE ma_chi_nhanh = %s
            """
            execute_query(session, query, [ma_cn])
            input("\nNhấn Enter để tiếp tục...")
        
        elif choice == "2":
            date = get_date_input("Nhập ngày")
            
            query = """
            SELECT * FROM doanh_thu_moi_ngay_theo_ma_cn
            WHERE ma_chi_nhanh = %s AND ngay = %s
            """
            execute_query(session, query, [ma_cn, date])
            input("\nNhấn Enter để tiếp tục...")
        
        elif choice == "0":
            break
        
        else:
            print("Lựa chọn không hợp lệ! Vui lòng thử lại.")
            input("\nNhấn Enter để tiếp tục...")

def kho_sp_menu(session):
    while True:
        clear_screen()
        print("\n=== Kho sản phẩm theo mã chi nhánh ===")
        
        ma_cn = int(input("Nhập mã chi nhánh (ví dụ: 1): "))
        
        print("\nChọn loại truy vấn:")
        print("1. Tất cả sản phẩm của chi nhánh")
        print("2. Thông tin sản phẩm cụ thể tại chi nhánh")
        print("0. Quay lại")
        
        choice = input("Lựa chọn: ")
        
        if choice == "1":
            query = """
            SELECT * FROM kho_sp_theo_ma_cn
            WHERE ma_chi_nhanh = %s
            """
            execute_query(session, query, [ma_cn])
            input("\nNhấn Enter để tiếp tục...")
        
        elif choice == "2":
            ma_sp = input("Nhập mã sản phẩm (ví dụ: SP001): ")
            
            query = """
            SELECT * FROM kho_sp_theo_ma_cn
            WHERE ma_chi_nhanh = %s AND ma_san_pham = %s
            """
            execute_query(session, query, [ma_cn, ma_sp])
            input("\nNhấn Enter để tiếp tục...")
        
        elif choice == "0":
            break
        
        else:
            print("Lựa chọn không hợp lệ! Vui lòng thử lại.")
            input("\nNhấn Enter để tiếp tục...")

def khach_hang_menu(session):
    while True:
        clear_screen()
        print("\n=== Số lượng khách hàng mỗi ngày theo mã chi nhánh ===")
        
        ma_cn = int(input("Nhập mã chi nhánh (ví dụ: 1): "))
        
        print("\nChọn loại truy vấn:")
        print("1. Số khách hàng tất cả các ngày")
        print("2. Số khách hàng theo ngày cụ thể")
        print("0. Quay lại")
        
        choice = input("Lựa chọn: ")
        
        if choice == "1":
            query = """
            SELECT * FROM sl_khach_hang_moi_ngay_theo_ma_cn
            WHERE ma_chi_nhanh = %s
            """
            execute_query(session, query, [ma_cn])
            input("\nNhấn Enter để tiếp tục...")
        
        elif choice == "2":
            date = get_date_input("Nhập ngày")
            
            query = """
            SELECT * FROM sl_khach_hang_moi_ngay_theo_ma_cn
            WHERE ma_chi_nhanh = %s AND ngay = %s
            """
            execute_query(session, query, [ma_cn, date])
            input("\nNhấn Enter để tiếp tục...")
        
        elif choice == "0":
            break
        
        else:
            print("Lựa chọn không hợp lệ! Vui lòng thử lại.")
            input("\nNhấn Enter để tiếp tục...")

def main():
    keyspace_name = 'btl2_data'
    cluster = None
    
    # Let user choose database
    cluster_ip = choose_database()
    if cluster_ip is None:
        print("Thoát chương trình.")
        return
    
    try:
        print(f"\nĐang kết nối tới {cluster_ip}...")
        cluster, session = connect_to_cluster(cluster_ip, keyspace_name)
        
        while True:
            clear_screen()
            print(f"\n===== CASSANDRA QUERY MENU ({cluster_ip}) =====")
            print("1. Chi tiết hóa đơn theo mã khách hàng")
            print("2. Doanh thu mỗi ngày theo mã chi nhánh")
            print("3. Kho sản phẩm theo mã chi nhánh")
            print("4. Số lượng khách hàng mỗi ngày theo mã chi nhánh")
            print("5. Chuyển đổi cơ sở dữ liệu")
            print("0. Thoát")
            
            choice = input("\nChọn chức năng: ")
            
            if choice == "1":
                chi_tiet_hoa_don_menu(session)
            elif choice == "2":
                doanh_thu_menu(session)
            elif choice == "3":
                kho_sp_menu(session)
            elif choice == "4":
                khach_hang_menu(session)
            elif choice == "5":
                # Close current connection
                if cluster:
                    cluster.shutdown()
                
                # Choose new database
                new_cluster_ip = choose_database()
                if new_cluster_ip is None:
                    break
                
                # Connect to new database
                try:
                    print(f"\nĐang kết nối tới {new_cluster_ip}...")
                    cluster, session = connect_to_cluster(new_cluster_ip, keyspace_name)
                    cluster_ip = new_cluster_ip
                except Exception as e:
                    print(f"Lỗi kết nối: {e}")
                    input("\nNhấn Enter để tiếp tục...")
                    break
            elif choice == "0":
                break
            else:
                print("Lựa chọn không hợp lệ!")
                input("\nNhấn Enter để tiếp tục...")
    
    except Exception as e:
        print(f"Error connecting to Cassandra: {e}")
    
    finally:
        # Always close connections
        if cluster:
            cluster.shutdown()
            print("Connection closed.")

if __name__ == "__main__":
    main()