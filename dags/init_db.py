import psycopg2
from faker import Faker
import random


# Cấu hình kết nối Postgres
DB_CONFIG = {
    "host": "postgres",
    "port": 5432,
    "database": "finhouse",
    "user": "finhouse",
    "password": "finhouse"
}

fake = Faker()

size_user = 10000
size_product = 10000
size_owner = 100
def init_metadata():
    try:
        conn = None # Gán None trước
        cur = None
    
    
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()
        print("🚀 Đang khởi tạo dữ liệu Metadata...")

        # 1. Tạo Users (Chủ cửa hàng và Khách hàng)
        print("--- Đang tạo Users...")
        user_ids = []
        for i in range(size_user): # Tạo 1,000,000 users
            username = fake.unique.user_name()
            email = fake.unique.email()
            full_name = fake.name()
            cur.execute(
                "INSERT INTO users (username, email, full_name) VALUES (%s, %s, %s) RETURNING user_id;",
                (username, email, full_name)
            )
            user_ids.append(cur.fetchone()[0])
        print(f"✅ Đã tạo {len(user_ids)} users thành công!")
        
        # 2. Tạo Categories (Danh mục 2 cấp)
        print("--- Đang tạo Categories...")
        categories = [
            "Electronics",
            "Fashion", 
            "Home & Garden", 
            "Books", 
            "Beauty", 
            "Sports & Outdoors",  
            "Toys & Hobbies",     
            "Automotive",        
            "Health & Personal Care", 
            "Pet Supplies"       
        ]      
        category_ids = []
        for cat_name in categories:
            cur.execute(
                "INSERT INTO categories (name, slug, level) VALUES (%s, %s, %s) RETURNING category_id",
                (cat_name, cat_name.lower().replace(" ", "-"), 1)
            )
            parent_id = cur.fetchone()[0]
            
            # Tạo sub-categories
            for _ in range(10):
                sub_name = f"{cat_name} {fake.word().capitalize()}"
                cur.execute(
                    "INSERT INTO categories (name, slug, level, parent_id) VALUES (%s, %s, %s, %s) RETURNING category_id",
                    (sub_name, sub_name.lower().replace(" ", "-"), 2, parent_id)
                )
                category_ids.append(cur.fetchone()[0])
        print(f"✅ Đã tạo {len(category_ids)} categories thành công!")
        
        # 3. Tạo Stores
        print("--- Đang tạo Stores...")
        store_ids = []
        # Lấy 100 users ngẫu nhiên làm chủ shop
        owners = random.sample(user_ids, size_owner)
        for owner_id in owners:
            store_name = f"{fake.company()} Official Store"
            cur.execute(
                "INSERT INTO stores (owner_id, store_name, is_official_store, rating) VALUES (%s, %s, %s, %s) RETURNING store_id",
                (owner_id, store_name, random.choice([True, False]), round(random.uniform(3.5, 5.0), 2))
            )
            store_ids.append(cur.fetchone()[0])
        print(f"✅ Đã tạo {len(store_ids)} stores thành công!")
        
        # 4. Tạo Products
        print("--- Đang tạo Products...")
        for _ in range(size_product): # Tạo 1,000,000 sản phẩm
            cur.execute(
                """INSERT INTO products (store_id, category_id, name, description, price, stock_quantity) 
                   VALUES (%s, %s, %s, %s, %s, %s)""",
                (
                    random.choice(store_ids),
                    random.choice(category_ids),
                    fake.catch_phrase(),
                    fake.text(max_nb_chars=200),
                    round(random.uniform(10.0, 2000.0) * 23000, -3), # Giá từ 230k đến 46tr VND
                    random.randint(10, 500)
                )
            )
        print(f"✅ Đã tạo {size_product} products thành công!")
        
        conn.commit()
        print("✅ Đã hoàn thành khởi tạo Metadata thành công!")

    except Exception as e:
        print(f"🔥 Lỗi: {e}")
        if conn:
            conn.rollback()
    finally:
        if cur: cur.close()
        if conn: conn.close()

if __name__ == "__main__":
    init_metadata()