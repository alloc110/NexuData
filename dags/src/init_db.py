from datetime import datetime
import json
import logging
import os
import random
import time
import sys
from faker import Faker
import psycopg2
from psycopg2.extras import execute_values

sys.path.append("/opt/airflow/plugins")
# GỌI LOGGER TẬP TRUNG TỪ FILE UTILS
from utils.logger_utils import get_logger

logger = get_logger("MetadataInitializer", "DataInitializer")

# =========================================================
# PostgreSQL Config
# =========================================================
DB_CONFIG = {
    "host": os.getenv("PG_HOST", "postgres"),
    "port": int(os.getenv("PG_PORT", 5432)),
    "database": os.getenv("PG_DB", "finhouse"),
    "user": os.getenv("PG_USER", "finhouse"),
    "password": os.getenv("PG_PASSWORD", "finhouse")
}

# =========================================================
# Faker & Dataset Size
# =========================================================
fake = Faker("en_US")

size_user = 100000
size_product = 100000
size_events = 100000
size_owner = 100
# =========================================================
# Catalogs & Generators (Giữ nguyên cấu trúc logic của bạn)
# =========================================================
PRODUCT_CATALOG = {
    "Electronics": {
        "brands": ["Apple", "Samsung", "Sony"],
        "products": ["Phones", "Laptops", "Accessories"],
        "adjectives": ["Premium",  "Wireless", "Smart"]
    },
    "Fashion": {
        "brands": ["Zara", "H&M"],
        "products": ["Men Clothing", "Women Clothing", "Shoes"],
        "adjectives": ["Sport", "Modern"]
    },
    "Home & Garden": {
        "brands": ["IKEA", "Philips"],
        "products": ["Furniture","Kitchen","Decor"],
        "adjectives": [ "Modern", "Minimalist","Compact"]
    },
    "Books": {
        "brands": ["Penguin", "OReilly", "HarperCollins"],
        "products": ["Fiction","Business","Education"],
        "adjectives": [ "Advanced", "Professional","Beginner", "Comprehensive"]
    },
    "Beauty": {
        "brands": ["Dior", "Chanel", "LOréal"],
        "products": ["Skincare","Makeup","Perfume"],
        "adjectives": ["Luxury","Natural","Organic","Refreshing"]
    },
    "Sports & Outdoors": {
        "brands": ["Nike", "Adidas", "Under Armour","Puma"],
        "products": ["Fitness","Camping","Cycling"],
        "adjectives": ["Professional","Durable","Outdoor","Lightweight"]
    },
    "Toys & Hobbies": {
        "brands": ["LEGO", "Hasbro", "Mattel"],
        "products": ["Board Games","RC Toys","Puzzles"],
        "adjectives": ["Creative","Educational","Interactive","Fun"]
    },
    "Automotive": {
        "brands": ["Bosch","Michelin","Castrol"],
        "products": ["Car Accessories","Motorbike Parts","Tools"],
        "adjectives": ["Heavy Duty","Professional","Durable","Compact"]
    },
    "Health & Personal Care": {
        "brands": ["Nature Made","Centrum","Omron"],
        "products": ["Supplements","Medical Devices","Personal Hygiene"],
        "adjectives": ["Healthy","Portable","Advanced","Daily"]
    },
    "Pet Supplies": {
        "brands": ["Pedigree","Whiskas","Purina"],
        "products": ["Dog Food","Cat Toys","Aquarium"],
        "adjectives": ["Nutritious","Interactive","Premium","Healthy"]
    }
}
        
CATEGORIES = {
    "Electronics": ["Phones", "Laptops", "Accessories"],
    "Fashion": ["Men Clothing", "Women Clothing", "Shoes"],
    "Home & Garden": ["Furniture", "Kitchen", "Decor"],
    "Books": ["Fiction", "Business", "Education"],
    "Beauty": ["Skincare", "Makeup", "Perfume"],
    "Sports & Outdoors": ["Fitness", "Camping", "Cycling"],
    "Toys & Hobbies": ["Board Games", "RC Toys", "Puzzles"],
    "Automotive": ["Car Accessories", "Motorbike Parts", "Tools"],
    "Health & Personal Care": ["Supplements", "Medical Devices", "Personal Hygiene"],
    "Pet Supplies": ["Dog Food", "Cat Toys", "Aquarium"]
}   

def generate_product_name(category):
    data = PRODUCT_CATALOG.get(category)
    return f"{random.choice(data['brands'])} {random.choice(data['adjectives'])} {random.choice(data['products'])}"

def generate_product_description(category):
    templates = [
        "High-quality {product} designed for everyday use.",
        "Latest generation {product} with premium build quality.",
        "Perfect choice for customers looking for reliable {product}.",
        "Modern and durable {product} suitable for work and entertainment.",
        "Top-rated {product} featuring advanced performance and stylish design."
    ]
    data = PRODUCT_CATALOG.get(category, PRODUCT_CATALOG["Electronics"])
    product = random.choice(data["products"])
    return random.choice(templates).format(product=product)

# =========================================================
# LUỒNG XỬ LÝ CHÍNH
# =========================================================
def init_metadata():
    conn = None
    cur = None

    try:
        total_start_time = time.time()
        
        # INFO LOG: Khởi động job
        init_record = logging.LogRecord("DataInitializer", logging.INFO, "", 0, "Starting metadata initialization job", None, None)
        init_record.extra_data = {"target_database": DB_CONFIG["database"]}
        logger.handle(init_record)

        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()

        # =====================================================
        # 1. USERS GENERATION
        # =====================================================
        step_start = time.time()
        users = []
        for _ in range(size_user):
            users.append((fake.unique.user_name(), fake.unique.email(), fake.name()))

        execute_values(cur, "INSERT INTO users (username, email, full_name) VALUES %s", users)
        
        user_record = logging.LogRecord("DataInitializer", logging.INFO, "", 0, "Successfully populated 'users' table", None, None)
        user_record.extra_data = {"metrics": {"rows_inserted": len(users), "duration_seconds": round(time.time() - step_start, 4)}}
        logger.handle(user_record)
        
        # =====================================================
        # 2. CATEGORIES GENERATION
        # =====================================================
        step_start = time.time()
        categories_parent = []
        categories_child = []
        category_mapping = {}
        
        parent_id = 1
        for cat, sub in CATEGORIES.items():
            categories_parent.append((cat, cat.lower().replace(" ", "-"), 1, None)) 
            category_mapping[parent_id] = cat
            for sub_name in sub:
                categories_child.append((sub_name, sub_name.lower().replace(" ", "-"), 2, parent_id))  
            parent_id += 1
        
        execute_values(cur, "INSERT INTO categories (name, slug, level, parent_id) VALUES %s", categories_parent)
        execute_values(cur, "INSERT INTO categories (name, slug, level, parent_id) VALUES %s", categories_child)
        
        cat_record = logging.LogRecord("DataInitializer", logging.INFO, "", 0, "Successfully populated 'categories' table", None, None)
        cat_record.extra_data = {"metrics": {"rows_inserted": len(categories_parent) + len(categories_child), "duration_seconds": round(time.time() - step_start, 4)}}
        logger.handle(cat_record)
        
        # =====================================================
        # 3. STORES GENERATION
        # =====================================================
        step_start = time.time()
        owners = random.sample(range(1, len(users) + 1), size_owner)
        stores_data = []
        for owner_id in owners:
            stores_data.append((owner_id, fake.company(), fake.state(), random.choice([True, False]), round(random.uniform(3.5, 5.0), 2)))
        
        execute_values(cur, "INSERT INTO stores (owner_id, store_name, address, is_official_store, rating) VALUES %s", stores_data)

        store_record = logging.LogRecord("DataInitializer", logging.INFO, "", 0, "Successfully populated 'stores' table", None, None)
        store_record.extra_data = {"metrics": {"rows_inserted": len(stores_data), "duration_seconds": round(time.time() - step_start, 4)}}
        logger.handle(store_record)
        
        # =====================================================
        # 4. PRODUCTS GENERATION
        # =====================================================
        step_start = time.time()
        store_ids = list(range(1, len(stores_data) + 1))
        category_ids = list(category_mapping.keys()) 

        products_data = []
        for _ in range(size_product):
            category_id = random.choice(category_ids)
            category_name = category_mapping[category_id]
            products_data.append((
                random.choice(store_ids),
                category_id,
                generate_product_name(category_name),
                generate_product_description(category_name),
                round(random.uniform(10, 100), 2)
            ))        
        execute_values(cur, "INSERT INTO products (store_id, category_id, name, description, price) VALUES %s", products_data)
        
        product_record = logging.LogRecord("DataInitializer", logging.INFO, "", 0, "Successfully populated 'products' table", None, None)
        product_record.extra_data = {"metrics": {"rows_inserted": len(products_data), "duration_seconds": round(time.time() - step_start, 4)}}
        logger.handle(product_record)
        
        # COMMIT TRANSACTION
        conn.commit()
        
        # INFO LOG: Hoàn tất toàn bộ tiến trình
        total_duration = time.time() - total_start_time
        finish_record = logging.LogRecord("DataInitializer", logging.INFO, "", 0, "Metadata initialization completed successfully", None, None)
        finish_record.extra_data = {
            "summary": {
                "status": "SUCCESS",
                "total_duration_seconds": round(total_duration, 4),
                "total_tables_affected": 4
            }
        }
        logger.handle(finish_record)

    except Exception as e:
        if conn:
            conn.rollback()
        # ERROR LOG: Xử lý ngoại lệ phát sinh
        error_record = logging.LogRecord("DataInitializer", logging.ERROR, "", 0, "Metadata initialization failed, transaction rolled back", None, None)
        error_record.extra_data = {"error_details": str(e)}
        logger.handle(error_record)
    finally:
        if cur: cur.close()
        if conn: conn.close()

if __name__ == "__main__":
    init_metadata()