import psycopg2
from faker import Faker
import random
from psycopg2.extras import execute_values
import time


# =========================================================
# PostgreSQL Config
# =========================================================

DB_CONFIG = {
    "host": "postgres",
    "port": 5432,
    "database": "finhouse",
    "user": "finhouse",
    "password": "finhouse"
}


# =========================================================
# Faker
# =========================================================

fake = Faker("en_US")


# =========================================================
# Dataset Size
# =========================================================

size_user = 10000
size_product = 10000
size_owner = 100

# =========================================================
# Product Catalog
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
        
# =========================================================
# Categories Catalog
# =========================================================

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


# =========================================================
# Product Name Generator
# =========================================================
def generate_product_name(category):
    data = PRODUCT_CATALOG.get(category)
    return (
        f"{random.choice(data['brands'])} "
        f"{random.choice(data['adjectives'])} "
        f"{random.choice(data['products'])}"
    )

# =========================================================
# Product Description Generator
# =========================================================
def generate_product_description(category):

    templates = [
        "High-quality {product} designed for everyday use.",
        "Latest generation {product} with premium build quality.",
        "Perfect choice for customers looking for reliable {product}.",
        "Modern and durable {product} suitable for work and entertainment.",
        "Top-rated {product} featuring advanced performance and stylish design."
    ]

    data = PRODUCT_CATALOG.get(
        category,
        PRODUCT_CATALOG["Electronics"]
    )

    product = random.choice(data["products"])

    return random.choice(templates).format(
        product=product
    )

# =========================================================
# Main
# =========================================================

def init_metadata():

    conn = None
    cur = None

    try:
        start_time = time.time()
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()

        print("🚀 Đang khởi tạo dữ liệu Metadata...")

        # =====================================================
        # USERS
        # =====================================================

        print("---------------- USERS ----------------")
        users = []
        for i in range(size_user): # Tạo 10,000 users
            username = fake.unique.user_name()
            email = fake.unique.email()
            full_name = fake.name()
            users.append((username, email, full_name))

        execute_values(cur, "INSERT INTO users (username, email, full_name) VALUES %s", users)

        print("#########################################################")
        print(f"✅ Đã tạo {len(users)} users thành công!")
        print("#########################################################")
        
        # =====================================================
        # CATEGORIES
        # =====================================================
        
        print("---------------- CATEGORIES ----------------")

        categories_parent = []
        categories_child = []
        category_mapping = {}
        
        parent_id = 1
        for cat, sub in CATEGORIES.items():
            categories_parent.append((cat, cat.lower().replace(" ", "-"), 1,None)) 
            category_mapping[parent_id] = cat
            for sub_name in sub:
                categories_child.append((sub_name, sub_name.lower().replace(" ", "-"), 2, parent_id))  
            parent_id += 1
        
        execute_values(cur, "INSERT INTO categories (name, slug, level, parent_id) VALUES %s", categories_parent)
        execute_values(cur, "INSERT INTO categories (name, slug, level, parent_id) VALUES %s", categories_child)
        print("#########################################################")
        print(f"✅ Đã tạo 30 categories thành công!")
        print("#########################################################")
        
        #########################################################
        # 3. Tạo Stores
        #########################################################

        print("---------------- STORES ----------------")
        # Lấy 10000 users ngẫu nhiên làm chủ shop
        owners = random.sample(range(1, len(users) + 1), size_owner)
        stores_data = []
        for owner_id in owners:
            store_name = fake.company()
            address = fake.state()
            stores_data.append((owner_id, store_name, address, random.choice([True, False]), round(random.uniform(3.5, 5.0), 2)))
        
        execute_values(cur, "INSERT INTO stores (owner_id, store_name, address, is_official_store, rating) VALUES %s", stores_data)

        print("#########################################################")
        print(f"✅ Đã tạo {len(stores_data)} stores thành công!")
        print("#########################################################")
        
        #########################################################
        # 4. Tạo Products
        #########################################################

        print("---------------- PRODUCTS ----------------")
        store_ids = list(range(1, len(stores_data) + 1))
        category_ids = list(category_mapping.keys()) 

        products_data = []
        for _ in range(size_product):
            store_id = random.choice(store_ids)
            category_id = random.choice(category_ids)
            category_name = category_mapping[category_id]
            product_name = generate_product_name(category_name)
            product_desc = generate_product_description(category_name)

            products_data.append((store_id,
                                    category_id,
                                    product_name,
                                    product_desc,
                                    round(random.uniform(10000, 1000000), 2),
                                    random.randint(1, 100)))
        
        execute_values(cur, "INSERT INTO products (store_id, category_id, name, description, price, stock_quantity) VALUES %s", products_data)
        
       
        print("#########################################################")
        print(f"✅ Đã tạo {size_product} products thành công!")
        print("#########################################################")
        
        conn.commit()
        
        end_time = time.time()
        
        print("------------------------------------------------------------------------------------------------------------------")
        print(f"| Đã hoàn thành khởi tạo Metadata thành công! |")
        print(f"|Thời gian: {end_time - start_time:.2f} giây |")
        print("------------------------------------------------------------------------------------------------------------------")

    except Exception as e:
        print(f"🔥 Lỗi: {e}")
        if conn:
            conn.rollback()
    finally:
        if cur: cur.close()
        if conn: conn.close()


if __name__ == "__main__":
    init_metadata()