DROP TABLE IF EXISTS products CASCADE;
DROP TABLE IF EXISTS stores CASCADE;
DROP TABLE IF EXISTS categories CASCADE;
DROP TABLE IF EXISTS users CASCADE;

-- 2. Bảng Người dùng (Users)
CREATE TABLE users (
    user_id SERIAL PRIMARY KEY,
    username VARCHAR(100) UNIQUE NOT NULL,
    email VARCHAR(255) UNIQUE NOT NULL,
    full_name VARCHAR(255),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 3. Bảng Danh mục (Categories) - Hỗ trợ đệ quy
CREATE TABLE categories (
    category_id SERIAL PRIMARY KEY,
    parent_id INT DEFAULT NULL,
    name VARCHAR(255) NOT NULL,
    slug VARCHAR(255) UNIQUE,
    level INT DEFAULT 1,
    CONSTRAINT fk_category_parent FOREIGN KEY (parent_id) REFERENCES categories (category_id) ON DELETE SET NULL
);

-- 4. Bảng Cửa hàng (Stores)
CREATE TABLE stores (
    store_id SERIAL PRIMARY KEY,
    owner_id INT NOT NULL,
    address  VARCHAR(255),
    store_name VARCHAR(255) NOT NULL,
    is_official_store BOOLEAN DEFAULT FALSE,
    rating DECIMAL(3,2) DEFAULT 0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT fk_store_owner FOREIGN KEY (owner_id) REFERENCES users (user_id)
);

-- 5. Bảng Sản phẩm (Products)
CREATE TABLE products (
    product_id SERIAL PRIMARY KEY,
    store_id INT NOT NULL,
    category_id INT NOT NULL,
    name VARCHAR(255) NOT NULL,
    description TEXT,
    price DECIMAL(15,2) NOT NULL,
    stock_quantity INT DEFAULT 0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT fk_product_store FOREIGN KEY (store_id) REFERENCES stores (store_id),
    CONSTRAINT fk_product_category FOREIGN KEY (category_id) REFERENCES categories (category_id)
);
