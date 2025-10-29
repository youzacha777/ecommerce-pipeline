-- 01-create-tables.sql
-- AddToCart 테이블 생성
CREATE TABLE IF NOT EXISTS cart_table (
    order_id       VARCHAR(50) PRIMARY KEY,
    user_id        VARCHAR(50) NOT NULL,
    user_name      VARCHAR(100),
    email          VARCHAR(100),
    product_id     VARCHAR(50),
    product_name   VARCHAR(100),
    quantity       INT,
    price          NUMERIC(10,2),
    total          NUMERIC(10,2),
    status         VARCHAR(20),
    created_at     TIMESTAMP DEFAULT NOW()
);

-- Purchase 테이블 생성
CREATE TABLE IF NOT EXISTS purchase_table (
    order_id       VARCHAR(50) PRIMARY KEY,
    user_id        VARCHAR(50) NOT NULL,
    user_name      VARCHAR(100),
    email          VARCHAR(100),
    product_id     VARCHAR(50),
    product_name   VARCHAR(100),
    quantity       INT,
    price          NUMERIC(10,2),
    total          NUMERIC(10,2),
    status         VARCHAR(20),
    created_at     TIMESTAMP DEFAULT NOW()
);

-- Cancel 테이블 생성
CREATE TABLE cancel_table (
    order_id      VARCHAR(50) PRIMARY KEY,
    user_id       VARCHAR(50) NOT NULL,
    product_id    VARCHAR(50),
    cancel_reason VARCHAR(100),
    cancel_time   TIMESTAMP DEFAULT NOW(),
    quantity      INT,
    price         NUMERIC(10,2),
    total         NUMERIC(10,2)
);


-- 인덱스 생성

-- 장바구니 테이블: user_id 검색 최적화
CREATE INDEX IF NOT EXISTS idx_cart_user_id ON cart_table(user_id);

-- 구매 테이블: user_id 검색 최적화
CREATE INDEX IF NOT EXISTS idx_purchase_user_id ON purchase_table(user_id);