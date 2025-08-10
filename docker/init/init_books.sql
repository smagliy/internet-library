CREATE TABLE IF NOT EXISTS books (
    book_id SERIAL PRIMARY KEY,
    title VARCHAR(500) NOT NULL,
    price NUMERIC(10, 2),
    genre VARCHAR(100),
    stock_quantity INT DEFAULT 0,
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS books_processed (
    processed_id SERIAL PRIMARY KEY,
    book_id INT,
    title VARCHAR(500),
    original_price NUMERIC(10, 2),
    rounded_price NUMERIC(10, 1),
    genre VARCHAR(100),
    price_category VARCHAR(10) CHECK (price_category IN ('budget', 'premium')),
    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_books_genre ON books(genre);
CREATE INDEX IF NOT EXISTS idx_books_last_updated ON books(last_updated);
CREATE INDEX IF NOT EXISTS idx_books_price_range ON books(price);


DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM books LIMIT 1) THEN
        INSERT INTO books (title, price, genre, stock_quantity, last_updated) VALUES
        ('Назва книги 1', 299.99, 'фантастика', 15, '2025-01-15 10:30:00'),
        ('Назва книги 2', 749.50, 'історичний роман', 5, '2025-02-20 09:00:00'),
        ('Назва книги 3', 450.00, 'детектив', 0, '2024-12-05 14:15:00'),
        ('Назва книги 4', 210.25, 'фантастика', 20, '2025-03-01 18:45:00'),
        ('Назва книги 5', 799.90, 'поезія', 3, '2025-01-30 12:00:00'),
        ('Назва книги 6', 380.10, 'історичний роман', 12, '2025-04-10 08:20:00');
    END IF;
END $$;
