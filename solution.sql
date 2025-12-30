/*=========================================================================
                        Инициализация базы
==========================================================================*/
-- Этап 1. Создание и заполнение БД

-- Обнуляем базу
DROP SCHEMA IF EXISTS raw_data CASCADE;

-- Создаем схему raw_data
CREATE SCHEMA IF NOT EXISTS raw_data;

-- Создаем таблицу sales в схеме raw_data
CREATE TABLE raw_data.sales (
    id INTEGER,
    auto TEXT,
    gasoline_consumption NUMERIC(5,2),
    price NUMERIC(9,2),
    order_date DATE,
    person TEXT,  -- сюда будет импортироваться person_name из CSV
    phone TEXT,
    discount NUMERIC(5,2),
    brand_origin TEXT
);

/*
    Примечание:
        изменено поле data -> order_date (совпадение имени поля с системнымы командами)
        импорт person_name -> в поле person
        учет null позиции при импорте данных (используется DBeaver - Import CSV)
*/

-- Полное обнуление схемы car_shop и всех её объектов
DROP SCHEMA IF EXISTS car_shop CASCADE;

-- Создание схемы car_shop
CREATE SCHEMA car_shop;

-- Таблица 1: Бренды автомобилей
CREATE TABLE car_shop.brands (
    id SERIAL PRIMARY KEY,                     /* Суррогатный первичный ключ с автоинкрементом */
    name VARCHAR(50) NOT NULL UNIQUE,          /* Название бренда ограниченной длины, уникальное */
    origin_country VARCHAR(50) NOT NULL        /* Страна происхождения бренда, текстовое значение */
);

-- Таблица 2: Модели автомобилей
CREATE TABLE car_shop.models (
    id SERIAL PRIMARY KEY,                     /* Суррогатный первичный ключ с автоинкрементом */
    brand_id INTEGER NOT NULL REFERENCES car_shop.brands(id) ON DELETE CASCADE,  /* Внешний ключ к брендам, целое число */
    name VARCHAR(100) NOT NULL,                /* Название модели, может быть длинным (например, "X5 M Competition") */
    gasoline_consumption NUMERIC(5,2) CHECK (gasoline_consumption > 0 OR gasoline_consumption IS NULL)  
                                                /* Расход топлива с точностью до 2 знаков, NULL для электромобилей */
);

-- Таблица 3: Цвета автомобилей
CREATE TABLE car_shop.colors (
    id SERIAL PRIMARY KEY,                     /* Суррогатный первичный ключ с автоинкрементом */
    name VARCHAR(50) NOT NULL UNIQUE           /* Название цвета, уникальное текстовое значение */
);

-- Таблица 4: Клиенты
CREATE TABLE car_shop.customers (
    id SERIAL PRIMARY KEY,                     /* Суррогатный первичный ключ с автоинкрементом */
    full_name VARCHAR(200) NOT NULL,           /* Полное имя клиента, может содержать пробелы и специальные символы */
    phone VARCHAR(20) UNIQUE                   /* Номер телефона, может содержать плюсы, скобки, дефисы */
);

-- Таблица 5: Автомобили (конкретные экземпляры)
CREATE TABLE car_shop.cars (
    id SERIAL PRIMARY KEY,                     /* Суррогатный первичный ключ с автоинкрементом */
    model_id INTEGER NOT NULL REFERENCES car_shop.models(id) ON DELETE RESTRICT,  /* Внешний ключ к моделям */
    color_id INTEGER NOT NULL REFERENCES car_shop.colors(id) ON DELETE RESTRICT,  /* Внешний ключ к цветам */
    base_price NUMERIC(9,2) NOT NULL CHECK (base_price > 0 AND base_price <= 9999999.99),  
                                                /* Базовая цена с точностью до 2 знаков, максимально 9,999,999.99 */
    discount NUMERIC(5,2) DEFAULT 0 CHECK (discount >= 0 AND discount <= 100)     
                                                /* Скидка в процентах с точностью до 2 знаков, от 0 до 100% */
);

-- Таблица 6: Продажи
CREATE TABLE car_shop.sales (
    id SERIAL PRIMARY KEY,                     /* Суррогатный первичный ключ с автоинкрементом */
    car_id INTEGER NOT NULL REFERENCES car_shop.cars(id) ON DELETE RESTRICT,      /* Внешний ключ к автомобилям */
    customer_id INTEGER NOT NULL REFERENCES car_shop.customers(id) ON DELETE RESTRICT,  /* Внешний ключ к клиентам */
    order_date DATE NOT NULL,                  /* Дата продажи без времени */
    final_price NUMERIC(9,2) NOT NULL CHECK (final_price > 0)  
                                                /* Итоговая цена с учётом скидки, положительное значение */
);

-- Создание индексов для ускорения поиска
CREATE INDEX idx_models_brand_id ON car_shop.models(brand_id);
CREATE INDEX idx_cars_model_id ON car_shop.cars(model_id);
CREATE INDEX idx_cars_color_id ON car_shop.cars(color_id);
CREATE INDEX idx_sales_car_id ON car_shop.sales(car_id);
CREATE INDEX idx_sales_customer_id ON car_shop.sales(customer_id);
CREATE INDEX idx_sales_order_date ON car_shop.sales(order_date);

/*=========================================================================
                    заполнение таблиц новой схемы
==========================================================================*/

-- Заполнение таблицы colors (цвета) из сырых данных
INSERT INTO car_shop.colors (name)
SELECT DISTINCT 
    TRIM(SPLIT_PART(auto, ',', 2)) as color_name
FROM raw_data.sales
WHERE TRIM(SPLIT_PART(auto, ',', 2)) != ''
ORDER BY color_name;

-- Заполнение таблицы brands (бренды) из сырых данных
INSERT INTO car_shop.brands (name, origin_country)
SELECT DISTINCT 
    SPLIT_PART(SPLIT_PART(auto, ' ', 1), ',', 1) as brand_name,
    brand_origin
FROM raw_data.sales
ORDER BY brand_name;

-- Заполнение таблицы models (модели) из сырых данных
INSERT INTO car_shop.models (brand_id, name, gasoline_consumption)
SELECT DISTINCT 
    b.id as brand_id,
    TRIM(SPLIT_PART(SPLIT_PART(auto, ',', 1), SPLIT_PART(auto, ' ', 1), 2)) as model_name,
    s.gasoline_consumption
FROM raw_data.sales s
JOIN car_shop.brands b ON SPLIT_PART(SPLIT_PART(s.auto, ' ', 1), ',', 1) = b.name
WHERE TRIM(SPLIT_PART(SPLIT_PART(s.auto, ',', 1), SPLIT_PART(s.auto, ' ', 1), 2)) != ''
ORDER BY model_name;

-- Заполнение таблицы customers (клиенты) из сырых данных
INSERT INTO car_shop.customers (full_name, phone)
SELECT DISTINCT 
    s.person,
    s.phone
FROM raw_data.sales s
WHERE s.person IS NOT NULL
ORDER BY s.person;

-- Заполнение таблицы cars (автомобили) из сырых данных
INSERT INTO car_shop.cars (model_id, color_id, base_price, discount)
SELECT DISTINCT 
    m.id as model_id,
    col.id as color_id,  -- Исправлено с c.id на col.id
    s.price / (1 - COALESCE(s.discount, 0) / 100.0) as base_price,
    COALESCE(s.discount, 0) as discount
FROM raw_data.sales s
JOIN car_shop.brands b ON SPLIT_PART(SPLIT_PART(s.auto, ' ', 1), ',', 1) = b.name
JOIN car_shop.models m ON m.brand_id = b.id 
    AND TRIM(SPLIT_PART(SPLIT_PART(s.auto, ',', 1), SPLIT_PART(s.auto, ' ', 1), 2)) = m.name
    AND (s.gasoline_consumption = m.gasoline_consumption 
         OR (s.gasoline_consumption IS NULL AND m.gasoline_consumption IS NULL))
JOIN car_shop.colors col ON TRIM(SPLIT_PART(s.auto, ',', 2)) = col.name
ORDER BY m.id, col.id;

-- Заполнение таблицы sales (продажи) из сырых данных
INSERT INTO car_shop.sales (car_id, customer_id, order_date, final_price)
SELECT 
    ca.id as car_id,
    cu.id as customer_id,
    s.order_date,
    s.price as final_price
FROM raw_data.sales s
JOIN car_shop.brands b ON SPLIT_PART(SPLIT_PART(s.auto, ' ', 1), ',', 1) = b.name
JOIN car_shop.models m ON m.brand_id = b.id 
    AND TRIM(SPLIT_PART(SPLIT_PART(s.auto, ',', 1), SPLIT_PART(s.auto, ' ', 1), 2)) = m.name
    AND (s.gasoline_consumption = m.gasoline_consumption 
         OR (s.gasoline_consumption IS NULL AND m.gasoline_consumption IS NULL))
JOIN car_shop.colors col ON TRIM(SPLIT_PART(s.auto, ',', 2)) = col.name
JOIN car_shop.cars ca ON ca.model_id = m.id AND ca.color_id = col.id 
    AND ca.base_price = s.price / (1 - COALESCE(s.discount, 0) / 100.0)
    AND ca.discount = COALESCE(s.discount, 0)
JOIN car_shop.customers cu ON s.person = cu.full_name AND (s.phone = cu.phone OR (s.phone IS NULL AND cu.phone IS NULL))
ORDER BY s.order_date;


/*=========================================================================
                            решение заданий
==========================================================================*/
-- Этап 2. Создание выборок

---- Задание 1. Напишите запрос, который выведет процент моделей машин, у которых нет параметра `gasoline_consumption`.
SELECT 
    ROUND(
        COUNT(CASE WHEN gasoline_consumption IS NULL THEN 1 END) * 100.0 / COUNT(*),
        2
    ) AS nulls_percentage_gasoline_consumption
FROM car_shop.models;


---- Задание 2. Напишите запрос, который покажет название бренда и среднюю цену его автомобилей в разбивке по всем годам с учётом скидки.
SELECT 
    b.name AS brand_name,
    EXTRACT(YEAR FROM s.order_date)::INTEGER AS year,
    ROUND(AVG(s.final_price), 2) AS price_avg
FROM car_shop.sales s
JOIN car_shop.cars c ON s.car_id = c.id
JOIN car_shop.models m ON c.model_id = m.id
JOIN car_shop.brands b ON m.brand_id = b.id
WHERE s.order_date IS NOT NULL
GROUP BY b.name, EXTRACT(YEAR FROM s.order_date)
ORDER BY brand_name, year;


---- Задание 3. Посчитайте среднюю цену всех автомобилей с разбивкой по месяцам в 2022 году с учётом скидки.
SELECT 
    EXTRACT(MONTH FROM s.order_date)::INTEGER AS month,
    2022 AS year,
    ROUND(AVG(s.final_price), 2) AS price_avg
FROM car_shop.sales s
WHERE EXTRACT(YEAR FROM s.order_date) = 2022
GROUP BY EXTRACT(MONTH FROM s.order_date)
ORDER BY month ASC;


---- Задание 4. Напишите запрос, который выведет список купленных машин у каждого пользователя.
SELECT 
    cu.full_name AS person,
    STRING_AGG(CONCAT(b.name, ' ', m.name), ', ') AS cars
FROM car_shop.sales s
JOIN car_shop.customers cu ON s.customer_id = cu.id
JOIN car_shop.cars ca ON s.car_id = ca.id
JOIN car_shop.models m ON ca.model_id = m.id
JOIN car_shop.brands b ON m.brand_id = b.id
GROUP BY cu.full_name
ORDER BY cu.full_name ASC;


---- Задание 5. 
SELECT 
    CASE 
        WHEN NULLIF(TRIM(b.origin_country), '') IS NULL OR LOWER(NULLIF(TRIM(b.origin_country), '')) = 'null'
        THEN 'страна не указана'
        ELSE b.origin_country 
    END AS brand_origin,
    MAX(c.base_price) AS price_max,
    MIN(c.base_price) AS price_min
FROM car_shop.sales s
JOIN car_shop.cars c ON s.car_id = c.id
JOIN car_shop.models m ON c.model_id = m.id
JOIN car_shop.brands b ON m.brand_id = b.id
GROUP BY b.origin_country
ORDER BY 
    CASE 
        WHEN NULLIF(TRIM(b.origin_country), '') IS NULL OR LOWER(NULLIF(TRIM(b.origin_country), '')) = 'null'
        THEN 1 
        ELSE 0 
    END,
    brand_origin;


---- Задание 6. 
WITH cleaned_phones AS (
    SELECT 
        id,
        REGEXP_REPLACE(phone, '[^0-9]', '', 'g') AS clean_phone
    FROM car_shop.customers
    WHERE phone IS NOT NULL
)
SELECT 
    COUNT(*) AS persons_from_usa_count
FROM cleaned_phones
WHERE clean_phone LIKE '1%';

