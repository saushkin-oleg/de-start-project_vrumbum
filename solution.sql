/*=========================================================================
                        Инициализация базы
==========================================================================*/
-- Этап 1. Создание и заполнение БД

-- Обнуляем базу
DROP SCHEMA IF EXISTS raw_data CASCADE;
DROP SCHEMA IF EXISTS car_shop CASCADE;

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




/*=========================================================================
                        Импорт данных из CSV файла cars.csv
==========================================================================*/

/*
    Примечание:
        изменено поле data -> order_date (совпадение имени поля с системнымы командами)
        импорт person_name -> в поле person
        учет null позиции при импорте данных (используется DBeaver - Import CSV)
*/

-- Очищаем таблицу перед импортом
TRUNCATE TABLE raw_data.sales;

-- Вариант 1: Использование команды COPY с указанием последовательности колонок из файла
COPY raw_data.sales (id, auto, gasoline_consumption, price, order_date, person, phone, discount, brand_origin)
FROM 'путь/к/файлу/cars.csv'
WITH (
    FORMAT CSV,
    HEADER true,
    DELIMITER ',',
    NULL 'NULL'  -- обрабатывает строки 'NULL' как NULL значения
);

-- Вариант 2:
/*
psql -U username -d database_name -c "TRUNCATE TABLE raw_data.sales; COPY raw_data.sales (id, auto, gasoline_consumption, price, order_date, person, phone, discount, brand_origin) FROM '/путь/к/cars.csv' DELIMITER ',' CSV HEADER NULL 'NULL';"
*/

-- Вариант 3: 
BEGIN;

-- Создаем временную таблицу для безопасного импорта
CREATE TEMP TABLE temp_import (
    id INTEGER,
    auto TEXT,
    gasoline_consumption NUMERIC(5,2),
    price NUMERIC(9,2),
    date DATE,  -- оригинальное имя из CSV
    person_name TEXT,  -- оригинальное имя из CSV
    phone TEXT,
    discount NUMERIC(5,2),
    brand_origin TEXT
);

-- Импортируем во временную таблицу
COPY temp_import FROM 'путь/к/файлу/cars.csv'
DELIMITER ','
CSV HEADER
NULL 'NULL';

-- Проверяем целостность данных перед вставкой
DO $$
DECLARE
    row_count INTEGER;
BEGIN
    SELECT COUNT(*) INTO row_count FROM temp_import;
    
    IF row_count = 0 THEN
        RAISE EXCEPTION 'Файл CSV пуст или не был загружен';
    END IF;
    
    RAISE NOTICE 'Загружено % записей во временную таблицу', row_count;
END $$;

-- Очищаем целевую таблицу
TRUNCATE TABLE raw_data.sales;

-- Копируем данные с переименованием полей
INSERT INTO raw_data.sales (id, auto, gasoline_consumption, price, order_date, person, phone, discount, brand_origin)
SELECT 
    id,
    auto,
    gasoline_consumption,
    price,
    date AS order_date,      -- переименование date → order_date
    person_name AS person,   -- переименование person_name → person
    phone,
    discount,
    brand_origin
FROM temp_import;

-- Проверяем, что все записи скопированы
DO $$
DECLARE
    temp_count INTEGER;
    final_count INTEGER;
BEGIN
    SELECT COUNT(*) INTO temp_count FROM temp_import;
    SELECT COUNT(*) INTO final_count FROM raw_data.sales;
    
    IF temp_count != final_count THEN
        RAISE EXCEPTION 'Потеряны записи при импорте. Во временной: %, в финальной: %', temp_count, final_count;
    ELSE
        RAISE NOTICE 'Успешно импортировано % записей', final_count;
    END IF;
END $$;

COMMIT;


-- =============================================================================================================
-- =============================================================================================================


-- Создание схемы car_shop
CREATE SCHEMA car_shop;

-- Таблица 1: Страны происхождения брендов
CREATE TABLE car_shop.countries (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL UNIQUE
);

-- Таблица 2: Бренды автомобилей
CREATE TABLE car_shop.brands (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL UNIQUE,
    country_id INTEGER REFERENCES car_shop.countries(id) ON DELETE SET NULL
);

-- Таблица 3: Модели автомобилей
CREATE TABLE car_shop.models (
    id SERIAL PRIMARY KEY,
    brand_id INTEGER NOT NULL REFERENCES car_shop.brands(id) ON DELETE CASCADE,
    name TEXT NOT NULL,
    gasoline_consumption NUMERIC(5,2) CHECK (gasoline_consumption > 0 OR gasoline_consumption IS NULL)
);

-- Таблица 4: Цвета автомобилей
CREATE TABLE car_shop.colors (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL UNIQUE
);

-- Таблица 5: Клиенты
CREATE TABLE car_shop.customers (
    id SERIAL PRIMARY KEY,
    full_name TEXT NOT NULL,
    phone TEXT  -- убрано ограничение длины
);

-- Таблица 6: Автомобили (конкретные экземпляры) - только модель и цвет
CREATE TABLE car_shop.cars (
    id SERIAL PRIMARY KEY,
    model_id INTEGER NOT NULL REFERENCES car_shop.models(id) ON DELETE RESTRICT,
    color_id INTEGER NOT NULL REFERENCES car_shop.colors(id) ON DELETE RESTRICT
);

-- Таблица 7: Продажи (цена и скидка перенесены сюда)
CREATE TABLE car_shop.sales (
    id SERIAL PRIMARY KEY,
    car_id INTEGER NOT NULL REFERENCES car_shop.cars(id) ON DELETE RESTRICT,
    customer_id INTEGER NOT NULL REFERENCES car_shop.customers(id) ON DELETE RESTRICT,
    order_date DATE NOT NULL,
    base_price NUMERIC(9,2) NOT NULL CHECK (base_price > 0),
    discount NUMERIC(5,2) DEFAULT 0 CHECK (discount >= 0 AND discount <= 100),
    final_price NUMERIC(9,2) NOT NULL CHECK (final_price > 0)
);

-- Создание индексов для ускорения поиска
CREATE INDEX idx_models_brand_id ON car_shop.models(brand_id);
CREATE INDEX idx_cars_model_id ON car_shop.cars(model_id);
CREATE INDEX idx_cars_color_id ON car_shop.cars(color_id);
CREATE INDEX idx_sales_car_id ON car_shop.sales(car_id);
CREATE INDEX idx_sales_customer_id ON car_shop.sales(customer_id);
CREATE INDEX idx_sales_order_date ON car_shop.sales(order_date);
CREATE INDEX idx_brands_country_id ON car_shop.brands(country_id);

/*=========================================================================
                    заполнение таблиц новой схемы
==========================================================================*/

-- Заполнение таблицы countries (страны) из сырых данных
INSERT INTO car_shop.countries (name)
SELECT DISTINCT 
    CASE 
        WHEN TRIM(brand_origin) = '' OR brand_origin IS NULL OR LOWER(TRIM(brand_origin)) = 'null'
        THEN 'Не указана'
        ELSE TRIM(brand_origin)
    END as country_name
FROM raw_data.sales
ORDER BY country_name;

-- Заполнение таблицы colors (цвета) из сырых данных
INSERT INTO car_shop.colors (name)
SELECT DISTINCT 
    TRIM(SPLIT_PART(auto, ',', 2)) as color_name
FROM raw_data.sales
WHERE TRIM(SPLIT_PART(auto, ',', 2)) != ''
ORDER BY color_name;

-- Заполнение таблицы brands (бренды) из сырых данных
INSERT INTO car_shop.brands (name, country_id)
SELECT DISTINCT 
    SPLIT_PART(SPLIT_PART(auto, ' ', 1), ',', 1) as brand_name,
    c.id as country_id
FROM raw_data.sales s
JOIN car_shop.countries c ON 
    CASE 
        WHEN TRIM(s.brand_origin) = '' OR s.brand_origin IS NULL OR LOWER(TRIM(s.brand_origin)) = 'null'
        THEN 'Не указана'
        ELSE TRIM(s.brand_origin)
    END = c.name
ORDER BY brand_name;

-- Заполнение таблицы models (модели) из сырых данных
INSERT INTO car_shop.models (brand_id, name, gasoline_consumption)
SELECT DISTINCT 
    b.id as brand_id,
    TRIM(
        CASE 
            WHEN SPLIT_PART(SPLIT_PART(s.auto, ',', 1), SPLIT_PART(s.auto, ' ', 1), 2) = ''
            THEN 'Без названия'
            ELSE SPLIT_PART(SPLIT_PART(s.auto, ',', 1), SPLIT_PART(s.auto, ' ', 1), 2)
        END
    ) as model_name,
    s.gasoline_consumption
FROM raw_data.sales s
JOIN car_shop.brands b ON SPLIT_PART(SPLIT_PART(s.auto, ' ', 1), ',', 1) = b.name
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
INSERT INTO car_shop.cars (model_id, color_id)
SELECT DISTINCT 
    m.id as model_id,
    col.id as color_id
FROM raw_data.sales s
JOIN car_shop.brands b ON SPLIT_PART(SPLIT_PART(s.auto, ' ', 1), ',', 1) = b.name
JOIN car_shop.models m ON m.brand_id = b.id 
    AND TRIM(
        CASE 
            WHEN SPLIT_PART(SPLIT_PART(s.auto, ',', 1), SPLIT_PART(s.auto, ' ', 1), 2) = ''
            THEN 'Без названия'
            ELSE SPLIT_PART(SPLIT_PART(s.auto, ',', 1), SPLIT_PART(s.auto, ' ', 1), 2)
        END
    ) = m.name
    AND (s.gasoline_consumption = m.gasoline_consumption 
         OR (s.gasoline_consumption IS NULL AND m.gasoline_consumption IS NULL))
JOIN car_shop.colors col ON TRIM(SPLIT_PART(s.auto, ',', 2)) = col.name
ORDER BY m.id, col.id;

-- Заполнение таблицы sales (продажи) из сырых данных
INSERT INTO car_shop.sales (car_id, customer_id, order_date, base_price, discount, final_price)
SELECT 
    ca.id as car_id,
    cu.id as customer_id,
    s.order_date,
    s.price / (1 - COALESCE(s.discount, 0) / 100.0) as base_price,
    COALESCE(s.discount, 0) as discount,
    s.price as final_price
FROM raw_data.sales s
JOIN car_shop.brands b ON SPLIT_PART(SPLIT_PART(s.auto, ' ', 1), ',', 1) = b.name
JOIN car_shop.models m ON m.brand_id = b.id 
    AND TRIM(
        CASE 
            WHEN SPLIT_PART(SPLIT_PART(s.auto, ',', 1), SPLIT_PART(s.auto, ' ', 1), 2) = ''
            THEN 'Без названия'
            ELSE SPLIT_PART(SPLIT_PART(s.auto, ',', 1), SPLIT_PART(s.auto, ' ', 1), 2)
        END
    ) = m.name
    AND (s.gasoline_consumption = m.gasoline_consumption 
         OR (s.gasoline_consumption IS NULL AND m.gasoline_consumption IS NULL))
JOIN car_shop.colors col ON TRIM(SPLIT_PART(s.auto, ',', 2)) = col.name
JOIN car_shop.cars ca ON ca.model_id = m.id AND ca.color_id = col.id
JOIN car_shop.customers cu ON s.person = cu.full_name 
    AND (s.phone = cu.phone OR (s.phone IS NULL AND cu.phone IS NULL))
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
        WHEN c.name IS NULL OR c.name = 'Не указана'
        THEN 'страна не указана'
        ELSE c.name 
    END AS brand_origin,
    MAX(s.base_price) AS price_max,
    MIN(s.base_price) AS price_min
FROM car_shop.sales s
JOIN car_shop.cars car ON s.car_id = car.id
JOIN car_shop.models m ON car.model_id = m.id
JOIN car_shop.brands b ON m.brand_id = b.id
LEFT JOIN car_shop.countries c ON b.country_id = c.id
GROUP BY c.name
ORDER BY 
    CASE 
        WHEN c.name IS NULL OR c.name = 'Не указана'
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