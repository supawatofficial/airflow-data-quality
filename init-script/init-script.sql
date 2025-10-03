-- E-commerce Transactions Database Init Script
-- สำหรับ Data Engineering Project - Airflow Data Quality
-- Script นี้มีปัญหา data quality เจตนาเพื่อการทดสอบ
-- Drop table if exists
DROP TABLE IF EXISTS transactions;
-- Create transactions table
CREATE TABLE transactions (
    transaction_id VARCHAR(20),
    customer_id VARCHAR(15) NOT NULL,
    transaction_date VARCHAR(50) NOT NULL, -- Changed to VARCHAR to allow different formats
    product_id VARCHAR(15) NOT NULL,
    product_category VARCHAR(50) NOT NULL,
    quantity INT,  -- Removed NOT NULL to allow missing values
    price DECIMAL(10,2), -- Removed NOT NULL to allow missing values
    discount DECIMAL(10,2) DEFAULT 0.00,
    amount DECIMAL(10,2) NOT NULL,
    payment_method VARCHAR(30) NOT NULL,
    shipping_address TEXT NOT NULL,
    status VARCHAR(20) NOT NULL
);

-- Insert sample data with data quality issues
INSERT INTO transactions (transaction_id, customer_id, transaction_date, product_id, product_category, quantity, price, discount, amount, payment_method, shipping_address, status) VALUES
-- Bangkok & Metropolitan Area (40% of data) - Various date formats and data issues
('TXN202507010001', 'CUST000001', '2025-07-01', 'PRD001', 'Electronics', 1, 15990.00, 500.00, 15490.00, 'Credit Card', '123/45 Sukhumvit Rd., Khlong Toei, Khlong Toei, Bangkok 10110', 'completed'),
('TXN202507010002', 'CUST000002', '2025/07/01', 'PRD002', 'Fashion', 2, 1290.00, 0.00, 2580.00, 'PromptPay', '456/12 Ratchadamnoen Rd., Phra Nakhon, Phra Nakhon, Bangkok 10200', 'completed'),
('TXN202507010003', 'CUST000003', 'July 01, 2025', 'PRD003', 'Beauty', NULL, 890.00, 89.00, 801.00, 'TrueMoney', '789/34 Phahonyothin Rd., Samsen Nai, Phaya Thai, Bangkok 10400', 'completed'), -- Missing quantity
('TXN202507010004', 'CUST000004', '2025-07-01', 'PRD004', 'Food & Beverage', 3, 450.00, 45.00, 1200.00, 'Bank Transfer', '321/67 Charoen Krung Rd., Wat Phraya Krai, Bang Kho Laem, Bangkok 10120', 'completed'), -- Wrong amount calculation: should be (450*3)-45 = 1305
('TXN202507010005', 'CUST000005', '2025/07/01', 'PRD005', 'Home & Living', 1, NULL, 259.00, 2331.00, 'Cash on Delivery', '654/89 Soi Lat Phrao 15, Chom Phon, Chatuchak, Bangkok 10900', 'completed'), -- Missing price

('TXN202507020001', 'CUST000001', 'July 02, 2025', 'PRD006', 'Sports', 1, 3490.00, 0.00, 3490.00, 'Credit Card', '123/45 Sukhumvit Rd., Khlong Toei, Khlong Toei, Bangkok 10110', 'completed'),
('TXN202507020002', 'CUST000006', '2025-07-02', 'PRD007', 'Electronics', 1, 8990.00, 899.00, 8091.00, 'PromptPay', '147/8 Tiwanon Rd., Bang Talat, Pak Kret, Nonthaburi 11120', 'completed'),
('TXN202507020003', 'CUST000007', '2025/07/02', 'PRD008', 'Fashion', 2, 2190.00, 219.00, 4500.00, 'Bank Transfer', '258/3 Borommaratchachonnani Rd., Bang Bamru, Bang Phlat, Bangkok 10700', 'completed'), -- Wrong amount: should be (2190*2)-219 = 4161
('TXN202507020004', 'CUST000008', 'July 02, 2025', 'PRD009', 'Beauty', NULL, NULL, 159.00, 1431.00, 'TrueMoney', '369/12 Rama IV Rd., Khlong Tan, Khlong Toei, Bangkok 10110', 'completed'), -- Missing quantity and price
('TXN202507020005', 'CUST000009', '2025-07-02', 'PRD010', 'Food & Beverage', 5, 290.00, 0.00, 1450.00, 'Cash on Delivery', '741/25 Vibhavadi Rangsit Rd., Din Daeng, Din Daeng, Bangkok 10400', 'pending'),

-- Chiang Mai (Major city) - More date format variations
('TXN202507030001', 'CUST000010', '2025/07/03', 'PRD011', 'Electronics', 1, 12990.00, 1299.00, 11691.00, 'Credit Card', '456/78 Moo 3, Nong Pa Khrang, Mueang, Chiang Mai 50000', 'completed'),
('TXN202507030002', 'CUST000011', 'July 03, 2025', 'PRD012', 'Fashion', 1, 1890.00, 0.00, 1890.00, 'PromptPay', '123/9 Huay Kaew Rd., Chang Khian, Mueang, Chiang Mai 50100', 'completed'),
('TXN202507030003', 'CUST000010', '2025-07-03', 'PRD013', 'Beauty', 2, 690.00, 69.00, 1400.00, 'Credit Card', '456/78 Moo 3, Nong Pa Khrang, Mueang, Chiang Mai 50000', 'completed'), -- Wrong amount: should be (690*2)-69 = 1311
('TXN202507030004', 'CUST000012', '2025/07/03', 'PRD014', 'Food & Beverage', 4, 350.00, 35.00, 1365.00, 'Bank Transfer', '789/45 Chiang Mai-Lampang Rd., Pa Daet, Mueang, Chiang Mai 50100', 'completed'),

-- Phuket (Tourist area)
('TXN202507040001', 'CUST000013', 'July 04, 2025', 'PRD015', 'Sports', 1, 4590.00, 459.00, 4131.00, 'Credit Card', '321 Moo 3, Talat Yai, Mueang, Phuket 83000', 'completed'),
('TXN202507040002', 'CUST000014', '2025-07-04', 'PRD016', 'Fashion', NULL, 2590.00, 0.00, 2590.00, 'PromptPay', '654/12 Ratsada Rd., Ratsada, Mueang, Phuket 83000', 'completed'), -- Missing quantity
('TXN202507040003', 'CUST000013', '2025/07/04', 'PRD017', 'Beauty', 1, 1290.00, 129.00, 1161.00, 'Credit Card', '321 Moo 3, Talat Yai, Mueang, Phuket 83000', 'completed'),

-- Khon Kaen (Northeast)
('TXN202507050001', 'CUST000015', 'July 05, 2025', 'PRD018', 'Electronics', 1, 6990.00, 699.00, 6291.00, 'Bank Transfer', '654/78 Mittraphap Rd., Nai Mueang, Mueang, Khon Kaen 40000', 'completed'),
('TXN202507050002', 'CUST000016', '2025-07-05', 'PRD019', 'Home & Living', 1, 3290.00, 329.00, 2961.00, 'PromptPay', '147/25 Kalapapruek Rd., Ban Pet, Mueang, Khon Kaen 40000', 'completed'),
('TXN202507050003', 'CUST000015', '2025/07/05', 'PRD020', 'Food & Beverage', 3, NULL, 42.00, 1218.00, 'Bank Transfer', '654/78 Mittraphap Rd., Nai Mueang, Mueang, Khon Kaen 40000', 'completed'), -- Missing price

-- Continue with more diverse data and quality issues
('TXN202507060001', 'CUST000017', 'July 06, 2025', 'PRD021', 'Fashion', 2, 1590.00, 0.00, 3100.00, 'Cash on Delivery', '258/14 Thepharak Rd., Bang Mot, Mueang, Samut Prakan 10270', 'completed'), -- Wrong amount: should be 3180
('TXN202507060002', 'CUST000018', '2025-07-06', 'PRD022', 'Electronics', 1, 9990.00, 999.00, 8991.00, 'Credit Card', '369/87 Srinagarindra Rd., Nong Bon, Prawet, Bangkok 10250', 'completed'), -- Wrong month: should be 07 not 06
('TXN202507060003', 'CUST000002', '2025/07/06', 'PRD023', 'Beauty', 1, 790.00, 79.00, 711.00, 'PromptPay', '456/12 Ratchadamnoen Rd., Phra Nakhon, Phra Nakhon, Bangkok 10200', 'completed'), -- Fixed invalid date

-- Hat Yai (South)
('TXN202507070001', 'CUST000019', 'July 07, 2025', 'PRD024', 'Sports', 1, 2890.00, 289.00, 2601.00, 'Bank Transfer', '741/36 Phetkasem Rd., Hat Yai, Hat Yai, Songkhla 90110', 'completed'),
('TXN202507070002', 'CUST000020', '2025-07-07', 'PRD025', 'Food & Beverage', 6, 180.00, 0.00, 1080.00, 'PromptPay', '852/14 Rat Yindi Rd., Kho Hong, Hat Yai, Songkhla 90110', 'completed'),

-- Udon Thani (Northeast)
('TXN202507080001', 'CUST000021', '2025/07/08', 'PRD026', 'Home & Living', 1, 4590.00, 459.00, 4131.00, 'Cash on Delivery', '963/25 Thanakit Rd., Mak Khaeng, Mueang, Udon Thani 41000', 'pending'),
('TXN202507080002', 'CUST000022', 'July 08, 2025', 'PRD027', 'Electronics', NULL, 7490.00, 749.00, 6741.00, 'Credit Card', '159/73 Udon Thani-Nong Khai Rd., Ban Lueam, Mueang, Udon Thani 41000', 'completed'), -- Missing quantity

-- More Bangkok addresses with repeat customers and more issues
('TXN202507090001', 'CUST000001', '2025-07-09', 'PRD028', 'Fashion', 1, 2190.00, 219.00, 1971.00, 'Credit Card', '123/45 Sukhumvit Rd., Khlong Toei, Khlong Toei, Bangkok 10110', 'completed'),
('TXN202507090002', 'CUST000023', '2025/07/09', 'PRD029', 'Beauty', 2, NULL, 0.00, 2380.00, 'PromptPay', '487/69 Ramkhamhaeng Rd., Hua Mak, Bang Kapi, Bangkok 10240', 'completed'), -- Missing price
('TXN202507090003', 'CUST000024', 'July 09, 2025', 'PRD030', 'Food & Beverage', 4, 320.00, 32.00, 1300.00, 'TrueMoney', '678/21 Pracha Uthit Rd., Thung Khru, Thung Khru, Bangkok 10140', 'completed'), -- Wrong amount: should be 1248

-- More problematic data
('TXN202507100001', 'CUST000010', '2025-07-10', 'PRD031', 'Sports', 1, 1890.00, 0.00, 1890.00, 'Credit Card', '456/78 Moo 3, Nong Pa Khrang, Mueang, Chiang Mai 50000', 'completed'), -- Fixed empty date
('TXN202507100002', 'CUST000006', '2025/07/10', 'PRD032', 'Home & Living', 0, 2890.00, 289.00, 2601.00, 'PromptPay', '147/8 Tiwanon Rd., Bang Talat, Pak Kret, Nonthaburi 11120', 'completed'), -- Zero quantity but positive amount
('TXN202507100003', 'CUST000013', 'July 10, 2025', 'PRD033', 'Electronics', 1, 5490.00, 549.00, 4941.00, 'Credit Card', '321 Moo 3, Talat Yai, Mueang, Phuket 83000', 'completed'),

-- Edge cases and more data quality issues
('TXN202507110001', 'CUST000025', '2025-07-11', 'PRD034', 'Fashion', -1, 1690.00, 169.00, 1521.00, 'Bank Transfer', '234/56 Charoen Mit Rd., Wat Bot, Mueang, Phitsanulok 65000', 'completed'), -- Negative quantity
('TXN202507110002', 'CUST000026', '2025/07/11', 'PRD035', 'Beauty', 1, -990.00, 99.00, 891.00, 'PromptPay', '567/89 Bypass Rd., Nai Mueang, Mueang, Ubon Ratchathani 34000', 'completed'), -- Negative price

-- Rayong (Eastern Seaboard)
('TXN202507120001', 'CUST000027', 'July 12, 2025', 'PRD036', 'Sports', 1, 3290.00, 0.00, 3290.00, 'Cash on Delivery', '789/12 Sukhumvit Rd., Noen Phra, Mueang, Rayong 21000', 'completed'),
('TXN202507120002', 'CUST000028', '2025-07-12', 'PRD037', 'Electronics', 1, 11990.00, 1199.00, 12000.00, 'Credit Card', '345/67 Rayong-Ban Chang Rd., Pak Nam, Mueang, Rayong 21000', 'completed'), -- Wrong amount: should be 10791

-- Some correct data for comparison
('TXN202507130001', 'CUST000029', '2025/07/13', 'PRD038', 'Home & Living', 1, 2190.00, 219.00, 1971.00, 'PromptPay', '456/23 Phahonyothin Rd., Wiang, Mueang, Chiang Rai 57000', 'completed'),
('TXN202507130002', 'CUST000030', 'July 13, 2025', 'PRD039', 'Food & Beverage', 3, 480.00, 48.00, 1392.00, 'Bank Transfer', '678/91 Chiang Rai-Phan Rd., Ban Du, Mueang, Chiang Rai 57100', 'completed'),

-- Future dates (data quality issue)
('TXN202507140001', 'CUST000002', '2026-07-14', 'PRD040', 'Electronics', 1, 8990.00, 899.00, 8091.00, 'PromptPay', '456/12 Ratchadamnoen Rd., Phra Nakhon, Phra Nakhon, Bangkok 10200', 'completed'), -- Future date
('TXN202507140002', 'CUST000031', '2025/07/14', 'PRD041', 'Fashion', 2, 1490.00, 0.00, 2980.00, 'TrueMoney', '123/78 Ekachai Rd., Bang Bon, Bang Bon, Bangkok 10150', 'completed'),

-- Missing values and calculation errors continue...
('TXN202507150001', 'CUST000032', 'July 15, 2025', 'PRD043', 'Sports', NULL, NULL, 259.00, 2331.00, 'Bank Transfer', '789/34 Mittraphap Rd., Nai Mueang, Mueang, Nakhon Ratchasima 30000', 'completed'), -- Missing quantity and price
('TXN202507150002', 'CUST000033', '2025-07-15', 'PRD044', 'Home & Living', 1, 3890.00, 389.00, 3600.00, 'Credit Card', '456/12 Suranarai Rd., Nong Bua Sala, Mueang, Nakhon Ratchasima 30000', 'completed'), -- Wrong amount: should be 3501

-- Very old dates (historical data quality issue)
('TXN202507160001', 'CUST000034', '1990/01/01', 'PRD045', 'Fashion', 1, 1890.00, 0.00, 1890.00, 'PromptPay', '234/67 Saeng Chuto Rd., Ban Nuea, Mueang, Kanchanaburi 71000', 'completed'), -- Very old date
('TXN202507160002', 'CUST000035', 'July 16, 2025', 'PRD046', 'Electronics', 1, 6490.00, 649.00, 5841.00, 'Cash on Delivery', '567/89 Lat Ya Rd., Pak Phraek, Mueang, Kanchanaburi 71000', 'pending'),

-- Final entries with various issues
('TXN202507170001', 'CUST000015', '2025-07-17', 'PRD047', 'Beauty', 2, 890.00, 89.00, 1700.00, 'Bank Transfer', '654/78 Mittraphap Rd., Nai Mueang, Mueang, Khon Kaen 40000', 'completed'), -- Wrong amount: should be 1691
('TXN202507170002', 'CUST000019', '2025/07/17', 'PRD048', 'Food & Beverage', 5, 290.00, 0.00, 1450.00, 'Bank Transfer', '741/36 Phetkasem Rd., Hat Yai, Hat Yai, Songkhla 90110', 'completed'), -- Fixed NULL date
('TXN202507170003', 'CUST000036', 'July 17, 2025', 'PRD049', 'Sports', 1, 4190.00, 419.00, 3771.00, 'Credit Card', '890/12 Rama 2 Rd., Bang Mot, Mueang, Samut Sakhon 74000', 'completed');