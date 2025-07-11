-- run this on redshift

CREATE DATABASE sales_classic_cars;

CREATE SCHEMA raw_zone;

-- DROP TABLE IF EXISTS raw_zone.customers;
-- DROP TABLE IF EXISTS raw_zone.orders;


/* Create the tables */
CREATE TABLE raw_zone.productlines (
    productLine varchar(50) PRIMARY KEY NOT NULL,
    textDescription varchar(4000) DEFAULT NULL,
    htmlDescription varchar(max),
    image varchar(max)
);

CREATE TABLE raw_zone.products (
    productCode varchar(15) PRIMARY KEY NOT NULL,
    productName varchar(70),
    productLine varchar(50) NOT NULL,
    productScale varchar(10),
    productVendor varchar(50),
    productDescription varchar(max),
    quantityInStock int,
    buyPrice decimal(10,2),
    MSRP decimal(10,2)
);

CREATE TABLE raw_zone.customers (
    customerNumber int PRIMARY KEY NOT NULL,
    customerName varchar(50),
    contactLastName varchar(50),
    contactFirstName varchar(50),
    phone varchar(50),
    addressLine1 varchar(50),
    addressLine2 varchar(50) DEFAULT NULL,
    city varchar(50),
    state varchar(50) DEFAULT NULL,
    postalCode varchar(15) DEFAULT NULL,
    country varchar(50),
    salesRepEmployeeNumber int DEFAULT NULL,
    creditLimit decimal(10,2) DEFAULT NULL
);

CREATE TABLE raw_zone.orders (
    orderNumber int PRIMARY KEY NOT NULL,
    orderDate date,
    requiredDate date,
    shippedDate date,
    status varchar(15),
    comments varchar(max),
    customerNumber int NOT NULL
);

CREATE TABLE raw_zone.orderdetails (
    orderNumber int NOT NULL,
    productCode varchar(15) NOT NULL,
    quantityOrdered int,
    priceEach decimal(10,2),
    orderLineNumber int NOT NULL,
    orderDate date
);

CREATE TABLE raw_zone.tmp_productlines AS SELECT * FROM raw_zone.productlines;
CREATE TABLE raw_zone.tmp_products AS SELECT * FROM raw_zone.products;
CREATE TABLE raw_zone.tmp_customers AS SELECT * FROM raw_zone.customers;
CREATE TABLE raw_zone.tmp_orders AS SELECT * FROM raw_zone.orders;
CREATE TABLE raw_zone.tmp_orderdetails AS SELECT * FROM raw_zone.orderdetails;

CREATE SCHEMA processed_zone;

CREATE TABLE processed_zone.fact_sales (
    orderNumber int,
    orderLineNumber int,
    customerNumber int,
    productCode varchar(15),
    orderDate date,
    requiredDate date,
    shippedDate date,
    status varchar(15),
    comments varchar(max),
    quantityOrdered int,
    priceEach decimal(10,2),
    totalPrices decimal(10,2)
);

CREATE TABLE processed_zone.dim_customers (
    customerNumber int,
    customerName varchar(50),
    contactLastName varchar(50),
    contactFirstName varchar(50),
    phone varchar(50),
    addressLine1 varchar(50),
    addressLine2 varchar(50),
    city varchar(50),
    state varchar(50),
    postalCode varchar(15),
    country varchar(50),
    creditLimit decimal(10,2)
);

CREATE TABLE processed_zone.dim_products (
    productCode varchar(15),
    productName varchar(70),
    productLine varchar(50),
    productScale varchar(10),
    productVendor varchar(50),
    productDescription varchar(max),
    quantityInStock int,
    buyPrice decimal(10,2),
    MSRP decimal(10,2),
    textDescriptionProductLine varchar(4000)
);