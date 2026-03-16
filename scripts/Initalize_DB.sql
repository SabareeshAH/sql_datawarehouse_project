/*
=========================================================================
Create Database and Schemas
=========================================================================

Purpose: This part of sql file / code involves creating a new database 
"Data_Warehouse" database for our project and also involves creating
three new schemas namely bronze, silver, gold for the project.

Warning: Ensure that database and schemas are not existing already if so
you can make use of the existing one and skips this procedure. Again
creating of database and schemas with the same existing name results in 
warning or error.
*/

-- creating the requuired database for the dataware house project.
create database "Data_Warehouse";

-- checking whether we are in correct database or not.
SELECT current_database();

-- creating the required schemas for the project, schema is like folder which helps to keep organized our database for efficient usage.
-- in this case we are creating 3 schemas namely bronze, silver, gold representing each layer.
create schema bronze;
create schema silver;
create schema gold;

