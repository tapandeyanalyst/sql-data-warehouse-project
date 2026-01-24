/*
=========================================================
Create Database and Schemas
=========================================================
Script Purpose:
  This script creates a new database named 'DataWarehouseDB' after checking if it already exists. 
  If the database exists, it is dropped and recreated. Additionally, the script set up three schemas 
  within the databse: 'bronze', 'silver', 'gold'.

WARNING:
  Running this script will drop the entier 'DataWarehouse' databse if it exits. 
  All data in the databse will be premanently delete. Proceed with caution and ensure you have proper backups 
  before executing this script.

  If you already have a database DataWarehouse in your system, then alternatively, you can create another data warehouse 
  database with another name as DataWarehouseDB and you can skip the code - IF EXISTS ......
*/

-- Creating a Database named 'DataWarehouseDB'
USE MASTER;
GO
IF EXISTS (SELECT 1 FROM sys.databases WHERE name = 'DataWarehouseDB') --Drop and recreate the 'DataWarehouseDB' database if exist
BEGIN
	ALTER DATABASE DataWarehouseDB SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
	DROP DATABASE DataWarehouseDB;
END;
GO
CREATE DATABASE DataWarehouseDB; -- Create the 'DataWarehouseDB' database
GO
USE DataWarehouseDB;
GO
CREATE SCHEMA bronze;
GO
CREATE SCHEMA silver;
GO
CREATE SCHEMA gold;
GO
