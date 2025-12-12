/*
=========================================================
Create Database and Schemas
=========================================================
Script Purpose:
  This script creates a new databse nameed 'DataWarehouse' after checking if it already exists. 
  If the database exists, it is dropped and recreated. Additionally, the script set up three schemas 
  within the databse: 'bronze', 'silver', 'gold'.

WARNING:
  Running this script will drop the entier 'DataWarehouse' databse if it exits. 
  All data in the databse will be premanently delete. Proceed with caution and ensure you have proper backups 
  before executing this script.

  If you already have a database DataWarehouse in your system, then alternatively, you can create another data warehouse 
  database with another name as DataWarehouseDB and you can skip the code - IF EXISTS ......
*/

-- Create Databse 'DataWarehouse'
USE MASTER;
GO

--Drop and recreate the 'DataWarehouse' database
IF EXISTS (SELECT 1 FROM sys.databases WHERE name = 'DataWarehouse')
BEGIN
	ALTER DATABASE DataWarehouse SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
	DROP DATABASE DataWarehouse;
END;
GO

-- Create the 'DataWarehouse' database

CREATE DATABASE DataWarehouse;
GO

USE DataWarehouse;
GO

CREATE SCHEMA bronze;
GO

CREATE SCHEMA silver;
GO

CREATE SCHEMA gold;
GO
