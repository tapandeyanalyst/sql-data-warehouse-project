/*
=========================================================
Stored Procedure: Load Silver Layer (Source -> bronze)
=========================================================
Script Purpose:
  This stored procedure loads data into the 'silver' schema from the bronze layer
  It performs the following actinos:
  - Truncates the silver tables before loading data.
  - Uses INSERT INTO command to load data from bronze tables to silver tables upon tranformation.

  Parameters:
    None.
    This stored procedure does not accept any parameters or return any values.
  Usagwe Exampe:
    EXEC silver.load_silver;
*/

USE DataWarehouseDB;
GO
CREATE OR ALTER PROCEDURE silver.load_silver AS
BEGIN
    DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME;

    BEGIN TRY

        SET @batch_start_time = GETDATE();
        PRINT '=====================================';
		PRINT 'Loading Silver Layer';
		PRINT '=====================================';
        PRINT '-------------------------------------';
		PRINT 'Loading CRM Tables';
		PRINT '-------------------------------------';
-----------------------------------------------------------------------------crm_cust_info
        SET @start_time = GETDATE();
        PRINT '>>Truncating Table : silver.crm_cust_info';
        TRUNCATE TABLE silver.crm_cust_info;
        PRINT '>>Inserting data into table : silver.crm_cust_info';
        INSERT INTO silver.crm_cust_info 
        (
        cst_id,
        cst_key,
        cst_firstname,
        cst_lastname,
        cst_marital_status,
        cst_gndr,
        cst_create_date
        )

          SELECT 
  	        cst_id, 
            cst_key, 
            TRIM(cst_firstname) AS cst_firstname,
            TRIM(cst_lastname)	AS cst_lastname, 
            CASE WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married' 
		         WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
		        ELSE 'n/a' END AS cst_marital_status, -- Standardization, normalizing to readable format
	        CASE WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male' 
		         WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
		        ELSE 'n/a' END AS cst_gndr, -- Standardization, normalizing to readable format
            cst_create_date
  
          FROM 
          -- This section, we are fixing the duplicate and nulls in primary key
          (
            SELECT 
            cst_id, 
            cst_key, 
            cst_firstname,
            cst_lastname, 
            cst_marital_status, 
            cst_gndr, 
            cst_create_date, 
            ROW_NUMBER() OVER (Partition by cst_id ORDER BY cst_create_date DESC) as cst_rownum
	        FROM bronze.crm_cust_info
	        ) t
	        WHERE cst_rownum = 1 -- Select the most recent record per customer.
	        AND cst_id IS NOT NULL;
            SET @end_time = GETDATE();
            PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
            PRINT '>> ----------';
		    


-------------------------------------------------------------------------------------------------------crm_prd_info
            SET @start_time = GETDATE();
            PRINT '>>Truncating Table : silver.crm_prd_info';
            TRUNCATE TABLE silver.crm_prd_info;
            PRINT '>>Inserting data into table : silver.crm_prd_info';
            INSERT INTO silver.crm_prd_info 
            (
            prd_id,
            prd_key,
            cat_id, 
            sales_prd_key,
            prd_nm,
            prd_cost,
            prd_line, 
            prd_start_dt,
            prd_end_dt
            )
            SELECT 

	            prd_id,
	            prd_key,
	            Replace(SUBSTRING(prd_key, 1,5), '-','_') as cat_id, -- required to join with [erp_px_cat_g1v2]
	            SUBSTRING(prd_key, 7, LEN(prd_key)) as sales_prd_key, -- required to join with [crm_sales_details]
	            prd_nm,
	            ISNULL(prd_cost,0) as prd_cost, --Updated for NULLs
	            CASE WHEN UPPER(TRIM(prd_line)) = 'M' THEN 'Mountain'
		             WHEN UPPER(TRIM(prd_line)) = 'R' THEN 'Road'
		             WHEN UPPER(TRIM(prd_line)) = 'S' THEN 'Other Sales'
		             WHEN UPPER(TRIM(prd_line)) = 'T' THEN 'Touring'
		             ELSE 'n/a' END as prd_line, 
		             prd_start_dt,
		             DATEADD(DAY, -1, LEAD(prd_start_dt) OVER (Partition  BY prd_key order by prd_start_dt ASC)) as prd_end_dt -- This is becuase the product end date is smaller than the start date.
              FROM bronze.crm_prd_info;
              SET @end_time = GETDATE();
              PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
              PRINT '>> ----------';

---------------------------------------------------------------------------------------crm_sales_details
            SET @start_time = GETDATE();
            PRINT '>>Truncating Table : silver.crm_sales_details';
            TRUNCATE TABLE silver.crm_sales_details;

            PRINT '>>Inserting data into table : silver.crm_sales_details';
            INSERT INTO silver.crm_sales_details (

	            sls_ord_num,
	            sls_prd_key,
	            sls_cust_id	,
	            sls_order_dt,
	            sls_ship_dt,
	            sls_due_dt,
	            sls_sales,
	            sls_quantity,
	            sls_price		
            )

            SELECT
	            sls_ord_num,
                sls_prd_key,
                sls_cust_id,
	            CASE when sls_order_dt = 0 OR LEN(sls_order_dt) !=8 THEN NULL 
	            ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE) -- As the current sls_order_dt is in INT so we directly cannot convert it to DATE. We need to convert it to VARCHAR first then DATE
	            END as sls_order_dt,

	            CASE when sls_ship_dt = 0 OR LEN(sls_ship_dt) !=8 THEN NULL 
	            ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE) -- As the current sls_order_dt is in INT so we directly cannot convert it to DATE. We need to convert it to VARCHAR first then DATE
	            END AS sls_ship_dt,

	            CASE when sls_due_dt = 0 OR LEN(sls_due_dt) !=8 THEN NULL 
	            ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE) -- As the current sls_order_dt is in INT so we directly cannot convert it to DATE. We need to convert it to VARCHAR first then DATE
	            END as sls_due_dt,
   
                CASE WHEN sls_sales is null or sls_sales <=0 or sls_sales != ABS(sls_price) * sls_quantity 
	            THEN ABS(sls_price) * sls_quantity
	            ELSE sls_sales 
	            END AS sls_sales,
                sls_quantity,
	            CASE when sls_price is NULL or sls_price <=0
	            THEN sls_sales / NULLIF(sls_quantity, 0) 
	            ELSE sls_price
	            END AS sls_price
                FROM bronze.crm_sales_details;
                SET @end_time = GETDATE();
                PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
                PRINT '>> ----------';

--------------------------------------------------------------------------------------------erp_cust_az12
            SET @start_time = GETDATE();
            PRINT '>>Truncating Table : silver.erp_cust_az12';
            TRUNCATE TABLE silver.erp_cust_az12;
            PRINT '>>Inserting data into table : silver.erp_cust_az12';
            INSERT INTO silver.erp_cust_az12 (
            CID,
            BDATE,
            GEN
            )
            SELECT 
                CASE WHEN UPPER(CID) LIKE 'NAS%' THEN SUBSTRING(CID, 4, LEN(CID))
                ELSE CID
                END AS CID,
    
                CASE WHEN BDATE > GETDATE() THEN NULL
                ELSE BDATE 
                END AS BDATE,
    
                CASE WHEN (UPPER(TRIM(GEN)) = 'F' OR UPPER(TRIM(GEN)) = 'FEMALE') THEN 'Female'
	            WHEN (UPPER(TRIM(GEN)) = 'M' OR UPPER(TRIM(GEN)) = 'MALE') THEN 'Male'
	            ELSE 'n/a'
	            END AS GEN
              FROM bronze.erp_cust_az12;
              SET @end_time = GETDATE();
              PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';

----------------------------------------------------------------------------------------------------erp_loc_a101
            SET @start_time = GETDATE();
            PRINT '>>Truncating Table : silver.erp_loc_a101';
            TRUNCATE TABLE silver.erp_loc_a101;
            PRINT '>>Inserting data into table : silver.erp_loc_a101';
            INSERT INTO silver.erp_loc_a101 (
            CID,
            CNTRY
            )
            SELECT 
            REPLACE(CID, '-','') AS CID,
            CASE 
                  WHEN TRIM(CNTRY) = 'DE' THEN 'Germany'
                  WHEN TRIM(CNTRY) IN ('USA','US') THEN 'United States'
                  WHEN (TRIM(CNTRY) = '' OR CNTRY IS NULL) THEN 'n/a'
                  ELSE TRIM(CNTRY)
                  END AS CNTRY
            FROM bronze.erp_loc_a101;
            SET @end_time = GETDATE();
            PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
            PRINT '>> ----------';

------------------------------------------------------------------------------------------------------erp_px_cat_g1v2
            
            SET @start_time = GETDATE();
            PRINT '>>Truncating Table : silver.erp_px_cat_g1v2';
            TRUNCATE TABLE silver.erp_px_cat_g1v2;
            PRINT '>>Inserting data into table : silver.erp_px_cat_g1v2';
            INSERT INTO silver.erp_px_cat_g1v2 (
            ID,
            CAT,
            SUBCAT,
            MAINTENANCE
            )

            SELECT ID
                  ,CAT
                  ,SUBCAT
                  ,MAINTENANCE
              FROM bronze.erp_px_cat_g1v2;
              SET @end_time = GETDATE();
              PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
              PRINT '>> ----------';


              SET @batch_end_time = GETDATE();
		      PRINT '>> *********************************';
		      PRINT '>> Loading Silver Layer is Completed';
		      PRINT '>> Total Duration: ' + CAST(DATEDIFF(second, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' seconds';

              END TRY

	            BEGIN CATCH
		            PRINT '=========================================='
		            PRINT 'ERROR OCCURED DURING LOADING SILVER LAYER'
		            PRINT 'Error Message' + ERROR_MESSAGE();
		            PRINT 'Error Message' + CAST(ERROR_NUMBER() AS NVARCHAR);
		            PRINT 'Error Message' + CAST(ERROR_STATE() AS NVARCHAR);
		            PRINT '=========================================='
	            END CATCH

  END;

