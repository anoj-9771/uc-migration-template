/****** Object:  StoredProcedure [STG_EDDIS].[create_table]    Script Date: 4/27/2021 3:06:56 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
ALTER PROC [STG_EDDIS].[create_table] @source_schema_name [NVARCHAR](255),@source_table_name [NVARCHAR](255),@print_cmd [VARCHAR](1),@run_cmd [VARCHAR](1) AS
/*
This stored procedure will:
1. get the column definition from STG_EDDIS.QLIK_TO_SYNAPSE_MAPPING
2. create table in STG_EDDIS.@source_schema_name _ @source_table_name (to store a full table copy)
2. create table in STG_EDDIS.@source_schema_name _ @source_table_name _CT (to store a change trackign file copy)
3. create table in EDDIS (with SCD2)
4. create current view in EDDIS (to diplay current view)

Date        Developer   Version  Comments
2021-03-14  mnewham     1.00     copied from [RAW_WASP].[create_table] and renamed to [STG_EDDIS].[create_table] to support Qlik sources

EXEC [STG_EDDIS].[create_table] @source_schema_name = 'CDC_ATTUNITY', @source_table_name = 'EDDIS_TICK_TOCK', @print_cmd = 'Y', @run_cmd = 'Y';
EXEC [STG_EDDIS].[create_table] @source_schema_name = 'EDDIS_OWNER', @source_table_name = 'CHANNELDATA', @print_cmd = 'Y', @run_cmd = 'N';

*/
BEGIN
  DECLARE @sql_string           VARCHAR(MAX);
  DECLARE @sql_string1          VARCHAR(MAX);
  DECLARE @column_definition    VARCHAR(MAX);
  DECLARE @column_names         VARCHAR(MAX);
  DECLARE @ods_metadata_columns VARCHAR(MAX);
  DECLARE @ct_metadata_columns VARCHAR(MAX);

  SET @ods_metadata_columns = 'HASHBYTES_all_columns char(64) NOT NULL, ODS_START_DATE [datetime2](7) NOT NULL, ODS_END_DATE [datetime2](7) NOT NULL, ODS_CURRENT_FLAG [varchar](1) NOT NULL, ';

  SET @ct_metadata_columns = '[header__change_seq] [varchar](35) NOT NULL, [header__change_oper] [varchar](1) NOT NULL, [header__change_mask] [nvarchar](max) NULL,	[header__stream_position] [varchar](128) NOT NULL,	[header__operation] [varchar](12) NOT NULL,	[header__transaction_id] [varchar](32) NOT NULL, [header__timestamp] [datetime2](6) NOT NULL, ';

  SET @column_definition = (
      SELECT
        STRING_AGG(
            (
                CASE
					WHEN a.[type] = 'FLOAT'
						THEN CONCAT('[', a.[name], ']', ' ', b.[SYNAPSE_DATA_TYPE])
--					WHEN a.[type] = 'NUMBER' AND a.[precision] IS NULL -- this is a NUMBER datatype in oracle - which is different to NUMBER(38,10)
--						THEN CONCAT('[', a.[name], ']', ' ', 'NUMERIC(38,10)')
					WHEN a.[scale] IS NOT NULL AND b.[SYNAPSE_DATA_TYPE] <> 'DATETIME2'
						THEN CONCAT('[', a.[name], ']', ' ', b.[SYNAPSE_DATA_TYPE], '(', ISNULL(a.[precision], 38), ',', a.[scale], ')')
					WHEN a.[precision] IS NOT NULL AND b.[SYNAPSE_DATA_TYPE] <> 'DATETIME2'
						THEN CONCAT('[', a.[name], ']', ' ', b.[SYNAPSE_DATA_TYPE], '(', a.[precision], ')')
					WHEN a.[length] IS NOT NULL AND a.[type] <> 'CLOB' AND b.[SYNAPSE_DATA_TYPE] <> 'DATETIME2'
						THEN CONCAT('[', a.[name], ']', ' ', b.[SYNAPSE_DATA_TYPE], '(', REPLACE(a.[length], '-1', 'MAX'), ')')
					ELSE CONCAT('[', a.[name], ']', ' ', b.[SYNAPSE_DATA_TYPE])
                END
                + /*CASE
					WHEN [NULLABLE] = 'N'
						THEN ' NOT NULL'
					ELSE ' NULL'
					END*/
					'  NULL' -- dfm file does not contain not null or nullable information :(

            ), ','
        ) WITHIN GROUP (ORDER BY [ordinal]) COLUMN_DEFINITION
    FROM [STG_EDDIS].[LOAD00000001_dfm_columns] a
    LEFT JOIN [STG_EDDIS].[QLIK_TO_SYNAPSE_MAPPING] b ON (a.[type] = b.[QLIK_DATA_TYPE])
    WHERE a.[sourceTable] = @source_table_name
    AND a.[sourceSchema] = @source_schema_name);

	SET @column_names = (
      SELECT
        STRING_AGG(
            (
                a.[name]
            ), ','
        ) WITHIN GROUP (ORDER BY [ordinal]) COLUMN_NAMES
    FROM [STG_EDDIS].[LOAD00000001_dfm_columns] a
    WHERE a.[sourceTable] = @source_table_name
    AND a.[sourceSchema] = @source_schema_name);

  IF (@print_cmd = 'Y')
  BEGIN
    PRINT ('@source_schema_name = ' + @source_schema_name);
	PRINT ('@source_table_name = ' + @source_table_name);
	PRINT ('@column_definition = ' + @column_definition);
	--SELECT @column_definition AS column_definition;
  END;


  -- create STAGE table
  SET @sql_string = 'CREATE TABLE [STG_EDDIS].[' + @source_schema_name + '_' + @source_table_name + '](' + @column_definition + ') WITH (DISTRIBUTION = ROUND_ROBIN, HEAP);';
  IF (@print_cmd = 'Y')
    PRINT @sql_string;
  IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE [TABLE_NAME] = @source_schema_name + '_' + @source_table_name AND [TABLE_SCHEMA] = 'STG_EDDIS')
  IF @run_cmd = 'Y'
  BEGIN
	EXEC (@sql_string);
  END;

  -- create STAGE CT table
  SET @sql_string = 'CREATE TABLE [STG_EDDIS].[' + @source_schema_name + '_' + @source_table_name + '__ct](' + @ct_metadata_columns + @column_definition + ') WITH (DISTRIBUTION = ROUND_ROBIN, HEAP);';
  IF (@print_cmd = 'Y')
    PRINT @sql_string;
  IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE [TABLE_NAME] = @source_schema_name + '_' + @source_table_name + '__ct' AND [TABLE_SCHEMA] = 'STG_EDDIS')
  IF @run_cmd = 'Y'	
  BEGIN
	EXEC (@sql_string);
  END;

  -- create SCD2 table
  SET @sql_string = 'CREATE TABLE [EDDIS].[' + @source_schema_name + '_' + @source_table_name + '](' + @ods_metadata_columns + @column_definition + ') WITH (HEAP, DISTRIBUTION = HASH ([HASHBYTES_all_columns]))';
  IF (@print_cmd = 'Y')
    PRINT @sql_string;
  IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE [TABLE_NAME] = @source_schema_name + '_' + @source_table_name AND [TABLE_SCHEMA] = 'EDDIS')
  IF @run_cmd = 'Y'
  BEGIN
    EXEC (@sql_string);
  END;

  -- create current VIEW table
  SET @sql_string = 'CREATE VIEW [EDDIS].[' + @source_schema_name + '_' + @source_table_name + '_curr_VW] AS SELECT ' + @column_names + ' FROM [EDDIS].[' + @source_schema_name + '_' + @source_table_name + '] WHERE [ODS_CURRENT_FLAG] = ''Y''';
  IF (@print_cmd = 'Y')
    PRINT @sql_string;
  IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE [TABLE_NAME] = @source_schema_name + '_' + @source_table_name + '_curr_VW' AND [TABLE_SCHEMA] = 'EDDIS')
  IF @run_cmd = 'Y'
  BEGIN
    EXEC (@sql_string);
  END;

END
