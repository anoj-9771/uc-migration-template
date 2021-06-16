CREATE TABLE [dbo].[DataProfile] (
    [batchid]       BIGINT         NULL,
    [taskId]        BIGINT         NULL,
    [datasetId]     BIGINT         NULL,
    [schemaName]    NVARCHAR (128) NULL,
    [tableName]     NVARCHAR (128) NULL,
    [columnName]    NVARCHAR (128) NULL,
    [rowCount]      BIGINT         NULL,
    [size]          FLOAT (53)     NULL,
    [type]          VARCHAR (128)  NULL,
    [columnLength]  INT            NULL,
    [min]           NVARCHAR (MAX) NULL,
    [max]           NVARCHAR (MAX) NULL,
    [avg]           FLOAT (53)     NULL,
    [stdev]         FLOAT (53)     NULL,
    [nullCount]     BIGINT         NULL,
    [distinctCount] BIGINT         NULL,
    [isPK]          BIT            DEFAULT ((0)) NULL,
    [loadDate]      DATETIME       DEFAULT ([CTL].[udf_getWAustDateTime](getdate())) NULL
);

