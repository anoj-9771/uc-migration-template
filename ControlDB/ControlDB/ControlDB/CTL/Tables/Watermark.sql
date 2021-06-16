﻿CREATE TABLE [CTL].[Watermark] (
    [WatermarkId] BIGINT        IDENTITY (1, 1) NOT NULL,
    [DatasetId]   BIGINT        NULL,
    [Column]      VARCHAR (250) NULL,
    [Operator]    VARCHAR (5)   NULL,
    [TypeId]      INT           NULL,
    [Value]       VARCHAR (500) NULL,
    [DataTypeId]  BIGINT        NULL,
    [Format]      VARCHAR (100) NULL,
    [BatchGrain]  VARCHAR (20)  NULL
);



