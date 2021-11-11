﻿CREATE TABLE [dbo].[access_z309_tmeterreading_hash] (
    [N_PROP]              VARCHAR (100) NULL,
    [N_PROP_METE]         VARCHAR (100) NULL,
    [N_METE_READ]         VARCHAR (100) NULL,
    [C_METE_READ_TOLE]    VARCHAR (100) NULL,
    [C_METE_READ_TYPE]    VARCHAR (100) NULL,
    [C_METE_READ_CONS]    VARCHAR (100) NULL,
    [C_METE_READ_STAT]    VARCHAR (100) NULL,
    [C_METE_CANT_READ]    VARCHAR (100) NULL,
    [C_PDE_READ_METH]     VARCHAR (100) NULL,
    [Q_METE_READ]         VARCHAR (100) NULL,
    [D_METE_READ]         VARCHAR (100) NULL,
    [T_METE_READ_TIME]    VARCHAR (100) NULL,
    [Q_METE_READ_CONS]    VARCHAR (100) NULL,
    [Q_METE_READ_DAYS]    VARCHAR (100) NULL,
    [F_READ_COMM_CODE]    VARCHAR (100) NULL,
    [F_READ_COMM_FREE]    VARCHAR (100) NULL,
    [Q_PDE_HIGH_LOW]      VARCHAR (100) NULL,
    [Q_PDE_REEN_COUN]     VARCHAR (100) NULL,
    [F_PDE_AUXI_READ]     VARCHAR (100) NULL,
    [D_METE_READ_UPDA]    VARCHAR (100) NULL,
    [_DLRawZoneTimeStamp] DATETIME      NULL,
    [year]                VARCHAR (4)   NULL,
    [month]               VARCHAR (2)   NULL,
    [day]                 VARCHAR (2)   NULL
)
WITH (CLUSTERED COLUMNSTORE INDEX, DISTRIBUTION = HASH([N_PROP]));


GO
CREATE STATISTICS [N_PROP]
    ON [dbo].[access_z309_tmeterreading_hash]([N_PROP]);
