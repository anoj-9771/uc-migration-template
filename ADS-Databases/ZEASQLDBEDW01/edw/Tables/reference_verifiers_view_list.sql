CREATE TABLE [edw].[reference_verifiers_view_list] (
    [friendly_name]           NVARCHAR (25)   NULL,
    [internal_name]           NVARCHAR (39)   NULL,
    [rv_domain]               NVARCHAR (29)   NULL,
    [low_value]               NVARCHAR (38)   NULL,
    [abbreviation]            NVARCHAR (24)   NULL,
    [fes_short_description]   NVARCHAR (40)   NULL,
    [fes_long_description]    NVARCHAR (54)   NULL,
    [property_value]          NVARCHAR (26)   NULL,
    [ops_master]              NVARCHAR (2000) NULL,
    [property_names]          NVARCHAR (34)   NULL,
    [_DLRawZoneTimeStamp]     DATETIME2 (7)   NULL,
    [_DLTrustedZoneTimeStamp] DATETIME2 (7)   NULL,
    [_RecordStart]            DATETIME2 (7)   NULL,
    [_RecordEnd]              DATETIME2 (7)   NULL,
    [_RecordCurrent]          INT             NULL,
    [_RecordDeleted]          INT             NULL
);



