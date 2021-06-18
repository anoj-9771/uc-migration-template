CREATE TABLE [edw].[VCPexip_conference] (
    [duration]                BIGINT         NULL,
    [end_time]                DATETIME       NULL,
    [id]                      NVARCHAR (MAX) NULL,
    [instant_message_count]   BIGINT         NULL,
    [name]                    NVARCHAR (MAX) NULL,
    [participant_count]       BIGINT         NULL,
    [resource_uri]            NVARCHAR (MAX) NULL,
    [service_type]            NVARCHAR (MAX) NULL,
    [start_time]              NVARCHAR (MAX) NULL,
    [tag]                     NVARCHAR (MAX) NULL,
    [year]                    NVARCHAR (MAX) NULL,
    [month]                   NVARCHAR (MAX) NULL,
    [day]                     NVARCHAR (MAX) NULL,
    [_DLRawZoneTimeStamp]     DATETIME2 (7)  NULL,
    [_DLTrustedZoneTimeStamp] DATETIME2 (7)  NULL,
    [_RecordStart]            DATETIME2 (7)  NULL,
    [_RecordEnd]              DATETIME2 (7)  NULL,
    [_RecordDeleted]          INT            NULL,
    [_RecordCurrent]          INT            NULL
);



