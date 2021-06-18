CREATE TABLE [compliance].[AvetmissLocation] (
    [AvetmissLocationSK]       BIGINT         NULL,
    [LocationID]               INT            NULL,
    [LocationCode]             NVARCHAR (6)   NULL,
    [LocationName]             NVARCHAR (30)  NULL,
    [LocationDescription]      NVARCHAR (150) NULL,
    [AddressLine1]             NVARCHAR (150) NULL,
    [AddressLine2]             NVARCHAR (150) NULL,
    [Suburb]                   NVARCHAR (100) NULL,
    [AusState]                 NVARCHAR (3)   NULL,
    [PostCode]                 INT            NULL,
    [Active]                   NVARCHAR (1)   NULL,
    [InstituteID]              NVARCHAR (6)   NOT NULL,
    [InstituteCode]            NVARCHAR (25)  NOT NULL,
    [InstituteName]            NVARCHAR (50)  NULL,
    [InstituteDescription]     NVARCHAR (50)  NULL,
    [RTOCode]                  NVARCHAR (50)  NOT NULL,
    [InstituteAddressLine1]    NVARCHAR (150) NULL,
    [InstituteAddressLine2]    NVARCHAR (150) NULL,
    [InstituteSuburb]          NVARCHAR (100) NULL,
    [InstituteAusState]        NVARCHAR (3)   NULL,
    [InstitutePostcode]        INT            NULL,
    [RegionID]                 NVARCHAR (4)   NOT NULL,
    [RegionName]               NVARCHAR (37)  NOT NULL,
    [_DLCuratedZoneTimeStamp]  DATETIME2 (7)  NULL,
    [_RecordStart]             DATETIME2 (7)  NULL,
    [_RecordEnd]               DATETIME2 (7)  NULL,
    [_RecordDeleted]           INT            NULL,
    [_RecordCurrent]           INT            NULL,
    [LocationSuburb]           NVARCHAR (100) NULL,
    [LocationPostCode]         INT            NULL,
    [LocationOrganisationCode] NVARCHAR (5)   NULL
);








GO


