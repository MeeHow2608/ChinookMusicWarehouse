-- Script for loading data into staging tables for dedicated SQL Pool in ASA

-- Create schemas, external data source and external data format

CREATE SCHEMA raw;
GO;
CREATE SCHEMA stage;
GO;



CREATE EXTERNAL DATA SOURCE chinook_music WITH 
(	LOCATION = N'https://<storageaccount>.dfs.core.windows.net/files/'
);
GO;


CREATE EXTERNAL FILE FORMAT ParquetFormat
WITH (
	FORMAT_TYPE = PARQUET, 
	DATA_COMPRESSION = 'org.apache.hadoop.io.compress.SnappyCodec'
)
GO;


-- Creating external tables based on parquet files from pyspark notebook
CREATE EXTERNAL TABLE raw.Track
(
    AlbumId INT,
    Bytes BIGINT,
    Composer NVARCHAR(255),
    GenreId INT,
    MediaTypeId INT,
    Milliseconds BIGINT,
    Name NVARCHAR(255),
    TrackId INT,
    UnitPrice DECIMAL(10,2)
) WITH 
(
    LOCATION = 'dataframes/track.parquet',
    DATA_SOURCE = chinook_music,
    FILE_FORMAT = ParquetFormat
);


CREATE EXTERNAL TABLE raw.Album
(
    AlbumId INT,
    ArtistId INT,
    Title NVARCHAR(255)
) WITH 
(
    LOCATION = 'dataframes/album.parquet',
    DATA_SOURCE = chinook_music,
    FILE_FORMAT = ParquetFormat
);


CREATE EXTERNAL TABLE raw.Artist
(
    ArtistId INT,
    Name NVARCHAR(255)
) WITH 
(
    LOCATION = 'dataframes/artist.parquet',
    DATA_SOURCE = chinook_music,
    FILE_FORMAT = ParquetFormat
);

CREATE EXTERNAL TABLE raw.PlaylistTrack
(
    PlaylistId INT,
    TrackId INT
) WITH 
(
    LOCATION = 'dataframes/playlisttrack.parquet',
    DATA_SOURCE = chinook_music,
    FILE_FORMAT = ParquetFormat
);



CREATE EXTERNAL TABLE raw.Playlist
(
    Name NVARCHAR(255),
    PlaylistId INT
) WITH 
(
    LOCATION = 'dataframes/playlist.parquet',
    DATA_SOURCE = chinook_music,
    FILE_FORMAT = ParquetFormat
);

CREATE EXTERNAL TABLE raw.MediaType
(
    MediaTypeId INT,
    Name NVARCHAR(255)
) WITH 
(
    LOCATION = 'dataframes/mediatype.parquet',
    DATA_SOURCE = chinook_music,
    FILE_FORMAT = ParquetFormat
);


CREATE EXTERNAL TABLE raw.Genre
(
    GenreId INT,
    Name NVARCHAR(255)
) WITH 
(
    LOCATION = 'dataframes/genre.parquet',
    DATA_SOURCE = chinook_music,
    FILE_FORMAT = ParquetFormat
);


CREATE EXTERNAL TABLE raw.InoviceLine
(
    InvoiceId INT,
    InvoiceLineId INT,
    Quantity INT,
    TrackId INT,
    UnitPrice DECIMAL(10,2)
) WITH 
(
    LOCATION = 'dataframes/invoiceline.parquet',
    DATA_SOURCE = chinook_music,
    FILE_FORMAT = ParquetFormat
);

CREATE EXTERNAL TABLE raw.Invoice
(
    BillingAddress NVARCHAR(255),
    BillingCity NVARCHAR(255),
    BillingCountry NVARCHAR(255),
    BillingPostalCode NVARCHAR(255),
    BillingState NVARCHAR(255),
    CustomerId INT,
    InvoiceDate DATE,
    InvoiceId INT,
    Total DECIMAL(10,2)
) WITH 
(
    LOCATION = 'dataframes/invoice.parquet',
    DATA_SOURCE = chinook_music,
    FILE_FORMAT = ParquetFormat
);


CREATE EXTERNAL TABLE raw.Customer
(

    Address NVARCHAR(255),
    City NVARCHAR(255),
    Company NVARCHAR(255),
    Country NVARCHAR(255),
    CustomerId INT,
    Email NVARCHAR(255),
    Fax NVARCHAR(255),
    FirstName NVARCHAR(255),
    LastName NVARCHAR(255),
    Phone NVARCHAR(255),
    PostalCode NVARCHAR(255),
    [State] NVARCHAR(255),
    SupportRepId INT

) WITH 
(
    LOCATION = 'dataframes/customer.parquet',
    DATA_SOURCE = chinook_music,
    FILE_FORMAT = ParquetFormat
);


CREATE EXTERNAL TABLE raw.Employee
(

    Address NVARCHAR(255),
    BirthDate DATE,
    City NVARCHAR(255),
    Country NVARCHAR(255),
    Email NVARCHAR(255),
    EmployeeId INT,
    Fax NVARCHAR(255),
    FirstName NVARCHAR(255),
    HireDate DATE,
    LastName NVARCHAR(255),
    Phone NVARCHAR(255),
    PostalCode NVARCHAR(255),
    ReportsTo INT,
    [State] NVARCHAR(255),
    Title NVARCHAR(255)

) WITH 
(
    LOCATION = 'dataframes/employee.parquet',
    DATA_SOURCE = chinook_music,
    FILE_FORMAT = ParquetFormat
);





-- Creating stage tables based on external raw tables
CREATE TABLE stage.DimTrack WITH (DISTRIBUTION = ROUND_ROBIN, HEAP) AS
SELECT 
    track.trackId as TrackId,
    track.Name as TrackName,
    album.Title as AlbumTitle,
    mediatype.Name as MediaType,
    genre.Name as Genre,
    track.Composer as Composer,
    artist.Name as Artist,
    track.Milliseconds as Milliseconds,
    track.Bytes as Bytes,
    track.UnitPrice as UnitPrice
FROM raw.Track AS track
JOIN raw.Album AS album ON track.AlbumId = album.AlbumId
JOIN raw.Artist AS artist ON artist.ArtistId = album.ArtistId
JOIN raw.Genre AS genre ON track.GenreId = genre.GenreId
JOIN raw.MediaType AS mediatype ON track.MediaTypeId = mediatype.MediaTypeId


CREATE TABLE stage.DimGeography WITH (DISTRIBUTION = ROUND_ROBIN, HEAP) AS
SELECT 
    BillingAddress AS Address,
    BillingCity AS City,
    BillingState AS State,
    BillingCountry AS Country,
    BillingPostalCode AS PostalCode
FROM raw.Invoice as invoice


INSERT INTO stage.DimGeography
SELECT customer.Address,
        customer.City,
        customer.State,
        customer.Country,
        customer.PostalCode
FROM raw.Customer as customer

INSERT INTO stage.DimGeography
SELECT Employee.Address,
        Employee.City,
        Employee.State,
        Employee.Country,
        Employee.PostalCode
FROM raw.Employee as Employee




CREATE TABLE stage.DimCustomer WITH (DISTRIBUTION = ROUND_ROBIN, HEAP) AS
SELECT 
    CustomerId,
    FirstName,
    LastName,
    Company,
    Phone,
    Fax,
    Email,
    SupportRepId,
    Address,
    City,
    State,
    Country,
    PostalCode
FROM raw.Customer as customer

CREATE TABLE stage.DimEmployee WITH (DISTRIBUTION = ROUND_ROBIN, HEAP) AS
SELECT 
    EmployeeId,
    FirstName,
    LastName,
    Title,
    BirthDate,
    HireDate,
    ReportsTo,
    Phone,
    Fax,
    Email,
    Address,
    City,
    State,
    Country,
    PostalCode
FROM raw.Employee as employee




CREATE TABLE #TmpStageDate (DateVal DATE NOT NULL)

DECLARE @StartDate DATE
DECLARE @EndDate DATE
SET @StartDate = '2021-01-01'
SET @EndDate = '2025-12-31'
DECLARE @LoopDate DATE
SET @LoopDate = @StartDate
WHILE @LoopDate <= @EndDate
BEGIN
    INSERT INTO #TmpStageDate VALUES
    (
        @LoopDate
    )
    SET @LoopDate = DATEADD(dd, 1, @LoopDate)
END


CREATE TABLE stage.DimDate WITH (DISTRIBUTION = ROUND_ROBIN, HEAP) AS
SELECT CAST(CONVERT(VARCHAR(8), DateVal, 112) as INT) AS DateKey,
    DateVal AS DateAltKey,
    Day(DateVal) AS Day,
    Month(DateVal) AS Month,
    Year(DateVal) AS Year
FROM #TmpStageDate
GO

DROP TABLE #TmpStageDate




CREATE TABLE stage.FactInvoice WITH (DISTRIBUTION = ROUND_ROBIN, HEAP) AS
SELECT 
    il.InvoiceLineId as InvoiceLineId,
    il.InvoiceId as InvoiceId,
    il.TrackId as TrackId,
    i.CustomerId as CustomerId,
    i.InvoiceDate as InvoiceDate,
    i.BillingAddress as BillingAddress,
    i.BillingCity as BillingCity,
    i.BillingCountry as BillingCountry,
    i.BillingState as BillingState,
    i.BillingPostalCode as BillingPostalCode,
    il.UnitPrice as UnitPrice,
    il.Quantity as Quantity
FROM raw.InoviceLine AS il JOIN raw.Invoice as i
ON il.InvoiceId = i.InvoiceId


