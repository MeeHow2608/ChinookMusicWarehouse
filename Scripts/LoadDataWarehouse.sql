-- Script for loading data into datawarehouse


-- Creating dimensions tables
CREATE TABLE dbo.DimTrack
WITH
(
    DISTRIBUTION = REPLICATE,
    CLUSTERED COLUMNSTORE INDEX
)
AS
SELECT ROW_NUMBER() OVER(ORDER BY TrackId) AS TrackKey,
    TrackID as TrackAltKey,
    TrackName,
    AlbumTitle,
    MediaType,
    Genre,
    Composer,
    Artist,
    Milliseconds,
    Bytes,
    UnitPrice
FROM (SELECT DISTINCT TrackId, TrackName, AlbumTitle, MediaType, Genre, Composer, Artist, Milliseconds, Bytes, UnitPrice FROM stage.DimTrack) AS Track



CREATE TABLE dbo.DimGeography
WITH
(
    DISTRIBUTION = REPLICATE,
    CLUSTERED COLUMNSTORE INDEX
)
AS
SELECT ROW_NUMBER() OVER(ORDER BY Address) AS GeographyKey,
    Address,
    City,
    State,
    Country,
    PostalCode
FROM (SELECT DISTINCT Address, City, State, Country, PostalCode FROM stage.DimGeography) AS Geography




CREATE TABLE dbo.DimEmployee
WITH
(
    DISTRIBUTION = REPLICATE,
    CLUSTERED COLUMNSTORE INDEX
)
AS
SELECT ROW_NUMBER() OVER(ORDER BY EmployeeId) AS EmployeeKey,
    EmployeeId AS EmployeeAltKey,
    FirstName,
    LastName,
    Title,
    BirthDate,
    HireDate,
    ROW_NUMBER() OVER(ORDER BY EmployeeId) AS ReportsTo,
    g.GeographyKey as GeographyKey,
    Phone,
    Fax,
    Email
FROM (SELECT DISTINCT EmployeeId, FirstName, LastName, Title, BirthDate, HireDate, ReportsTo, Phone, Fax, Email, Address, City, State, Country, PostalCode FROM stage.DimEmployee) as e
JOIN dbo.DimGeography g
ON e.Address = g.Address
AND e.City = g.City
AND e.State = g.State
AND e.Country = g.Country
AND e.PostalCode = g.PostalCode


CREATE TABLE dbo.DimCustomer
WITH
(
    DISTRIBUTION = REPLICATE,
    CLUSTERED COLUMNSTORE INDEX
)
AS
SELECT ROW_NUMBER() OVER(ORDER BY CustomerId) AS CustomerKey,
    c.CustomerId AS CustomerAltKey,
    c.FirstName,
    c.LastName,
    c.Company,
    c.Phone,
    c.Fax,
    c.Email,
    e.EmployeeKey AS SupportRepId,
    g.GeographyKey as GeographyKey
FROM (SELECT DISTINCT CustomerId, FirstName, LastName, Company, Phone, Fax, Email, SupportRepId, Address, City, State, Country, PostalCode FROM stage.DimCustomer) as c
JOIN dbo.DimGeography AS g
ON c.Address = g.Address
AND c.City = g.City
AND c.State = g.State
AND c.Country = g.Country
AND c.PostalCode = g.PostalCode
JOIN dbo.DimEmployee AS e
ON c.SupportRepId = e.EmployeeAltKey



CREATE TABLE dbo.DimDate
WITH
(
    DISTRIBUTION = REPLICATE,
    CLUSTERED COLUMNSTORE INDEX
)
AS
SELECT DateKey,
        DateAltKey,
        Day,
        Month,
        (Select DateName( month , DateAdd( month , d.Month , 0 ) - 1 )) AS MonthName,
        Year
FROM stage.DimDate as d


-- Create facts table
CREATE TABLE dbo.FactInvoice
WITH
(
    DISTRIBUTION = HASH(InvoiceKey),
    CLUSTERED COLUMNSTORE INDEX
)
AS
SELECT ROW_NUMBER() OVER(ORDER BY InvoiceLineId) AS InvoiceKey,
    InvoiceLineId AS InvoiceAltKey,
    InvoiceId,
    t.TrackKey as TrackKey,
    c.CustomerKey as CustomerKey,
    d.DateKey AS DateKey,
    g.GeographyKey as GeographyKey,
    f.UnitPrice,
    f.Quantity
FROM (SELECT DISTINCT * FROM stage.FactInvoice) as f
JOIN dbo.DimGeography AS g
ON f.BillingAddress = g.Address
AND f.BillingCity = g.City
AND f.BillingState = g.State
AND f.BillingCountry = g.Country
AND f.BillingPostalCode = g.PostalCode
JOIN dbo.DimTrack AS t
ON t.TrackAltKey = f.TrackId
JOIN dbo.DimDate as d
ON d.DateAltKey = f.InvoiceDate
JOIN dbo.DimCustomer as c
ON c.CustomerAltKey = f.CustomerId



