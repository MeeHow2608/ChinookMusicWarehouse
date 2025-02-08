-- Script for querying data warehouse


-- Simple example SELECT queries

-- Genres with biggest total sales
SELECT t.genre as Genre, SUM(f.UnitPrice) as Sales
FROM dbo.FactInvoice AS f
JOIN dbo.DimTrack AS t
ON t.TrackKey = f.TrackKey
GROUP BY t.Genre
ORDER BY Sales DESC

-- Quantity of items sold by month
SELECT d.MonthName, SUM(f.Quantity) as Quantity
FROM dbo.FactInvoice as f
JOIN dbo.DimDate as d
ON d.DateKey = f.DateKey
GROUP BY d.MonthName, d.Month
ORDER BY d.Month

-- Sales per customer
SELECT SUM(f.UnitPrice) as Sales, c.FirstName, c.LastName, g.Country
FROM dbo.FactInvoice as f 
JOIN dbo.DimGeography as g 
ON g.GeographyKey = f.GeographyKey
JOIN dbo.DimCustomer as c 
ON c.CustomerKey = f.CustomerKey
GROUP BY c.CustomerKey, g.Country, c.FirstName, c.LastName

-- Quantity of items sold per city
SELECT SUM(f.Quantity) as Quantity, g.Country, g.City
FROM dbo.FactInvoice as f 
JOIN dbo.DimCustomer as c 
ON c.CustomerKey = f.CustomerKey
JOIN dbo.DimGeography as g 
ON g.GeographyKey = c.GeographyKey
GROUP BY g.Country, g.City
ORDER BY g.Country



