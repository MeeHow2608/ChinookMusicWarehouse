-- Script for querying data warehouse


-- Simple example SELECT queries

-- Genres with biggest total revenue
SELECT t.genre, SUM(f.UnitPrice)
FROM dbo.FactInvoice AS f
JOIN dbo.DimTrack AS t
ON t.TrackKey = f.TrackKey
GROUP BY t.Genre

-- Quantity of files sold by month
SELECT d.MonthName, SUM(f.Quantity)
FROM dbo.FactInvoice as f
JOIN dbo.DimDate as d
ON d.DateKey = f.DateKey
GROUP BY d.MonthName, d.Month
ORDER BY d.Month
