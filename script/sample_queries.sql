-- This query will fail, * is not allowed
SELECT *
FROM airline_conversions a
    JOIN socialco_impressions s
        ON a.identifier = s.identifier

-- This query will fail, identifier is not allowed in SELECT
SELECT a.identifier
FROM airline_conversions a
    JOIN socialco_impressions s
        ON a.identifier = s.identifier

-- simple successful query
SELECT COUNT(a.identifier)
FROM airline_conversions a
    JOIN socialco_impressions s
        ON a.identifier = s.identifier


-- more complex query
SELECT COUNT(DISTINCT a.identifier)
FROM airline_conversions a
    JOIN socialco_impressions s
        ON a.identifier = s.identifier
WHERE a.sale_date >= s.impression_date