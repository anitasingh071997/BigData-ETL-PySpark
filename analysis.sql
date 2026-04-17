-- Total Sales by Region
SELECT Region, SUM(Sales) AS Total_Sales
FROM sales
GROUP BY Region;

-- Profit by Category
SELECT Category, SUM(Profit) AS Total_Profit
FROM sales
GROUP BY Category;

-- Top 5 Products
SELECT Product_Name, SUM(Sales) AS Sales
FROM sales
GROUP BY Product_Name
ORDER BY Sales DESC
LIMIT 5;