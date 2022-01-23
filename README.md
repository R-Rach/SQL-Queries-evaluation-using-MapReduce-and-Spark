# SQL-Queries-evaluation-using-MapReduce-and-Spark

A REST-based service for Amazon sales dataset.
This service accepts an SQL query in the form of the template shown below and translate into MapReduce and Spark job on the dataset.

SELECT <COLUMNS>, FUNC(COLUMN1)
FROM <TABLE>
WHERE <COLUMN1> = Y
GROUP BY <COLUMNS>
HAVING FUNC(COLUMN1)>X

--Here FUNC can be COUNT, MAX, MIN, SUM


Dataset Description:

- Id: Product id (number 0, ..., 548551)
- ASIN: Amazon Standard Identification Number
- title: Name/title of the product
- group: Product group (Book, DVD, Video or Music)
- salesrank: Amazon Salesrank
- similar: ASINs of co-purchased products (people who buy X also buy Y)
- categories: Location in product category hierarchy to which the product belongs (separated by |, category id in [])
- reviews: Product review information: time, user id, rating, total number of votes on the review, total number of helpfulness votes (how many people found the   review to be helpful)
