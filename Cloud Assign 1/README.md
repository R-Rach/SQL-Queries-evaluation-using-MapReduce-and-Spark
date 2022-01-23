# Python Script

## Description
 - generating tables amazon-meta.txt is divided into for csv files - products.csv, similar.csv, categories.csv, reviews.csv.

## Setup
 - Use ‘txt2csv.py’ for converting the data into csv tables.
 - Requirement : 
   - Language - Python3. 
   - Libraries - tqdm==4.31.1, os, re, csv, datetime.
- Download (link given in assignment description) and keep the 'amazon-meta.txt’ data file in the same folder as the script.
- Executing the script will result in 4 tables (product, similar, categories, reviews) in the same folder.
- Put these four files in /MapReduce/Data folder.




# SQL Query processing using MapReduce and Spark

## Setup
 - Download Eclipse EE Developers tool in Eclipse IDE, follow {https://self-learning-java-tutorial.blogspot.com/2014/12/add-javaee-perspective-to-eclipse.html}.
 - Import (MapReduce folder) as existing maven project, follow the tutorial {https://vaadin.com/learn/tutorials/import-maven-project-eclipse#_import_the_project}.
 - Project dependencies and configurations: pom.xml
 - Setum Tomcat 9 on JavaEE environment and run the project on server

## Configuration
 - Change the output and input paths for map reduce and spark inside GlobalVariables.java according to machine

## API
 - Send a POST request to `http://localhost:8080/MapReduce/query`, with raw JSON body as { "query" : "SELECT customerid, sum(helpful) FROM reviews WHERE rating = 5 GROUP BY customerid HAVING sum(helpful) >= 50" } (Use Postman for this)
 - GET request on `http://localhost:8080/MapReduce/query/test1` : Returns a page displaying Hello. {Useful for testing if the API is live}

## Requirements 
 - Maven, Jersey, Eclipse, Tomcat 9, Hadoop maven dependencies, Spark maven dependencies, Scala maven dependencies

## Assumptions
 - Query should contain all clauses (SELECT/FROM/WHERE/GROUP BY/HAVING)
 - Query should be of specific format (with correct spacing as shown below in few Examples, also many examples inside main() method of QueryController.java)
 - EXAMPLES:
   - SELECT customerid, sum(helpful) FROM reviews WHERE rating = 5 GROUP BY customerid HAVING sum(helpful) >= 50
   - SELECT customerid, sum(helpful) FROM reviews WHERE customerid = \"A3UN6WX5RRO2AG\" GROUP BY customerid HAVING sum(helpful) >= 50
   - SELECT id, asin, salesrank count(*) FROM products WHERE title = \"Fantastic Food with Splenda : 160 Great Recipes for Meals Low in Sugar, Carbohydrates, Fat, and Calories\" GROUP BY id, asin, salesrank HAVING count(*) > 1
   - SELECT category, count(*) FROM categories WHERE category = \"Religion & Spirituality[22]\" GROUP BY category HAVING count(*) >= 1

