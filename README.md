# Spark-Cassandra
This is an ETL pipeline with JSON as source and Cassandra as Target. Spark is used to transform and process data.

# Create Keyspace in Cassandra
CREATE KEYSPACE retail_db
WITH replication = {'class':'SimpleStrategy', 'replication_factor' : 1};

# Create Table
CREATE TABLE order_details(
   order_id int,
   customer_fullname text,
   order_date date,
   customer_city text,
   order_item_subtotal float,
   product_name text,
   order_status text,
   PRIMARY KEY (order_id,order_date)
   );
