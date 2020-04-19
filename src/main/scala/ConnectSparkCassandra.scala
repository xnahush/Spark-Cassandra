import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object ConnectSparkCassandra {
  def writeDataToCassandra(): Unit ={
    val spark= SparkSession.builder()
      .appName("sample job to connect cassandra and load data")
      .master("local[*]")
      .getOrCreate()

    val ordersdf = spark.read
      .json("C:\\Data\\retail_db_json\\orders")
    val customersdf = spark.read
      .json("C:\\Data\\retail_db_json\\customers")
    val productsdf = spark.read
      .json("C:\\Data\\retail_db_json\\products")
    val order_itemsdf = spark.read
      .json("C:\\Data\\retail_db_json\\order_items")

    ordersdf.createOrReplaceTempView("orders_view")
    val orders = spark.sql("select order_customer_id, to_date(order_date) as order_date, order_id, order_status from orders_view")
    orders.createOrReplaceTempView("daily_orders")

    order_itemsdf.createOrReplaceTempView("order_items_view")
    val order_items = spark.sql("select order_item_order_id,order_item_product_id, order_item_subtotal from order_items_view ")
    order_items.createOrReplaceTempView("daily_order_items")

    customersdf.createOrReplaceTempView("customers_view")
    val customers = spark.sql("select concat(customer_fname,' ',customer_lname) as customer_fullname, customer_id, customer_city from customers_view")
    customers.createOrReplaceTempView("daily_customers")

    productsdf.createOrReplaceTempView("products_view")
    val products = spark.sql("select product_id,product_name from products_view")
    products.createOrReplaceTempView("daily_products")

    val datatowrite = spark.sql("select daily_orders.order_id, daily_customers.customer_fullname, daily_orders.order_date, daily_customers.customer_city, daily_order_items.order_item_subtotal, daily_products.product_name, daily_orders.order_status from daily_orders join daily_order_items on daily_orders.order_id=daily_order_items.order_item_order_id " +
                      "join daily_customers on daily_orders.order_customer_id=daily_customers.customer_id join daily_products on daily_order_items.order_item_product_id=daily_products.product_id")

    datatowrite.write
      .format("org.apache.spark.sql.cassandra")
      .mode("overwrite")
      .option("confirm.truncate", "true")
      .option("spark.cassandra.connection.host", "127.0.0.1")
      .option("spark.cassandra.connection.port", "9042")
      .option("keyspace", "retail_db")
      .option("table", "order_details")
      .save()
    spark.stop()

  }


  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    writeDataToCassandra()
  }


}
