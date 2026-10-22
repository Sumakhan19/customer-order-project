from pyspark.sql.functions import sum,avg,col,row_number,desc,rank,lag
from pyspark.sql.window import Window

customers=spark.read.csv('/Volumes/workspace/default/customers/customer.csv',header=True,inferSchema=True)
customers.createOrReplaceTempView("customers")
display(customers)

orders=spark.read.csv('/Volumes/workspace/default/orders/orders.csv',header=True,inferSchema=True)
orders.createOrReplaceTempView("orders")
display(orders)

orders.printSchema()

customer_orders_df = (
    customers.alias("c")
    .join(
        orders.alias("o"),
        col("c.customer_id") == col("o.customer_id"),
        "inner"
    )
    .select(
        col("c.customer_name"),
        col("c.city"),
        col("o.customer_id"),
        col("o.order_id"),
        col("o.amount"),
        col("o.order_date")
    )
)
display(customer_orders_df)


customer_orders_df=customer_orders_df.repartition(4,"city")
                                                                   

display(customer_orders_df)

customer_summary_df = customer_orders_df.groupBy(
    "customer_id",
    "customer_name",
    "city"
).agg(
    sum("amount").alias("total_amount"),
    avg("amount").alias("average_order_amount")
)

display(customer_summary_df)

city_window = Window.partitionBy(
    "city"
).orderBy(
    desc("total_amount")
)

result_df = customer_summary_df.withColumn(
    "row_number",
    row_number().over(city_window)
)

display(result_df)

result_df = result_df.withColumn(
    "rank",
    rank().over(city_window)
)

display(result_df)

city_wise_top_cusomer=result_df.filter(col("rank")==1)
display(city_wise_top_cusomer)

city_wise_top_cusomer.write.mode("overwrite").saveAsTable("city_wise_top_cusomer")        

result_df = result_df.withColumn(
    "previous_customer_amount",
    lag("total_amount", 1).over(city_window)
)

print("LAG Example")
result_df.show()        


result_df = result_df.withColumn(
    "amount_difference",
    col("total_amount") -
    col("previous_customer_amount")
)

print("Final Result")
result_df.show()


result_df.write.mode("overwrite").csv('/Volumes/workspace/default/result',header=True)