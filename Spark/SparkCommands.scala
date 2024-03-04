var df = spark.read.format("csv").load("s3://airline-delay-kalindu/Input/DelayedFlights-updated.csv")
df.createOrReplaceTempView("delay_flights")


val startTime = System.currentTimeMillis();
val result_df = spark.sql("SELECT _c1 AS Year, AVG((_c25/_c15)*100) AS Avg_Carrier_Delay FROM delay_flights WHERE _c1 BETWEEN 2003 AND 2010 GROUP BY _c1 ORDER BY _c1").show()
val endTime = System.currentTimeMillis();
val executionTime = (endTime - startTime)/1000.0;
println(s"Query executed in $executionTime s")

val startTime = System.currentTimeMillis();
val result_df = spark.sql("SELECT _c1 AS Year, AVG((_c27/_c15)*100) AS Avg_NAS_Delay FROM delay_flights WHERE _c1 BETWEEN 2003 AND 2010 GROUP BY _c1 ORDER BY _c1").show()
val endTime = System.currentTimeMillis();
val executionTime = (endTime - startTime)/1000.0;
println(s"Query executed in $executionTime s")

val startTime = System.currentTimeMillis();
val result_df = spark.sql("SELECT _c1 AS Year, AVG((_c26/_c15)*100) AS Avg_Weather_Delay FROM delay_flights WHERE _c1 BETWEEN 2003 AND 2010 GROUP BY _c1 ORDER BY _c1").show()
val endTime = System.currentTimeMillis();
val executionTime = (endTime - startTime)/1000.0;
println(s"Query executed in $executionTime s")

val startTime = System.currentTimeMillis();
val result_df = spark.sql("SELECT _c1 AS Year, AVG((_c29/_c15)*100) AS Avg_Aircraft_Delay FROM delay_flights WHERE _c1 BETWEEN 2003 AND 2010 GROUP BY _c1 ORDER BY _c1").show()
val endTime = System.currentTimeMillis();
val executionTime = (endTime - startTime)/1000.0;
println(s"Query executed in $executionTime s")

val startTime = System.currentTimeMillis();
val result_df = spark.sql("SELECT _c1 AS Year, AVG((_c28/_c15)*100) AS Avg_Security_Delay FROM delay_flights WHERE _c1 BETWEEN 2003 AND 2010 GROUP BY _c1 ORDER BY _c1").show()
val endTime = System.currentTimeMillis();
val executionTime = (endTime - startTime)/1000.0;
println(s"Query executed in $executionTime s")
