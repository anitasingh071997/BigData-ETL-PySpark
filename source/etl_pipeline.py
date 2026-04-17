import os
os.environ["HADOOP_HOME"] = "C:\\hadoop"
os.environ["PATH"] += ";C:\\hadoop\\bin"

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, regexp_replace, when, expr

# --------------------------------------------
# STEP 1: Create Spark Session
# --------------------------------------------
spark = SparkSession.builder \
    .appName("ETL Pipeline - Clean Data") \
    .getOrCreate()

# --------------------------------------------
# STEP 2: Read CSV
# --------------------------------------------
df = spark.read.csv(
    "data/sales.csv",
    header=True,
    inferSchema=False,
    quote='"',
    escape='"',
    multiLine=True
)

# Fix column names
df = df.toDF(*[c.replace('.', '_') for c in df.columns])

df.printSchema()

# --------------------------------------------
# STEP 3: Remove Nulls & Duplicates
# --------------------------------------------
df_clean = df.dropna().dropDuplicates()

# --------------------------------------------
# STEP 4: Clean Numeric Columns
# --------------------------------------------
df_clean = df_clean.withColumn(
    "Sales",
    regexp_replace(col("Sales"), ",", "")
).withColumn(
    "Profit",
    regexp_replace(col("Profit"), ",", "")
)

# --------------------------------------------
# STEP 5: Safe Casting
# --------------------------------------------
df_clean = df_clean.withColumn(
    "Sales",
    expr("try_cast(Sales as double)")
).withColumn(
    "Profit",
    expr("try_cast(Profit as double)")
)

# --------------------------------------------
# STEP 6: Create Profit Ratio
# --------------------------------------------
df_transformed = df_clean.withColumn(
    "Profit_Ratio",
    when(col("Sales") != 0, col("Profit") / col("Sales"))
)

# --------------------------------------------
# STEP 7: Save Output (NO HADOOP)
# --------------------------------------------
df_transformed.toPandas().to_csv(
    "output/cleaned_data.csv",
    index=False
)

# Aggregations
df_transformed.groupBy("Region").agg(
    {"Sales": "sum", "Profit": "sum"}
).show()

## Save insights
df_transformed.groupBy("Category").agg(
    {"Sales": "sum"}
).toPandas().to_csv("output/category_sales.csv", index=False)

print("Data cleaning completed successfully!")

# --------------------------------------------
# STEP 8: Stop Spark
# --------------------------------------------
spark.stop()