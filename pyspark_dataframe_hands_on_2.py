"""
실습 2. 워밍업 2 - 헤더 없는 CSV 파일 처리하기
- 입력 데이터: 헤더 없는 CSV 파일
- 데이터에 스키마 지정하기
  - cust_id, item_id, amount_spent를 데이터 컬럼으로 추가하기 (모두 숫자)
- cust_id를 기준으로 amount_spent의 합을 계산하기
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, FloatType

# ------------------------------------------
# [1] Spark 세션 생성
# ------------------------------------------

spark = SparkSession.builder \
    .appName('PySpark DataFrame #2') \
    .master('local[*]') \
    .getOrCreate()

# ------------------------------------------
# [2] 스키마 정의 및 데이터 로드
# ------------------------------------------

schema = StructType([
    StructField("cust_id", StringType(), True),
    StructField("item_id", StringType(), True),
    StructField("amount_spent", FloatType(), True)
])

df = spark.read.schema(schema).csv("data/customer-orders.csv")

df.printSchema()

# ------------------------------------------
# [3] 고객별 총 소비 금액 계산
# ------------------------------------------

# 방식 1: 기본 groupBy + sum + rename
df_total = df.groupBy("cust_id") \
             .sum("amount_spent") \
             .withColumnRenamed("sum(amount_spent)", "total_spent")

df_total.show(10)

# 방식 2: alias를 사용한 집계 표현
df_total = df.groupBy("cust_id") \
             .agg(F.sum("amount_spent").alias("total_spent"))

df_total.show(10)

# ------------------------------------------
# [4] 고객별 추가 통계 (최대값, 평균값 포함)
# ------------------------------------------

df_stats = df.groupBy("cust_id") \
             .agg(
                 F.sum("amount_spent").alias("total_spent"),
                 F.max("amount_spent").alias("max_spent"),
                 F.avg("amount_spent").alias("avg_spent")
             )

df_stats.show(10)

# ------------------------------------------
# [5] SQL 방식으로 동일 작업 수행
# ------------------------------------------

df.createOrReplaceTempView("customer_orders")

sql_results = spark.sql("""
    SELECT
        cust_id,
        SUM(amount_spent) AS total_spent,
        MAX(amount_spent) AS max_spent,
        AVG(amount_spent) AS avg_spent
    FROM customer_orders
    GROUP BY cust_id
""")

sql_results.show(10)

# ------------------------------------------
# [6] 현재 Spark SQL Catalog에 등록된 테이블 확인
# ------------------------------------------

print("📋 Registered Spark SQL Tables:")
# for table in spark.catalog.listTables():
#     print(f"- {table.name} ({table.tableType})")
