"""
실습 3. 텍스트를 파싱해서 구조화된 데이터로 변환하기
- Regex를 이용해서 아래와 같이 변환해보는 것이 목표
입력: “On 2021-01-04 the cost per ton from 85001 to 85002 is $28.32 at ABC Hauling”
○ regex 패턴: “On (\S+) the cost per ton from (\d+) to (\d+) is (\S+) at (.*)”
■ \S (non-whitespace character), \d (numeric character)

출력:
|week|departure_zipcode|arrival_zipcode|cost|vendor|
|2021-01-04|85001|85002|$28.32|ABC Hauling|
"""
from pyspark.sql import SparkSession
from pyspark import SparkConf
from pyspark.sql.functions import regexp_extract, col
from pyspark.sql.types import StructType, StructField, StringType

# ------------------------------------------
# [1] Spark 세션 생성
# ------------------------------------------

conf = SparkConf().setAppName("PySpark DataFrame #3").setMaster("local[*]")

spark = SparkSession.builder.config(conf=conf).getOrCreate()

# ------------------------------------------
# [2] 원본 텍스트 파일 로드 (한 줄당 1 레코드)
# ------------------------------------------

schema = StructType([StructField("text", StringType(), True)])

transfer_cost_df = spark.read.schema(schema).text("data/transfer_cost.txt")

# 원본 로그 확인 (줄 단위)
transfer_cost_df.show(5, truncate=False)

# ------------------------------------------
# [3] 정규표현식 패턴 정의 및 컬럼 추출
# ------------------------------------------

# 예시 문장 패턴:
# On week10 the cost per ton from 98001 to 29403 is $175.00 at BlueLine Logistics

regex_pattern = r"On (\S+) the cost per ton from (\d+) to (\d+) is (\S+) at (.*)"

df_extracted = transfer_cost_df \
    .withColumn("week", regexp_extract(col("text"), regex_pattern, 1)) \
    .withColumn("departure_zipcode", regexp_extract(col("text"), regex_pattern, 2)) \
    .withColumn("arrival_zipcode", regexp_extract(col("text"), regex_pattern, 3)) \
    .withColumn("cost", regexp_extract(col("text"), regex_pattern, 4)) \
    .withColumn("vendor", regexp_extract(col("text"), regex_pattern, 5))

df_extracted.printSchema()
df_extracted.show(5, truncate=False)

# ------------------------------------------
# [4] 불필요한 원문 텍스트 컬럼 제거
# ------------------------------------------

final_df = df_extracted.drop("text")

# ------------------------------------------
# [5] 결과 저장 (CSV 및 JSON)
# ------------------------------------------

final_df.write.mode("overwrite").csv("extracted.csv")  # 폴더로 저장됨
final_df.write.mode("overwrite").json("extracted.json")  # 폴더로 저장됨

# ------------------------------------------
# [6] 저장 파일 목록 확인 (로컬 환경에서만 유효)
# ------------------------------------------

import os

print("\n📂 CSV 파일 목록:")
print(os.listdir("extracted.csv"))

print("\n📂 JSON 파일 목록:")
print(os.listdir("extracted.json"))

# (필요 시 일부 내용 미리보기)
with open("extracted.json/" + os.listdir("extracted.json")[0], encoding="utf-8") as f:
    print("\n📄 JSON 샘플:", f.readline())
