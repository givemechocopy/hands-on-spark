"""
실습 4. Stackoverflow 서베이 기반 인기 언어 찾기
- stackoverflow CSV파일에서 다음 두 필드는 ;를 구분자로 프로그래밍 언어를
구분
  - LanguageHaveWorkedWith
  - LanguageWantToWorkWith
- 이를 별개 레코드로 분리하여 가장 많이 사용되는 언어 top 50와 가장 많이 쓰고 싶은 언어 top 50를 계산해보기
"""
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# ------------------------------------------
# [1] Spark 세션 생성
# ------------------------------------------

spark = SparkSession.builder \
    .appName("StackOverflow Language Analysis") \
    .config("spark.jars", "/usr/local/lib/python3.7/dist-packages/pyspark/jars/RedshiftJDBC42-no-awssdk-1.2.20.1043.jar") \
    .getOrCreate()

# ------------------------------------------
# [2] 설문 데이터 로드 및 필요한 컬럼 선택
# ------------------------------------------

df = spark.read.csv(
    "survey_results_public.csv",
    header=True
).select("ResponseId", "LanguageHaveWorkedWith", "LanguageWantToWorkWith")

df.printSchema()

# ------------------------------------------
# [3] 언어 컬럼 전처리: 세미콜론(;) 분리 및 트림
# ------------------------------------------

df_cleaned = df \
    .withColumn("language_have", F.split(F.trim(F.col("LanguageHaveWorkedWith")), ";")) \
    .withColumn("language_want", F.split(F.trim(F.col("LanguageWantToWorkWith")), ";"))

df_cleaned.select("ResponseId", "language_have", "language_want").show(5, truncate=False)

# ------------------------------------------
# [4] 사용 경험이 있는 언어 분석
# ------------------------------------------

df_language_have = df_cleaned.select(
    "ResponseId",
    F.explode("language_have").alias("language_have")
).filter(F.col("language_have").isNotNull())

# 언어별 사용 빈도 계산
df_language_have_count = df_language_have.groupBy("language_have").count()

# 상위 50개 언어 추출
df_language50_have = df_language_have_count.orderBy(F.desc("count")).limit(50)

df_language50_have.show(10, truncate=False)

# CSV로 저장
df_language50_have.write.mode("overwrite").csv("language50_have")

# ------------------------------------------
# [5] 배우고 싶은 언어 분석
# ------------------------------------------

df_language_want = df_cleaned.select(
    "ResponseId",
    F.explode("language_want").alias("language_want")
).filter(F.col("language_want").isNotNull())

# 언어별 희망 빈도 계산
df_language_want_count = df_language_want.groupBy("language_want").count()

# 상위 50개 언어 추출
df_language50_want = df_language_want_count.orderBy(F.desc("count")).limit(50)

df_language50_want.show(10, truncate=False)

# CSV로 저장
df_language50_want.write.mode("overwrite").csv("language50_want")

# ------------------------------------------
# [6] 저장 파일 확인 (로컬 실행 환경 전용)
# ------------------------------------------

import os

print("\n📂 Top 50 Languages Used:")
print(os.listdir("language50_have"))

print("\n📂 Top 50 Languages Wanted:")
print(os.listdir("language50_want"))
