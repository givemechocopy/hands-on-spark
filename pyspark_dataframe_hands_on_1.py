"""
실습 1. 워밍업 - 헤더가 없는 CSV 파일 처리하기
- 입력 데이터: 헤더가 없는 CSV ㅏ일
- 데이터 스키마 지정하기
- SparkConf 사용해보기
- measure_type 값이 TMIN인 레코드 대상으로 stationId별 최소 온도 찾기
"""
from pyspark.sql import SparkSession
from pyspark import SparkConf
from pyspark.sql.types import StringType, IntegerType, FloatType, StructType, StructField
import pandas as pd

# ------------------------------------------
# [1] Pandas로 간단한 사전 분석
# ------------------------------------------

# CSV 파일을 Pandas로 읽고 컬럼 이름 수동 지정
pd_df = pd.read_csv(
    "data/1800.csv",
    names=["stationID", "date", "measure_type", "temperature"],
    usecols=[0, 1, 2, 3]  # 필요한 열만 선택
)

# TMIN 측정값만 필터링
pd_minTemps = pd_df[pd_df['measure_type'] == "TMIN"]

# 최소 온도를 stationID별로 그룹화하여 계산
pd_minTempsByStation = pd_minTemps[["stationID", "temperature"]].groupby("stationID").min()
# print(pd_minTempsByStation.head())

# ------------------------------------------
# [2] PySpark 환경 설정 및 세션 생성
# ------------------------------------------

conf = SparkConf().setAppName("PySpark DataFrame #1").setMaster("local[*]")

spark = SparkSession.builder.config(conf=conf).getOrCreate()

# ------------------------------------------
# [3] Spark DataFrame으로 데이터 로드 및 스키마 지정
# ------------------------------------------

# 명시적인 스키마 정의 (헤더 없음)
schema = StructType([
    StructField("stationID", StringType(), True),
    StructField("date", IntegerType(), True),
    StructField("measure_type", StringType(), True),
    StructField("temperature", FloatType(), True)
])

# CSV 파일을 스키마에 맞게 로딩
df = spark.read.schema(schema).csv("data/1800.csv")

# df.printSchema()

# ------------------------------------------
# [4] TMIN 필터링 및 최소 온도 집계
# ------------------------------------------

# 방법 1: 컬럼 표현식으로 필터링
minTemps = df.filter(df.measure_type == "TMIN")

# 방법 2: SQL 문자열 표현식으로 필터링 (동일 결과)
# minTemps = df.where("measure_type = 'TMIN'")

# 스테이션별 최소 온도 계산
minTempsByStation = minTemps.groupBy("stationID").min("temperature")

# 결과 출력 (DataFrame 형태)
minTempsByStation.show()

# ------------------------------------------
# [5] 필요한 컬럼만 추출
# ------------------------------------------

stationTemps = minTemps.select("stationID", "temperature")
stationTemps.show(5)

# ------------------------------------------
# [6] 결과 수집 후 출력
# ------------------------------------------

results = minTempsByStation.collect()

print("\n--- Station별 최저 기온 ---")
for result in results:
    print(f"{result['stationID']}\t{result['min(temperature)']:.2f}F")

# ------------------------------------------
# [7] SQL 방식으로 동일 결과 도출
# ------------------------------------------

# 임시 뷰 등록
df.createOrReplaceTempView("station1800")

sql_results = spark.sql("""
    SELECT stationID, MIN(temperature) AS min_temp
    FROM station1800
    WHERE measure_type = 'TMIN'
    GROUP BY stationID
""").collect()

print("\n--- SQL 방식 결과 ---")
for row in sql_results:
    print(f"{row['stationID']}\t{row['min_temp']:.2f}F")
