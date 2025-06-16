# PySpark의 핵심 클래스인 SparkSession을 불러온다. (DataFrame 및 SQL 작업의 시작점)
from pyspark.sql import SparkSession

# SparkSession을 생성한다.
# .master("local[*]") → 로컬 머신의 모든 CPU 코어를 사용하여 실행
# .appName("PySpark Test") → 실행되는 Spark 애플리케이션의 이름을 지정
# .getOrCreate() → 기존 세션이 있으면 재사용, 없으면 새로 생성
spark = SparkSession.builder\
        .master("local[*]")\
        .appName('PySpark Tutorial')\
        .getOrCreate()

# 숫자 0부터 9까지 포함된 DataFrame을 생성한다. (컬럼명: id)
df = spark.range(10)

# 생성한 DataFrame의 내용을 콘솔에 출력한다.
df.show()
