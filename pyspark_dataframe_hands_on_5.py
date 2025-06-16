"""
실습 5. Redshift 연결해보기
- MAU (Monthly Active User) 계산해보기
- 두 개의 테이블을 Redshift에서 Spark으로 로드
  - JDBC 연결 실습
- DataFrame과 SparkSQL을 사용해서 조인
- DataFrame JOIN
  - left_DF.join(right_DF, join condition, join type)
    - join type: “inner”, “left”, “right”, “outer”, “semi”, “anti”
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import date_format, asc, countDistinct
import os

# ------------------------------------------
# [0] JDBC 드라이버 다운로드 (필요한 경우 수동 수행)
# ------------------------------------------

os.system(
    "cd /usr/local/lib/python3.10/dist-packages/pyspark/jars && "
    "wget -nc https://s3.amazonaws.com/redshift-downloads/drivers/jdbc/2.1.0.30/redshift-jdbc42-2.1.0.30.jar"
)

# ------------------------------------------
# [1] Spark 세션 생성
# ------------------------------------------

spark = SparkSession.builder \
    .appName("PySpark DataFrame #5 - Redshift Integration") \
    .getOrCreate()

# ------------------------------------------
# [2] Redshift 테이블 로딩 (JDBC 연결)
# ------------------------------------------

JDBC_URL = (
    "jdbc:redshift://learnde.cduaw970ssvt.ap-northeast-2.redshift.amazonaws.com:5439/dev"
    "?user=guest&password=Guest1234"
)
JDBC_DRIVER = "com.amazon.redshift.Driver"

def load_table(table_name: str):
    return spark.read \
        .format("jdbc") \
        .option("driver", JDBC_DRIVER) \
        .option("url", JDBC_URL) \
        .option("dbtable", table_name) \
        .load()

df_user_session_channel = load_table("raw_data.user_session_channel")
df_session_timestamp = load_table("raw_data.session_timestamp")

df_user_session_channel.printSchema()
df_session_timestamp.printSchema()

# ------------------------------------------
# [3] 테이블 조인 및 채널별 사용자 수 분석
# ------------------------------------------

# Inner Join on sessionID
join_expr = df_user_session_channel.sessionid == df_session_timestamp.sessionid

session_df = df_user_session_channel.join(
    df_session_timestamp, join_expr, "inner"
).select(
    "userid", df_user_session_channel.sessionid, "channel", "ts"
)

# 채널별 세션 수 (내림차순 정렬)
channel_count_df = session_df.groupBy("channel") \
    .count() \
    .orderBy("count", ascending=False)

channel_count_df.show()

# ------------------------------------------
# [4] 월별 MAU(Monthly Active User) 계산
# ------------------------------------------

mau_df = session_df.withColumn(
    'month', date_format('ts', 'yyyy-MM')
).groupBy('month') \
 .agg(countDistinct("userid").alias("mau")) \
 .orderBy(asc('month'))

mau_df.show()

# ------------------------------------------
# [5] Spark SQL 방식으로 동일 분석 수행
# ------------------------------------------

df_user_session_channel.createOrReplaceTempView("user_session_channel")
df_session_timestamp.createOrReplaceTempView("session_timestamp")

# [SQL] 채널별 고유 사용자 수
channel_count_sql = spark.sql("""
    SELECT channel, COUNT(DISTINCT userId) AS unique_users
    FROM session_timestamp st
    JOIN user_session_channel usc ON st.sessionID = usc.sessionID
    GROUP BY channel
    ORDER BY channel
""")

channel_count_sql.show()

# [SQL] 월별 MAU 계산
mau_sql = spark.sql("""
    SELECT 
      LEFT(st.ts, 7) AS month,
      COUNT(DISTINCT usc.userid) AS mau
    FROM session_timestamp st
    JOIN user_session_channel usc ON st.sessionid = usc.sessionid
    GROUP BY month
    ORDER BY month DESC
""")

mau_sql.show()
