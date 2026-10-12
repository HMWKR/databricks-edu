# Databricks notebook source
# MAGIC %md
# MAGIC # Silver Layer — 환율 데이터 정제
# MAGIC
# MAGIC Bronze 테이블의 원본 데이터를 읽어 **정제된(Cleansed) 데이터**로 변환하여 Silver Delta 테이블에 저장합니다.
# MAGIC
# MAGIC ## Bronze vs Silver 차이
# MAGIC | | Bronze | Silver |
# MAGIC |---|---|---|
# MAGIC | **목적** | 원본 그대로 보존 | 정제 및 표준화 |
# MAGIC | **데이터 타입** | 모두 String | 숫자는 Decimal, 날짜는 Date |
# MAGIC | **중복** | 허용 (append) | 제거 (dedup) |
# MAGIC | **통화코드** | `JPY(100)` 그대로 | `JPY` + `unit_mult=100` 분리 |
# MAGIC
# MAGIC ## 처리 순서
# MAGIC | Step | 셀 | 설명 |
# MAGIC |---|---|---|
# MAGIC | **1** | 셀 1 | Bronze 테이블 읽기 |
# MAGIC | **2** | 셀 2 | 정제: 타입 변환, 통화코드 정규화, 중복 제거 |
# MAGIC | **3** | 셀 3 | Silver Delta 테이블에 저장 |

# COMMAND ----------

# DBTITLE 1,Step 1: Bronze 테이블 읽기
# ============================================================
# Step 1: Bronze 테이블 읽기
# ============================================================

BRONZE_TABLE = "main.bronze.exchange_rate_korea_exim"

bronze_df = spark.table(BRONZE_TABLE)
print(f"📖 Bronze 테이블 로드: {bronze_df.count()}행")
print(f"   테이블: {BRONZE_TABLE}")

# 원본 데이터 미리보기
display(bronze_df.select("ingested_at", "search_date", "cur_unit", "cur_nm", "deal_bas_r", "ttb", "tts"))

# COMMAND ----------

# DBTITLE 1,Step 2: 정제 (타입 변환, 통화코드 정규화, 중복 제거)
# ============================================================
# Step 2: 정제 — 타입 변환, 통화코드 정규화, 중복 제거
# ============================================================
#
# 정제 항목:
#   1. 숫자 필드: "1,341.50" (콤마 포함 문자열) → Decimal 타입
#   2. 통화코드: "JPY(100)" → cur_unit="JPY", unit_mult=100
#   3. 날짜: "20261012" (YYYYMMDD 문자열) → Date 타입
#   4. 중복 제거: 동일 (search_date, cur_unit) 중 최신 ingested_at만保留

from pyspark.sql.functions import (
    regexp_replace, regexp_extract, col, to_date, split, 
    when, row_number, expr, lit
)
from pyspark.sql.types import DecimalType
from pyspark.sql.window import Window

# ── 1) 숫자 필드 변환 (콤마 제거 + 빈 문자열 → NULL → Decimal) ──
# 빈 문자열("")은 Decimal로 캐스팅 시 에러 발생 → NULL로 변환 후 캐스팅
numeric_cols = ["deal_bas_r", "ttb", "tts", "bkpr", "ttbdde", "ttsdde", "ye_eyr_rate", "yy_efee_r", "mm_efee_r"]

silver_df = bronze_df
for c in numeric_cols:
    silver_df = silver_df.withColumn(
        c, 
        when(col(c) == "", None)
        .otherwise(regexp_replace(col(c), ",", "").cast(DecimalType(15, 4)))
    )

# ── 2) 통화코드 정규화 ──
# 예: "JPY(100)" → cur_unit="JPY", unit_mult=100
# 예: "USD"     → cur_unit="USD", unit_mult=1
silver_df = silver_df.withColumn(
    "cur_unit_raw", col("cur_unit")  # 원본 보존
).withColumn(
    "cur_unit", 
    when(col("cur_unit").contains("("), 
         split(col("cur_unit"), "\\(")[0]
    ).otherwise(col("cur_unit"))
).withColumn(
    "unit_mult",
    when(col("cur_unit_raw").contains("("),
         regexp_extract(col("cur_unit_raw"), r"\((\d+)\)", 1).cast("int")
    ).otherwise(lit(1))
)

# ── 3) 날짜 변환 ──
silver_df = silver_df.withColumn(
    "search_date", 
    to_date(col("search_date"), "yyyyMMdd")
).withColumn(
    "ingested_at", 
    col("ingested_at").cast("timestamp")
)

# ── 4) 중복 제거 (동일 search_date + cur_unit 중 최신 ingested_at) ──
window = Window.partitionBy("search_date", "cur_unit").orderBy(col("ingested_at").desc())
silver_df = silver_df.withColumn("rn", row_number().over(window)) \
    .filter(col("rn") == 1) \
    .drop("rn")

print(f"✅ 정제 완료: {silver_df.count()}행 (중복 제거 후)")
print(f"   스키마:")
silver_df.printSchema()

# 정제 결과 미리보기
display(silver_df.select(
    "search_date", "cur_unit", "cur_nm", "unit_mult",
    "deal_bas_r", "ttb", "tts"
))

# COMMAND ----------

# DBTITLE 1,Step 3: Silver Delta 테이블에 저장
# ============================================================
# Step 3: Silver Delta 테이블에 저장
# ============================================================
#
# Silver 테이블 설계:
#   • 카탈로그: main
#   • 스키마:   silver
#   • 테이블명: exchange_rate_clean
#   • 전체 경로: main.silver.exchange_rate_clean
#
# 적재 전략: OVERWRITE (매일 실행 시 최신 정제 결과로 교체)
#   • Silver는 정제된 상태이므로 OVERWRITE로 깔끔하게 유지
#   • Bronze가 원본 이력을 보존하므로 Silver는 중복 누적 불필요

CATALOG = "main"
SCHEMA  = "silver"
TABLE   = "exchange_rate_clean"
FULL_TABLE_NAME = f"{CATALOG}.{SCHEMA}.{TABLE}"

# ── 1) 카탈로그/스키마 생성 ──
spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
print(f"✅ 카탈로그/스키마 준비: {CATALOG}.{SCHEMA}")

# ── 2) Delta 테이블에 OVERWRITE ──
silver_df.write \
    .mode("overwrite") \
    .saveAsTable(FULL_TABLE_NAME)

print(f"✅ Silver 테이블 저장 완료: {FULL_TABLE_NAME}")

# ── 3) 저장 결과 확인 ──
result_df = spark.sql(f"""
    SELECT 
        search_date,
        cur_unit,
        cur_nm,
        unit_mult,
        deal_bas_r,
        ttb,
        tts
    FROM {FULL_TABLE_NAME}
    ORDER BY cur_unit
""")

print(f"📊 {FULL_TABLE_NAME} 총 행 수: {result_df.count()}")
display(result_df)