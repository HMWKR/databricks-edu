# Databricks notebook source
# MAGIC %md
# MAGIC # Gold Layer — 환율 비즈니스 요약
# MAGIC
# MAGIC Silver 테이블의 정제된 데이터를 읽어 **비즈니스 의사결정용 요약 테이블**을 생성합니다.
# MAGIC
# MAGIC ## Medallion 아키텍처 전체 흐름
# MAGIC ```
# MAGIC Bronze (원본)     →    Silver (정제)     →    Gold (요약)
# MAGIC main.bronze.          main.silver.          main.gold.
# MAGIC exchange_rate_        exchange_rate_        exchange_rate_
# MAGIC korea_exim            clean                 summary
# MAGIC ```
# MAGIC
# MAGIC ## Gold 테이블 내용
# MAGIC | 컬럼 | 설명 |
# MAGIC |---|---|
# MAGIC | `search_date` | 조회 일자 |
# MAGIC | `cur_unit` | 통화코드 (USD, EUR, ...) |
# MAGIC | `cur_nm` | 통화명 (미국 달러, ...) |
# MAGIC | `deal_bas_r` | 매매기준율 (Decimal) |
# MAGIC | `krw_per_unit` | 1 단위당 KRW 환산액 |
# MAGIC | `spread` | 매도-매수 스프레드 (tts - ttb) |
# MAGIC | `is_major` | 주요 통화 여부 (USD/EUR/JPY/CNY/GBP) |
# MAGIC
# MAGIC ## 처리 순서
# MAGIC | Step | 셀 | 설명 |
# MAGIC |---|---|---|
# MAGIC | **1** | 셀 1 | Silver 테이블 읽기 |
# MAGIC | **2** | 셀 2 | 비즈니스 요약: KRW 환산, 스프레드, 주요 통화 플래그 |
# MAGIC | **3** | 셀 3 | Gold Delta 테이블에 저장 |

# COMMAND ----------

# DBTITLE 1,Step 1: Silver 테이블 읽기
# ============================================================
# Step 1: Silver 테이블 읽기
# ============================================================

SILVER_TABLE = "main.silver.exchange_rate_clean"

silver_df = spark.table(SILVER_TABLE)
print(f"📖 Silver 테이블 로드: {silver_df.count()}행")
print(f"   테이블: {SILVER_TABLE}")

# 정제된 데이터 미리보기
display(silver_df.select("search_date", "cur_unit", "cur_nm", "unit_mult", "deal_bas_r", "ttb", "tts"))

# COMMAND ----------

# DBTITLE 1,Step 2: 비즈니스 요약 (KRW 환산, 스프레드, 주요 통화)
# ============================================================
# Step 2: 비즈니스 요약 생성
# ============================================================
#
# Gold 레이어에서 수행하는 비즈니스 변환:
#   1. KRW 환산액: 1 단위당 KRW 가치 (unit_mult 고려)
#   2. 스프레드: 매도-매수 차이 (tts - ttb) → 수수료/이윤 지표
#   3. 주요 통화 플래그: USD/EUR/JPY/CNY/GBP → is_major
#   4. 전일대비 변동률: 이전 영업일 대비 매매기준율 변화율

from pyspark.sql.functions import (
    col, when, lit, round as spark_round, expr
)
from pyspark.sql.window import Window

# ── 1) KRW 환산액 및 스프레드 계산 ──
gold_df = silver_df.select(
    col("search_date"),
    col("cur_unit"),
    col("cur_nm"),
    col("unit_mult"),
    col("deal_bas_r"),
    col("ttb"),
    col("tts"),
    col("bkpr"),
    # KRW 환산: 매매기준율 / unit_mult (예: JPY(100) → 100엔당 KRW → 1엔당 KRW)
    spark_round(col("deal_bas_r") / col("unit_mult"), 4).alias("krw_per_unit"),
    # 스프레드: 매도 - 매수 (은행 수익)
    spark_round(col("tts") - col("ttb"), 4).alias("spread"),
    # 스프레드율: 스프레드 / 매매기준율 * 100
    spark_round((col("tts") - col("ttb")) / col("deal_bas_r") * 100, 4).alias("spread_pct"),
)

# ── 2) 주요 통화 플래그 ──
major_currencies = ["USD", "EUR", "JPY", "CNY", "GBP", "CHF", "CAD", "AUD"]
gold_df = gold_df.withColumn(
    "is_major",
    when(col("cur_unit").isin(major_currencies), lit(True)).otherwise(lit(False))
)

# ── 3) 전일대비 변동률 ──
# 동일 통화의 이전 search_date 매매기준율과 비교
window = Window.partitionBy("cur_unit").orderBy(col("search_date"))
gold_df = gold_df.withColumn(
    "prev_deal_bas_r", 
    expr("lag(deal_bas_r) OVER (PARTITION BY cur_unit ORDER BY search_date)")
).withColumn(
    "change_rate",
    when(col("prev_deal_bas_r").isNotNull(),
         spark_round((col("deal_bas_r") - col("prev_deal_bas_r")) / col("prev_deal_bas_r") * 100, 4)
    ).otherwise(lit(None))
).drop("prev_deal_bas_r")

print(f"✅ Gold 요약 생성 완료: {gold_df.count()}행")
print(f"   스키마:")
gold_df.printSchema()

# 요약 결과 미리보기 — 주요 통화만
display(gold_df.filter(col("is_major") == True).select(
    "search_date", "cur_unit", "cur_nm", "deal_bas_r", 
    "krw_per_unit", "spread", "spread_pct", "change_rate"
).orderBy("cur_unit"))

# COMMAND ----------

# DBTITLE 1,Step 3: Gold Delta 테이블에 저장
# ============================================================
# Step 3: Gold Delta 테이블에 저장
# ============================================================
#
# Gold 테이블 설계:
#   • 카탈로그: main
#   • 스키마:   gold
#   • 테이블명: exchange_rate_summary
#   • 전체 경로: main.gold.exchange_rate_summary
#
# 적재 전략: OVERWRITE (매일 실행 시 최신 요약으로 교체)

CATALOG = "main"
SCHEMA  = "gold"
TABLE   = "exchange_rate_summary"
FULL_TABLE_NAME = f"{CATALOG}.{SCHEMA}.{TABLE}"

# ── 1) 카탈로그/스키마 생성 ──
spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
print(f"✅ 카탈로그/스키마 준비: {CATALOG}.{SCHEMA}")

# ── 2) Delta 테이블에 OVERWRITE ──
gold_df.write \
    .mode("overwrite") \
    .saveAsTable(FULL_TABLE_NAME)

print(f"✅ Gold 테이블 저장 완료: {FULL_TABLE_NAME}")

# ── 3) 최종 결과 확인 — 주요 통화 요약 ──
print(f"\n📊 주요 통화 환율 요약 ({FULL_TABLE_NAME})")
display(spark.sql(f"""
    SELECT 
        cur_unit,
        cur_nm,
        deal_bas_r  AS 매매기준율,
        krw_per_unit AS `1단위당_KRW`,
        spread      AS 스프레드,
        spread_pct  AS 스프레드율_pct,
        change_rate AS 전일대비_변동률_pct
    FROM {FULL_TABLE_NAME}
    WHERE is_major = true
    ORDER BY cur_unit
"""))

# ── 4) 전체 통화 요약 통계 ──
print(f"\n📈 전체 통화 통계:")
display(spark.sql(f"""
    SELECT 
        COUNT(*) AS 전체_통화수,
        SUM(CASE WHEN is_major THEN 1 ELSE 0 END) AS 주요통화수,
        ROUND(AVG(spread), 2) AS 평균_스프레드,
        ROUND(AVG(spread_pct), 4) AS 평균_스프레드율,
        ROUND(MAX(krw_per_unit), 2) AS 최대_KRW_환산,
        ROUND(MIN(krw_per_unit), 2) AS 최소_KRW_환산
    FROM {FULL_TABLE_NAME}
"""))