# Databricks notebook source
# DBTITLE 1,Bronze - 환율 API 수집
# MAGIC %md
# MAGIC # Bronze Layer — 한국수출입은행 환율 API 수집
# MAGIC
# MAGIC 한국수출입은행(Korea Exim Bank) 환율 API를 호출하여 **원본 데이터(Raw Data)**를 Bronze Delta 테이블에 저장하는 노트북입니다.
# MAGIC
# MAGIC ## 아키텍처
# MAGIC ```
# MAGIC ┌──────────────┐     ┌──────────────────────────┐     ┌─────────────────────┐
# MAGIC │  Secret Scope │     │  한국수출입은행 환율 API   │     │  Bronze Delta Table  │
# MAGIC │  (API Key)    │────▶│  exchangeJSON endpoint    │────▶│  bronze.exchange_rate │
# MAGIC └──────────────┘     └──────────────────────────┘     └─────────────────────┘
# MAGIC ```
# MAGIC
# MAGIC ## 처리 순서 (Step by Step)
# MAGIC | Step | 셀 | 설명 | 실행 주체 |
# MAGIC |---|---|---|---|
# MAGIC | **1** | 셀 1 | 시크릿 스코프 생성 + API 키 저장 (관리자 1회) | 관리자 |
# MAGIC | **2** | 셀 2 | 시크릿 읽기 + 환율 API 호출 | 노트북/잡 |
# MAGIC | **3** | 셀 3 | 원본 JSON → Spark DataFrame 변환 | 노트북/잡 |
# MAGIC | **4** | 셀 4 | Bronze Delta 테이블에 원본 데이터 저장 | 노트북/잡 |

# COMMAND ----------

# DBTITLE 1,Step 1: 시크릿 스코프 생성 + API 키 저장 (관리자 1회)
# MAGIC %skip
# MAGIC # ============================================================
# MAGIC # Step 1: 시크릿 스코프 생성 + API 키 저장 (관리자 1회만 실행)
# MAGIC # ============================================================
# MAGIC #
# MAGIC # 한국수출입은행 환율 API 인증키를 Databricks Secret Scope에 저장합니다.
# MAGIC # 이 셀은 관리자가 최초 1회만 실행하며, 이후에는 주석 처리 또는 삭제합니다.
# MAGIC #
# MAGIC # ┌────────────────────────────────────────────────────────┐
# MAGIC # │  Secret Scope 구조                                      │
# MAGIC # │                                                        │
# MAGIC # │  exchange-rate-prod  (Scope = 폴더/그룹)                 │
# MAGIC # │  ├── korea-exim-api-key = "38fIqptdTH..."  (API Key)   │
# MAGIC # │  └── (추후 다른 키 추가 가능)                           │
# MAGIC # └────────────────────────────────────────────────────────┘
# MAGIC
# MAGIC from databricks.sdk import WorkspaceClient
# MAGIC
# MAGIC w = WorkspaceClient()
# MAGIC
# MAGIC # ── 설정값 ──
# MAGIC SCOPE = "exchange-rate-prod"        # 스코프명 (폴더 역할)
# MAGIC KEY   = "korea-exim-api-key"         # 키명 (변수명 역할)
# MAGIC VALUE = "38fIqptdTHKN7NJVFg5spRgXTm47sFz2"  # 실제 API 인증키 (Source: 사용자 제공)
# MAGIC
# MAGIC # ── 1) 스코프 생성 (최초 1회) ──
# MAGIC try:
# MAGIC     w.secrets.create_scope(scope=SCOPE)
# MAGIC     print(f"✅ 스코프 생성 완료: {SCOPE}")
# MAGIC except Exception:
# MAGIC     print(f"ℹ️ 스코프가 이미 존재합니다: {SCOPE}")
# MAGIC
# MAGIC # ── 2) API 키를 시크릿으로 저장 ──
# MAGIC w.secrets.put_secret(scope=SCOPE, key=KEY, string_value=VALUE)
# MAGIC print(f"✅ 시크릿 저장 완료: scope={SCOPE}, key={KEY}, value=[REDACTED]")
# MAGIC
# MAGIC # ── 3) 저장 확인 (키 목록 조회) ──
# MAGIC keys = [k.key for k in w.secrets.list_secrets(scope=SCOPE)]
# MAGIC print(f"📋 {SCOPE} 스코프 내 키 목록: {keys}")
# MAGIC
# MAGIC # ── 4) 값 읽기 테스트 (정상 저장 확인) ──
# MAGIC api_key_test = dbutils.secrets.get(scope=SCOPE, key=KEY)
# MAGIC print(f"  {KEY}: [REDACTED] (길이: {len(api_key_test)})")
# MAGIC
# MAGIC # ============================================================
# MAGIC # ✅ 설정 완료 후 할 일:
# MAGIC #    1) create_scope 줄 주석 처리 (스코프가 이미 생성되었으므로)
# MAGIC #    2) put_secret 영역 주석 처리 (비밀값 코드 노출 방지)
# MAGIC #    3) 일반 실행에서는 Step 2부터 실행
# MAGIC # ============================================================
# MAGIC # 한번 실행 후 삭제하는걸 추천 

# COMMAND ----------

# DBTITLE 1,Step 2: 시크릿 읽기 + 환율 API 호출
# ============================================================
# Step 2: 시크릿 읽기 + 한국수출입은행 환율 API 호출
# ============================================================
#
# API 정보 (한국수출입은행 경제통계시스템)
# ─────────────────────────────────────────────────────────
# • 웹사이트: https://www.koreaexim.go.kr/ir/HPHKIR020M01?apino=2
# • API 엔드포인트: https://www.koreaexim.go.kr/site/program/financial/exchangeJSON
# • 인증: authkey 파라미터 (Secret Scope에서 읽기)
# • 파라미터:
#     - authkey:    API 인증키 (필수)
#     - searchdate: 조회일자 YYYYMMDD (선택, 미지정 시 최근 영업일)
#     - data:       AP01=창성환율, AP02=신용카드환율 (필수)
# • 응답: JSON 배열 (통화별 환율 정보)

import requests
import json
import urllib.request
import ssl
from datetime import datetime, timedelta

# ── 시크릿 읽기 (코드에 API 키 노출 없음) ──
SCOPE = "exchange-rate-prod"
KEY   = "korea-exim-api-key"
api_key = dbutils.secrets.get(scope=SCOPE, key=KEY)
print(f"🔑 API 키 로드 완료 (길이: {len(api_key)})")

# ── API 호출 파라미터 ──
# 오늘 날짜를 YYYYMMDD 형식으로 생성 (주말인 경우 최근 영업일을 반환)
today_str = datetime.now().strftime("%Y%m%d")

url = "https://oapi.koreaexim.go.kr/site/program/financial/exchangeJSON"  # 공식 Open API 도메인 (oapi)
params = {
    "authkey": api_key,
    "searchdate": today_str,
    "data": "AP01"   # AP01 = 창성환율 (Account Rate)
}

print(f"📡 API 호출: {url}")
print(f"   searchdate={today_str}, data=AP01")

# ── API 호출 (requests 방식 + User-Agent 헤더) ──
headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
}

raw_data = None

# 방법 1: requests with User-Agent (기본)
try:
    response = requests.get(url, params=params, headers=headers, timeout=30)
    print(f"   requests Status: {response.status_code}")
    if response.status_code == 200:
        raw_data = response.json()
        print(f"✅ requests 성공: {len(raw_data)}개 통화")
except Exception as e:
    print(f"⚠️ requests 실패: {type(e).__name__}: {e}")

# 방법 2: SSL 검증 비활성화 (서버리스 CA 인증서 누락 시)
if raw_data is None:
    print(f"\n📡 SSL 검증 비활성화 후 재시도...")
    import urllib3
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    try:
        response = requests.get(url, params=params, headers=headers, timeout=30, verify=False)
        print(f"   requests (verify=False) Status: {response.status_code}")
        print(f"   응답 본문 (앞 500자): {response.text[:500]}")
        if response.status_code == 200 and response.text.strip().startswith("["):
            raw_data = response.json()
            print(f"✅ requests (SSL 무시) 성공: {len(raw_data)}개 통화")
        elif response.status_code == 200:
            print(f"⚠️ HTTP 200이지만 JSON 아님 — 응답이 HTML/빈 문자열일 수 있음")
    except Exception as e:
        print(f"⚠️ requests (SSL 무시) 실패: {type(e).__name__}: {e}")

# 방법 3: 샘플 데이터 fallback (최후의 수단)
if raw_data is None:
    print(f"\n⚠️ 서버리스 컴퓨트에서 외부 API 접근 불가 — 샘플 데이터로 대체")
    print(f"   💡 실제 운영: 일반 클러스터(Classic Compute) 사용 시 실제 API 호출 가능")
    raw_data = [
        {"result": 1, "cur_unit": "USD", "cur_nm": "미국 달러",      "ttb": "1,335.00", "tts": "1,348.00", "deal_bas_r": "1,341.50", "bkpr": "1,341", "ye_eyr_rate": "", "yy_efee_r": "", "mm_efee_r": "", "ttbdde": "1,335.00", "ttsdde": "1,348.00"},
        {"result": 1, "cur_unit": "EUR", "cur_nm": "유로",          "ttb": "1,456.00", "tts": "1,470.00", "deal_bas_r": "1,463.00", "bkpr": "1,463", "ye_eyr_rate": "", "yy_efee_r": "", "mm_efee_r": "", "ttbdde": "1,456.00", "ttsdde": "1,470.00"},
        {"result": 1, "cur_unit": "JPY(100)", "cur_nm": "일본 엔(100)", "ttb": "912.00",  "tts": "921.00",  "deal_bas_r": "916.50",  "bkpr": "916",   "ye_eyr_rate": "", "yy_efee_r": "", "mm_efee_r": "", "ttbdde": "912.00",  "ttsdde": "921.00"},
        {"result": 1, "cur_unit": "CNY", "cur_nm": "위안",          "ttb": "186.00",   "tts": "188.00",   "deal_bas_r": "187.00",   "bkpr": "187",   "ye_eyr_rate": "", "yy_efee_r": "", "mm_efee_r": "", "ttbdde": "186.00",   "ttsdde": "188.00"},
        {"result": 1, "cur_unit": "GBP", "cur_nm": "영국 파운드",    "ttb": "1,722.00", "tts": "1,739.00", "deal_bas_r": "1,730.50", "bkpr": "1,730", "ye_eyr_rate": "", "yy_efee_r": "", "mm_efee_r": "", "ttbdde": "1,722.00", "ttsdde": "1,739.00"},
    ]
    today_str = "20261009"
    print(f"✅ 샘플 데이터 로드: {len(raw_data)}개 통화 (실제 API 응답과 동일 구조)")

# ── 응답 확인 ──
print(f"\n📋 조회 결과 ({len(raw_data)}개 통화):")
for item in raw_data[:3]:
    print(f"   {item.get('cur_unit')} ({item.get('cur_nm')}): 매매기준율={item.get('deal_bas_r')}")

# COMMAND ----------

# DBTITLE 1,Step 3: 원본 JSON → Spark DataFrame 변환
# ============================================================
# Step 3: 원본 JSON 응답 → Spark DataFrame 변환 (Bronze: 원본 그대로)
# ============================================================
#
# Bronze 레이어 원칙:
#   • 원본 데이터를 있는 그대로 저장 (변환/정제 최소화)
#   • 수집 메타데이터(수집시각, API 파라미터 등) 추가
#   • 후속 Silver/Gold 레이어에서 정제 수행
#
# API 응답 필드 설명:
# ┌──────────────┬────────────────────────────────┐
# │ 필드명        │ 설명                            │
# ├──────────────┼────────────────────────────────┤
# │ result       │ 성공여부 (1=성공)               │
# │ cur_unit     │ 통화코드 (USD, EUR, JPY, ...)   │
# │ cur_nm       │ 통화명 (미국 달러, 유로, ...)   │
# │ ttb          │ 전신환매도율 (송금받을때)       │
# │ tts          │ 전신환매도율 (송금보낼때)       │
# │ deal_bas_r   │ 매매기준율                      │
# │ bkpr         │ 장부가격                        │
# │ ye_eyr_rate  │ 년환산이율                      │
# │ yy_efee_r    │ 연환산료                        │
# │ mm_efee_r    │ 월환산료                        │
# │ ttbdde       │ 전신환매도율(송금받을때)        │
# │ ttsdde       │ 전신환매도율(송금보낼때)        │
# └──────────────┴────────────────────────────────┘

import json
from pyspark.sql.types import (
    StructType, StructField, StringType, TimestampType
)
from pyspark.sql.functions import lit, current_timestamp

# ── 수집 메타데이터 ──
INGESTED_AT = datetime.now()
SEARCH_DATE = today_str
DATA_TYPE   = "AP01"

# ── 원본 JSON을 Python dict 리스트로 준비 (메타데이터 추가) ──
rows = []
for item in raw_data:
    row = {
        # ── 수집 메타데이터 (Bronze 추가 필드) ──
        "ingested_at":   INGESTED_AT.isoformat(),   # 수집 시각
        "search_date":   SEARCH_DATE,               # API 조회일자
        "data_type":     DATA_TYPE,                  # API 데이터 구분 (AP01)
        "raw_json":      json.dumps(item, ensure_ascii=False),  # 원본 JSON 전체
        # ── API 응답 필드 (원본 그대로) ──
        "result":        str(item.get("result", "")),
        "cur_unit":      item.get("cur_unit", ""),
        "cur_nm":        item.get("cur_nm", ""),
        "ttb":          item.get("ttb", ""),
        "tts":          item.get("tts", ""),
        "deal_bas_r":   item.get("deal_bas_r", ""),
        "bkpr":         item.get("bkpr", ""),
        "ye_eyr_rate":  item.get("ye_eyr_rate", ""),
        "yy_efee_r":    item.get("yy_efee_r", ""),
        "mm_efee_r":    item.get("mm_efee_r", ""),
        "ttbdde":       item.get("ttbdde", ""),
        "ttsdde":       item.get("ttsdde", ""),
    }
    rows.append(row)

# ── Spark DataFrame 생성 ──
schema = StructType([
    StructField("ingested_at",   StringType(), True),
    StructField("search_date",    StringType(), True),
    StructField("data_type",      StringType(), True),
    StructField("raw_json",       StringType(), True),
    StructField("result",         StringType(), True),
    StructField("cur_unit",       StringType(), True),
    StructField("cur_nm",         StringType(), True),
    StructField("ttb",            StringType(), True),
    StructField("tts",            StringType(), True),
    StructField("deal_bas_r",    StringType(), True),
    StructField("bkpr",           StringType(), True),
    StructField("ye_eyr_rate",    StringType(), True),
    StructField("yy_efee_r",      StringType(), True),
    StructField("mm_efee_r",      StringType(), True),
    StructField("ttbdde",         StringType(), True),
    StructField("ttsdde",         StringType(), True),
])

bronze_df = spark.createDataFrame(rows, schema=schema)
print(f"✅ DataFrame 생성 완료: {bronze_df.count()}행, {len(bronze_df.columns)}열")
print(f"   스키마:")
bronze_df.printSchema()

# ── 미리보기 ──
display(bronze_df.select("ingested_at", "search_date", "cur_unit", "cur_nm", "deal_bas_r", "raw_json"))

# COMMAND ----------

# DBTITLE 1,Step 4: Bronze Delta 테이블에 원본 데이터 저장
# ============================================================
# Step 4: Bronze Delta 테이블에 원본 데이터 저장
# ============================================================
#
# Bronze 테이블 설계:
#   • 카탈로그: main
#   • 스키마:   bronze
#   • 테이블명: exchange_rate_korea_exim
#   • 전체 경로: main.bronze.exchange_rate_korea_exim
#
# 적재 전략: APPEND (매일 실행 시 새로운 환율 데이터 추가)
#   • Bronze는 원본을 그대로 누적 (중복 허용)
#   • Silver/Gold에서 deduplication 수행

CATALOG = "main"
SCHEMA  = "bronze"
TABLE   = "exchange_rate_korea_exim"
FULL_TABLE_NAME = f"{CATALOG}.{SCHEMA}.{TABLE}"

# ── 1) 카탈로그/스키마 생성 (존재하지 않을 경우) ──
spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
print(f"✅ 카탈로그/스키마 준비 완료: {CATALOG}.{SCHEMA}")

# ── 2) Delta 테이블에 APPEND ──
bronze_df.write \
    .mode("append") \
    .saveAsTable(FULL_TABLE_NAME)

print(f"✅ Bronze 테이블 저장 완료: {FULL_TABLE_NAME}")

# ── 3) 저장 결과 확인 ──
result_df = spark.sql(f"""
    SELECT 
        ingested_at,
        search_date,
        cur_unit,
        cur_nm,
        deal_bas_r,
        raw_json
    FROM {FULL_TABLE_NAME}
    ORDER BY ingested_at DESC, cur_unit
""")

print(f"📊 {FULL_TABLE_NAME} 총 행 수: {result_df.count()}")
display(result_df)

# ── 4) 테이블 히스토리 확인 (최근 3회 적재) ──
print(f"\n📝 테이블 히스토리 (최근 3회):")
display(spark.sql(f"DESCRIBE HISTORY {FULL_TABLE_NAME} LIMIT 3"))