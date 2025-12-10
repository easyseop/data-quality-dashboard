from services.db import get_connection
from flask import request

def get_filter_context_regular():
    conn = get_connection()
    cur = conn.cursor()

    # 1) 정기 데이터 조회
    cur.execute("""
        SELECT 기준년월일, 검증차수, 검증구분
        FROM DQ_BASE_DATE_INFO
        WHERE 검증구분 = '정기'
        ORDER BY 기준년월일 DESC
    """)
    rows = cur.fetchall()
    conn.close()

    # 2) 변환
    date_list = [
        {
            "base": r["기준년월일"],             # YYYYMMDD
            "year": r["기준년월일"][:4],        # YYYY
            "cycle": r["검증차수"]               # 상반기 / 하반기
        }
        for r in rows
    ]

    # 3) 필터용 년도 목록
    year_list = sorted({d["year"] for d in date_list}, reverse=True)

    # 선택된 필터 (기본값 최신)
    selected_year = request.args.get("year", year_list[0])

    # 4) 해당 연도의 차수 목록만 표시
    cycle_list = [d["cycle"] for d in date_list if d["year"] == selected_year]
    cycle_list = list(dict.fromkeys(cycle_list))  # 중복 제거
    selected_cycle = request.args.get("cycle", cycle_list[0])

    # 5) 최종 기준년월일(base) 선택
    selected_base = None
    for d in date_list:
        if d["year"] == selected_year and d["cycle"] == selected_cycle:
            selected_base = d["base"]
            break

    # 선택 연도/차수 조합이 존재하지 않을 경우 → 가장 최신 정기 기준일 사용
    if not selected_base:
        selected_base = date_list[0]["base"]

    return {
        "date_list": date_list,
        "year_list": year_list,
        "cycle_list": cycle_list,
        "selected_year": selected_year,
        "selected_cycle": selected_cycle,
        "selected_base": selected_base
    }



# ============================================================
# 🔧 공통 함수: 수시 필터 구성
# ============================================================
def get_filter_context_occa():
    conn = get_connection()
    cur = conn.cursor()

    # 1) 수시 데이터 조회
    cur.execute("""
        SELECT 기준년월일, 검증구분, 정기검증기준년월일
        FROM DQ_BASE_DATE_INFO
        WHERE 검증구분 = '수시'
        ORDER BY 기준년월일 DESC
    """)
    rows = cur.fetchall()
    conn.close()

    # 2) 변환
    date_list = [
        {
            "base": r["기준년월일"],                 # YYYYMMDD
            "year": r["기준년월일"][:4],            # YYYY
            "month": r["기준년월일"][4:6],          # MM
            "linked_regular": r["정기검증기준년월일"]    # 연결된 정기 기준일
        }
        for r in rows
    ]

    if not date_list:
        return {
            "date_list": [],
            "year_list": [],
            "month_list": [],
            "selected_year": None,
            "selected_month": None,
            "selected_base": None,
            "selected_regular_base": None
        }

    # 3) 연도 목록
    year_list = sorted({d["year"] for d in date_list}, reverse=True)

    selected_year = request.args.get("year", year_list[0])

    # 4) 선택된 연도의 월 목록
    month_list = sorted({d["month"] for d in date_list if d["year"] == selected_year}, reverse=True)

    selected_month = request.args.get("month", month_list[0])

    # 5) 기준년월일(base) 찾기
    selected_base = None
    selected_regular_base = None

    for d in date_list:
        if d["year"] == selected_year and d["month"] == selected_month:
            selected_base = d["base"]
            selected_regular_base = d["linked_regular"]
            break

    # 기본값 처리
    if not selected_base:
        selected_base = date_list[0]["base"]
        selected_regular_base = date_list[0]["linked_regular"]

    return {
        "date_list": date_list,
        "year_list": year_list,
        "month_list": month_list,
        "selected_year": selected_year,
        "selected_month": selected_month,
        "selected_base": selected_base,
        "selected_regular_base": selected_regular_base
    }