from flask import request
from services.db import get_connection

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

    # -----------------------------
    # 2) 기본 데이터 변환
    # -----------------------------
    date_list = [
        {
            "base": r["기준년월일"],                    # YYYYMMDD
            "year": r["기준년월일"][:4],               # YYYY
            "month": r["기준년월일"][4:6],             # MM
            "linked_regular": r["정기검증기준년월일"]   # 연결된 정기 기준일
        }
        for r in rows
    ]

    # 데이터가 없을 경우 빈 구조 반환
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

    # -----------------------------
    # 3) 연도 목록 생성
    # -----------------------------
    year_list = sorted({d["year"] for d in date_list}, reverse=True)

    selected_year = request.args.get("year", year_list[0])
    if selected_year not in year_list:
        selected_year = year_list[0]

    # -----------------------------
    # 4) 선택된 연도의 월 목록 생성
    # -----------------------------
    month_list = sorted(
        {d["month"] for d in date_list if d["year"] == selected_year},
        reverse=True
    )

    selected_month = request.args.get("month")
    if not selected_month or selected_month not in month_list:
        selected_month = month_list[0]

    # -----------------------------
    # 5) 기준년월일(base), 연동된 정기 기준일 찾기
    # -----------------------------
    selected_base = None
    selected_regular_base = None

    for d in date_list:
        if d["year"] == selected_year and d["month"] == selected_month:
            selected_base = d["base"]
            selected_regular_base = d["linked_regular"]
