# utils/base_filter.py
from flask import request
from services.db import get_connection

def load_base_date_rows():
    """DQ_BASE_DATE_INFO 전체 로딩 (공통)"""
    conn = get_connection()
    cur = conn.cursor()

    cur.execute("""
        SELECT 기준년월일, 검증차수, 검증구분, 정기검증기준년월일
        FROM DQ_BASE_DATE_INFO
        ORDER BY 기준년월일 DESC
    """)

    rows = cur.fetchall()
    conn.close()
    return rows


    rows = cur.fetchall()
    conn.close()
    return rows

def query_table_summary(selected_base, app_code, table_suffix=""):
    """
    table_suffix: ""(정기), "_OCCA"(수시)
    """

    conn = get_connection()
    cur = conn.cursor()

    # AppCode 목록
    cur.execute(f"""
        SELECT DISTINCT 어플리케이션코드
        FROM DQ_MF_ASSERTION_LIST{table_suffix}
        WHERE 기준년월일 = %s
        ORDER BY 1
    """, (selected_base,))
    app_code_list = [row["어플리케이션코드"] for row in cur.fetchall()]

    params = [selected_base, selected_base, selected_base, selected_base]

    sql = f"""
        SELECT
            A.기준년월일,
            A.어플리케이션코드,
            A.테이블명,
            SUM(CASE WHEN R.오류여부 = 'Y' THEN 1 ELSE 0 END) AS error_cnt,
            SUM(CASE WHEN R.오류여부 = 'N' THEN 1 ELSE 0 END) AS normal_cnt,
            ROUND(
                SUM(CASE WHEN R.오류여부 = 'Y' THEN 1 ELSE 0 END)
                / NULLIF(SUM(CASE WHEN R.오류여부 IN ('Y','N') THEN 1 ELSE 0 END), 0) * 100,
                2
            ) AS error_rate
        FROM DQ_MF_ASSERTION_LIST{table_suffix} A
        LEFT JOIN (
            SELECT 기준년월일, 서버코드, 테이블명, 컬럼명, 오류여부
            FROM DQ_MF_INST_RESULT{table_suffix} WHERE 기준년월일 = %s
            UNION ALL
            SELECT 기준년월일, 서버코드, 테이블명, 컬럼명, 오류여부
            FROM DQ_MF_DATE_RESULT{table_suffix} WHERE 기준년월일 = %s
            UNION ALL
            SELECT 기준년월일, 서버코드, 테이블명, 컬럼명, 오류여부
            FROM DQ_MF_LIST_RESULT{table_suffix} WHERE 기준년월일 = %s
        ) R
             ON  A.기준년월일 = R.기준년월일
            AND A.서버코드   = R.서버코드
            AND A.테이블명   = R.테이블명
            AND A.컬럼명     = R.컬럼명
        WHERE A.기준년월일 = %s
          AND A.제외여부 = 'N'
    """

    if app_code and app_code != "ALL":
        sql += " AND A.어플리케이션코드 = %s"
        params.append(app_code)

    sql += """
        GROUP BY A.기준년월일, A.어플리케이션코드, A.테이블명
        ORDER BY error_rate DESC;
    """

    cur.execute(sql, params)
    rows = cur.fetchall()

    cur.close()
    conn.close()

    return rows, app_code_list


    # utils/dashboard_utils.py



# ---------------------------------------------------
# 1) 정기 기준일 필터 처리
# ---------------------------------------------------
def get_regular_filter_context(request):
    conn = get_connection()
    cur = conn.cursor()

    cur.execute("""
        SELECT DISTINCT 기준년월일, 검증차수
        FROM DQ_BASE_DATE_INFO
        WHERE 검증구분 = '정기'
        ORDER BY 기준년월일 DESC
    """)
    rows = cur.fetchall()
    conn.close()

    # 데이터 변환
    date_list = [
        {
            "base": r["기준년월일"],
            "year": r["기준년월일"][:4],
            "cycle": r["검증차수"]
        }
        for r in rows
    ]

    year_list = sorted({d["year"] for d in date_list}, reverse=True)

    # 요청 파라미터
    selected_year = request.args.get("year")
    selected_cycle = request.args.get("cycle")

    # year 보정
    if not selected_year or selected_year not in year_list:
        selected_year = year_list[0]

    # cycle 목록 추출
    cycle_list = sorted({
        d["cycle"]
        for d in date_list
        if d["year"] == selected_year
    }, reverse=True)

    # cycle 보정
    if not selected_cycle or selected_cycle not in cycle_list:
        selected_cycle = cycle_list[0]

    # base 결정
    selected_base = next(
        (d["base"] for d in date_list if d["year"] == selected_year and d["cycle"] == selected_cycle),
        date_list[0]["base"]
    )

    return {
        "year_list": year_list,
        "cycle_list": cycle_list,
        "selected_year": selected_year,
        "selected_cycle": selected_cycle,
        "selected_base": selected_base
    }


# ---------------------------------------------------
# 2) Summary KPI 조회
# ---------------------------------------------------
def get_summary_kpi(base_date):
    conn = get_connection()
    cur = conn.cursor()

    sql = """
        SELECT
            db_type,
            (inst_err_cnt + list_err_cnt + ymd_err_cnt +
             inst_pass_cnt + list_pass_cnt + ymd_pass_cnt) AS total_cnt,
            (inst_err_cnt + list_err_cnt + ymd_err_cnt) AS error_cnt,
            (inst_pass_cnt + list_pass_cnt + ymd_pass_cnt) AS normal_cnt
        FROM DQ_SUMMARY_REPORT
        WHERE base_date=%s
    """

    cur.execute(sql, (base_date,))
    rows = cur.fetchall()

    cur.close()
    conn.close()
    return rows


# ---------------------------------------------------
# 3) 품질 KPI 요약
# ---------------------------------------------------
def get_quality_kpi(base_date):
    conn = get_connection()
    cur = conn.cursor()

    sql = """
        SELECT diagtype,
               COUNT(*) AS verified,
               SUM(CASE WHEN 오류여부='Y' THEN 1 ELSE 0 END) AS error
        FROM (
            SELECT 'I' AS diagtype, 오류여부 FROM DQ_MF_INST_RESULT WHERE 기준년월일=%s
            UNION ALL
            SELECT 'D' AS diagtype, 오류여부 FROM DQ_MF_DATE_RESULT WHERE 기준년월일=%s
            UNION ALL
            SELECT 'L' AS diagtype, 오류여부 FROM DQ_MF_LIST_RESULT WHERE 기준년월일=%s
        ) T
        GROUP BY diagtype
    """

    cur.execute(sql, (base_date, base_date, base_date))
    qrows = cur.fetchall()
    cur.close()
    conn.close()

    # 초기값
    kpi_inst = {"verified": 0, "error": 0, "quality": 0}
    kpi_date = {"verified": 0, "error": 0, "quality": 0}
    kpi_list = {"verified": 0, "error": 0, "quality": 0}

    for r in qrows:
        v = r["verified"]
        e = r["error"]
        q = round((v - e) / v * 100, 2) if v else 0

        if r["diagtype"] == "I":
            kpi_inst = {"verified": v, "error": e, "quality": q}
        elif r["diagtype"] == "D":
            kpi_date = {"verified": v, "error": e, "quality": q}
        else:
            kpi_list = {"verified": v, "error": e, "quality": q}

    total_verified = kpi_inst["verified"] + kpi_date["verified"] + kpi_list["verified"]
    total_error = kpi_inst["error"] + kpi_date["error"] + kpi_list["error"]
    total_quality = round((total_verified - total_error) / total_verified * 100, 2) if total_verified else 0

    kpi_all = {"verified": total_verified, "error": total_error, "quality": total_quality}

    return kpi_all, kpi_inst, kpi_date, kpi_list


# ---------------------------------------------------
# 4) 정비계획(최근 3개월)
# ---------------------------------------------------
def get_maint_chart():
    conn = get_connection()
    cur = conn.cursor()

    sql = """
        SELECT
            base_date,
            COUNT(*) AS target_cnt,
            SUM(CASE WHEN maint_plan_reg='Y' THEN 1 ELSE 0 END) AS registered_cnt,
            SUM(CASE WHEN maint_plan_yn='Y' THEN 1 ELSE 0 END) AS maint_yes,
            SUM(CASE WHEN maint_plan_yn='N' THEN 1 ELSE 0 END) AS maint_no,
            ROUND(SUM(CASE WHEN maint_plan_reg='Y' THEN 1 ELSE 0 END) / COUNT(*) * 100, 2) AS rate
        FROM DQ_MAINT_PLAN_TABLE
        GROUP BY base_date
        ORDER BY base_date DESC
        LIMIT 3
    """

    cur.execute(sql)
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return rows
