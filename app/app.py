from flask import Flask,send_file ,render_template, request, jsonify
from sample_data import sample_tables, sample_columns, sample_column_detail
from collections import defaultdict
from services.db import get_connection
from utils.filter_occa import *
from utils.filter_regular import *
from utils.filter_base import *
import pandas as pd
from io import BytesIO

app = Flask(__name__)


# ===================== Dashboard ( / ) ===================== #
@app.route("/")
def dashboard():
    # ---- 정기 기준일 필터만 사용 ----
    ctx = get_regular_filter_context(request)

    # 🔥 dtype은 필터 UI에는 보이지만 실제 동작은 정기만
    selected_dtype = "정기"

    selected_year  = ctx["selected_year"]
    selected_cycle = ctx["selected_cycle"]
    selected_base  = ctx["selected_base"]
    year_list      = ctx["year_list"]
    cycle_list     = ctx["cycle_list"]

    # ---- Summary KPI ----
    overall_kpi = get_summary_kpi(selected_base)

    # ---- 품질 KPI ----
    kpi_all, kpi_inst, kpi_date, kpi_list = get_quality_kpi(selected_base)

    # ---- 정비계획 ----
    maint_chart = get_maint_chart()

    return render_template(
        "dashboard.html",
        year_list=year_list,
        cycle_list=cycle_list,

        selected_year=selected_year,
        selected_cycle=selected_cycle,
        selected_dtype=selected_dtype,   # 🔥 필터는 표시용으로 유지
        selected_base=selected_base,

        overall_kpi=overall_kpi,
        kpi_all=kpi_all,
        kpi_inst=kpi_inst,
        kpi_date=kpi_date,
        kpi_list=kpi_list,
        maint_chart=maint_chart
    )




# ===================== summary download ( / ) ===================== #

@app.route("/download/summary", methods=["GET"])
def download_summary():
    # 최신 기준일자 또는 선택 기준일자
    target_date = request.args.get("date", None)

    conn = get_connection()
    cur = conn.cursor()

    # ---- 1) SUMMARY 데이터 조회 ----
    cur.execute("""
        SELECT base_date, db_type,
               inst_err_cnt, list_err_cnt, ymd_err_cnt,
               inst_pass_cnt, list_pass_cnt, ymd_pass_cnt
        FROM DQ_SUMMARY_REPORT
        WHERE base_date = %s
        ORDER BY db_type
    """, (target_date,))
    rows = cur.fetchall()

    cur.close()
    conn.close()

    # DataFrame 변환
    df = pd.DataFrame(rows)

    # ---- 2) 신규 집계 컬럼 추가 ----
    df["total_err"] = df["inst_err_cnt"] + df["list_err_cnt"] + df["ymd_err_cnt"]
    df["total_pass"] = df["inst_pass_cnt"] + df["list_pass_cnt"] + df["ymd_pass_cnt"]
    df["total"] = df["total_err"] + df["total_pass"]
    df["quality_rate(%)"] = round(df["total_pass"] / df["total"] * 100, 2)

    # ---- 3) 엑셀 생성 ----
    output = BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name=f"Summary_{target_date}")

        # 📌 추후 지표 확장 가이드
        # - 신규 지표가 추가될 경우:
        #   1) SELECT SQL에 신규 컬럼 추가
        #   2) df["column_name"] = 계산식 or raw value
        #   3) df.to_excel() 그대로 실행하면 반영 완료됨

    output.seek(0)

    filename = f"DataQuality_Summary_{target_date}.xlsx"
    return send_file(
        output,
        as_attachment=True,
        download_name=filename,
        mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )


# ===================== Trend Main ===================== #
@app.route("/trend")
def trend():
    return render_template("trend.html")


# =====================  /trend/seq 연속오류 분석  ===================== #
@app.route("/trend/seq")
def trend_view():
    page = int(request.args.get("page", 1))
    per_page = 10
    selected_app = request.args.get("app", "ALL")
    selected_etype = request.args.get("etype", "ALL")

    conn = get_connection()
    cur = conn.cursor()

    # ===== 기준년월일 목록 (정기만 사용) =====
    cur.execute("""
        SELECT DISTINCT 기준년월일, 검증차수, 검증구분
        FROM DQ_BASE_DATE_INFO
        WHERE 검증구분 = '정기'       -- 🔥 수시는 제외
        ORDER BY 기준년월일 DESC
    """)
    raw_rows = cur.fetchall()

    date_list = [
        {
            "base": r["기준년월일"],
            "year": r["기준년월일"][:4],
            "cycle": r["검증차수"],
            "type": r["검증구분"]
        }
        for r in raw_rows
    ]

    # ===== 필터 목록 =====
    year_list = sorted({d["year"] for d in date_list}, reverse=True)
    dtype_list = ["정기"]     # 🔥 dtype 고정 → 수시 선택 불가

    selected_year = request.args.get("year", year_list[0])
    selected_dtype = "정기"   # 🔥 항상 정기

    # ===== cycle list =====
    cycle_list = sorted({
        d["cycle"]
        for d in date_list
        if d["year"] == selected_year
    }, reverse=True)

    selected_cycle = request.args.get("cycle", cycle_list[0])

    # ===== base date (정기만) =====
    try:
        selected_base = next(
            d["base"] for d in date_list
            if d["year"] == selected_year and d["cycle"] == selected_cycle
        )
    except StopIteration:
        selected_base = date_list[0]["base"]

    # ===== D1, D2, D3 (정기만) =====
    filtered_for_seq = sorted([d["base"] for d in date_list], reverse=True)

    d1 = selected_base
    idx = filtered_for_seq.index(selected_base)
    d2 = filtered_for_seq[idx+1] if idx + 1 < len(filtered_for_seq) else None
    d3 = filtered_for_seq[idx+2] if idx + 2 < len(filtered_for_seq) else None

    # ===== APP 목록 =====
    cur.execute("SELECT DISTINCT 어플리케이션코드 FROM DQ_MF_ASSERTION_LIST ORDER BY 1")
    app_list = [row["어플리케이션코드"] for row in cur.fetchall()]

    app_sql = "" if selected_app == "ALL" else f"AND A.어플리케이션코드='{selected_app}'"

    # ===== 연속 오류 SQL =====
    sql = f"""
        WITH recent_only AS (
            SELECT A.어플리케이션코드 AS app_code, A.테이블명 AS table_name, A.컬럼명 AS column_name
            FROM DQ_MF_ASSERTION_LIST A
            JOIN (
                SELECT 기준년월일, 서버코드, 테이블명, 컬럼명, 오류여부 FROM DQ_MF_INST_RESULT
                UNION ALL
                SELECT 기준년월일, 서버코드, 테이블명, 컬럼명, 오류여부 FROM DQ_MF_DATE_RESULT
                UNION ALL
                SELECT 기준년월일, 서버코드, 테이블명, 컬럼명, 오류여부 FROM DQ_MF_LIST_RESULT
            ) R
            ON A.기준년월일 = R.기준년월일
            AND A.서버코드 = R.서버코드
            AND A.테이블명 = R.테이블명
            AND A.컬럼명 = R.컬럼명
            WHERE R.기준년월일='{d1}' AND R.오류여부='Y'
            {app_sql}
        ),
        merged AS (
            SELECT 기준년월일, 테이블명, 컬럼명, 오류여부
            FROM (
                SELECT 기준년월일, 테이블명, 컬럼명, 오류여부 FROM DQ_MF_INST_RESULT
                UNION ALL
                SELECT 기준년월일, 테이블명, 컬럼명, 오류여부 FROM DQ_MF_DATE_RESULT
                UNION ALL
                SELECT 기준년월일, 테이블명, 컬럼명, 오류여부 FROM DQ_MF_LIST_RESULT
            ) X
            WHERE 기준년월일 IN ('{d1}' {f",'{d2}'" if d2 else ""} {f",'{d3}'" if d3 else ""})
        )
        SELECT r.app_code, r.table_name, r.column_name,
            MAX(CASE WHEN m.기준년월일='{d1}' THEN m.오류여부 END) AS d1,
            MAX(CASE WHEN m.기준년월일='{d2}' THEN m.오류여부 END) AS d2,
            MAX(CASE WHEN m.기준년월일='{d3}' THEN m.오류여부 END) AS d3
        FROM recent_only r
        LEFT JOIN merged m
        ON r.table_name=m.테이블명 AND r.column_name=m.컬럼명
        GROUP BY r.app_code, r.table_name, r.column_name
    """

    cur.execute(sql)
    records = cur.fetchall()
    cur.close()
    conn.close()

    # ===== seq 계산 =====
    rows = []
    for r in records:
        seq = 1
        if r["d1"] == "Y" and r["d2"] == "Y":
            seq = 2
            if r["d3"] == "Y":
                seq = 3

        error_type = "신규오류" if seq == 1 and r["d2"] != "Y" else "연속오류"
        rows.append({**r, "seq": seq, "error_type": error_type})

    # ===== 오류 유형 필터 =====
    if selected_etype == "NEW":
        rows = [r for r in rows if r["error_type"] == "신규오류"]
    elif selected_etype == "SEQ":
        rows = [r for r in rows if r["error_type"] != "신규오류"]

    rows = sorted(rows, key=lambda x: (x["seq"], x["error_type"] == "신규오류"), reverse=True)

    total = len(rows)
    total_pages = (total + per_page - 1) // per_page
    rows = rows[(page - 1) * per_page : page * per_page]

    return render_template(
        "trend_seq.html",
        rows=rows,
        year_list=year_list,
        dtype_list=dtype_list,
        cycle_list=cycle_list,
        selected_year=selected_year,
        selected_dtype="정기",
        selected_cycle=selected_cycle,
        selected_app=selected_app,
        selected_etype=selected_etype,
        d1=d1, d2=d2, d3=d3,
        page=page, total_pages=total_pages,
        total_count=total,
        per_page=per_page,
        app_list=app_list
    )



@app.route("/owner/regular")
def owner_regular_view():
    page = int(request.args.get("page", 1))
    per_page = 10
    selected_app = request.args.get("app", "ALL")

    ctx = get_filter_context_regular()
    selected_base = ctx["selected_base"]

    conn = get_connection()
    cur = conn.cursor()

    # App 목록
    cur.execute("""
        SELECT DISTINCT 어플리케이션코드
        FROM DQ_MF_ASSERTION_LIST
        WHERE 기준년월일 = %s
        ORDER BY 1
    """, (selected_base,))
    app_list = [row["어플리케이션코드"] for row in cur.fetchall()]

    app_sql = "" if selected_app == "ALL" else f" AND A.어플리케이션코드='{selected_app}' "

    # 오류 담당자 조회 SQL
    sql = f"""
        WITH err AS (
            SELECT A.어플리케이션코드 AS app_code,
                   COUNT(*) AS error_cols
            FROM DQ_MF_ASSERTION_LIST A
            JOIN (
                SELECT 기준년월일, 서버코드, 테이블명, 컬럼명, 오류여부 FROM DQ_MF_INST_RESULT
                UNION ALL
                SELECT 기준년월일, 서버코드, 테이블명, 컬럼명, 오류여부 FROM DQ_MF_DATE_RESULT
                UNION ALL
                SELECT 기준년월일, 서버코드, 테이블명, 컬럼명, 오류여부 FROM DQ_MF_LIST_RESULT
            ) R
            ON A.기준년월일 = R.기준년월일
            AND A.서버코드 = R.서버코드
            AND A.테이블명 = R.테이블명
            AND A.컬럼명 = R.컬럼명
            WHERE R.오류여부='Y'
              AND R.기준년월일='{selected_base}'
              {app_sql}
            GROUP BY A.어플리케이션코드
        )
        SELECT e.app_code, e.error_cols,
               M.user_nm, M.user_id, M.org_nm, M.brn_nm
        FROM err e
        LEFT JOIN DQ_TBL_MANAGER_INFO M
        ON e.app_code = M.app_code
        ORDER BY e.error_cols DESC, M.user_nm
    """

    cur.execute(sql)
    result = cur.fetchall()

    # 페이징
    total = len(result)
    total_pages = (total + per_page - 1) // per_page
    sliced = result[(page-1)*per_page : page*per_page]

    rows = [{"rownum": i+1+(page-1)*per_page, **r} for i, r in enumerate(sliced)]

    cur.close()
    conn.close()

    return render_template(
        "owner_regular.html",
        rows=rows,
        app_list=app_list,
        selected_app=selected_app,
        page=page,
        total_pages=total_pages,
        **ctx
    )



# ============================================================
# 📌 수시 검증 담당자 화면
# ============================================================
@app.route("/owner/occa")
def owner_occa_view():
    page = int(request.args.get("page", 1))
    per_page = 10
    selected_app = request.args.get("app", "ALL")

    ctx = get_filter_context_occa()
    selected_base = ctx["selected_base"]

    conn = get_connection()
    cur = conn.cursor()

    # 앱 목록
    cur.execute("""
        SELECT DISTINCT 어플리케이션코드
        FROM DQ_MF_ASSERTION_LIST_OCCA
        WHERE 기준년월일 = %s
        ORDER BY 1
    """, (selected_base,))
    app_list = [row["어플리케이션코드"] for row in cur.fetchall()]

    app_sql = "" if selected_app == "ALL" else f" AND A.어플리케이션코드='{selected_app}' "

    sql = f"""
        WITH err AS (
            SELECT A.어플리케이션코드 AS app_code,
                   COUNT(*) AS error_cols
            FROM DQ_MF_ASSERTION_LIST_OCCA A
            JOIN (
                SELECT 기준년월일, 서버코드, 테이블명, 컬럼명, 오류여부 FROM DQ_MF_INST_RESULT_OCCA
                UNION ALL
                SELECT 기준년월일, 서버코드, 테이블명, 컬럼명, 오류여부 FROM DQ_MF_DATE_RESULT_OCCA
                UNION ALL
                SELECT 기준년월일, 서버코드, 테이블명, 컬럼명, 오류여부 FROM DQ_MF_LIST_RESULT_OCCA
            ) R
            ON A.기준년월일 = R.기준년월일
            AND A.서버코드 = R.서버코드
            AND A.테이블명 = R.테이블명
            AND A.컬럼명 = R.컬럼명
            WHERE R.오류여부='Y'
              AND R.기준년월일='{selected_base}'
              {app_sql}
            GROUP BY A.어플리케이션코드
        )
        SELECT e.app_code, e.error_cols,
               M.user_nm, M.user_id, M.org_nm, M.brn_nm
        FROM err e
        LEFT JOIN DQ_TBL_MANAGER_INFO M
        ON e.app_code = M.app_code
        ORDER BY e.error_cols DESC, M.user_nm
    """

    cur.execute(sql)
    result = cur.fetchall()

    total = len(result)
    total_pages = (total + per_page - 1) // per_page
    sliced = result[(page-1)*per_page : page*per_page]

    rows = [{"rownum": i+1+(page-1)*per_page, **r} for i, r in enumerate(sliced)]

    cur.close()
    conn.close()

    return render_template(
        "owner_occa.html",
        rows=rows,
        app_list=app_list,
        selected_app=selected_app,
        page=page,
        total_pages=total_pages,
        **ctx
    )



# ===================== 2) Tables ( /tables ) ===================== #
@app.route("/tables/regular")
def tables_regular_view():
    ctx = get_filter_context_regular()
    selected_base = ctx["selected_base"]

    selected_app = request.args.get("app", "ALL")

    rows, app_code_list = query_table_summary(
        selected_base,
        selected_app,
        table_suffix=""    # 정기
    )

    return render_template(
        "tables_regular.html",
        tables=rows,
        app_code_list=app_code_list,
        selected_app=selected_app,
        **ctx
    )


# ---------------------------------------------------
# 3) 수시 테이블 페이지
# ---------------------------------------------------
@app.route("/tables/occa")
def tables_occa_view():
    ctx = get_filter_context_occa()
    selected_base = ctx["selected_base"]

    selected_app = request.args.get("app", "ALL")

    rows, app_code_list = query_table_summary(
        selected_base,
        selected_app,
        table_suffix="_OCCA"   # 수시
    )

    return render_template(
        "tables_occa.html",
        tables=rows,
        app_code_list=app_code_list,
        selected_app=selected_app,
        **ctx
    )




# ===================== 3) Detail ( /detail/<table_name> ) ===================== #
@app.route("/detail/<mode>/<table_name>")
def table_detail(mode, table_name):
    selected_date = request.args.get("date")
    regular_base = request.args.get("regular")  # 수시일 경우만 존재

    # mode → suffix
    if mode == "regular":
        selected_type = "정기"
        suffix = ""
    else:
        selected_type = "수시"
        suffix = "_OCCA"

    conn = get_connection()
    cur = conn.cursor()

    sql = f"""
        SELECT
            A.컬럼명 AS column_name,
            SUM(CASE WHEN R.오류여부='Y' THEN 1 ELSE 0 END) AS error_cnt,
            SUM(CASE WHEN R.오류여부='N' THEN 1 ELSE 0 END) AS normal_cnt,
            ROUND(
                SUM(CASE WHEN R.오류여부='Y' THEN 1 ELSE 0 END) /
                NULLIF(SUM(CASE WHEN R.오류여부 IN ('Y','N') THEN 1 ELSE 0 END), 0) * 100,
                2
            ) AS error_rate
        FROM DQ_MF_ASSERTION_LIST{suffix} A
        LEFT JOIN (
            SELECT 기준년월일, 서버코드, 테이블명, 컬럼명, 오류여부
            FROM DQ_MF_INST_RESULT{suffix}
            UNION ALL
            SELECT 기준년월일, 서버코드, 테이블명, 컬럼명, 오류여부
            FROM DQ_MF_DATE_RESULT{suffix}
            UNION ALL
            SELECT 기준년월일, 서버코드, 테이블명, 컬럼명, 오류여부
            FROM DQ_MF_LIST_RESULT{suffix}
        ) R
        ON A.기준년월일 = R.기준년월일
        AND A.서버코드 = R.서버코드
        AND A.테이블명 = R.테이블명
        AND A.컬럼명 = R.컬럼명
        WHERE A.기준년월일=%s
          AND A.테이블명=%s
        GROUP BY A.컬럼명
        ORDER BY error_rate DESC
    """

    cur.execute(sql, (selected_date, table_name))
    columns = cur.fetchall()

    return render_template(
        "detail.html",
        table_name=table_name,
        selected_date=selected_date,
        selected_type=selected_type,
        mode=mode,
        regular_base=regular_base,
        columns=columns
    )








# ===================== 3) Detail ( /detail/(drillDQwn)column_detail ) ===================== #
@app.route("/detail/<mode>/drilldown", methods=["POST"])
def detail_drilldown(mode):
    data = request.get_json()
    table_name = data["table"]
    column_name = data["column"]
    target_date = data["date"]

    suffix = "" if mode == "regular" else "_OCCA"

    conn = get_connection()
    cur = conn.cursor()

    sql = f"""
        SELECT error_type, sample_value, COUNT(*) AS cnt
        FROM (
            SELECT 'INST' AS error_type, 인스턴스코드검증값 AS sample_value
            FROM DQ_MF_INST_RESULT{suffix}
            WHERE 기준년월일=%s AND 테이블명=%s AND 컬럼명=%s AND 오류여부='Y'

            UNION ALL
            SELECT 'DATE', 년월일검증값
            FROM DQ_MF_DATE_RESULT{suffix}
            WHERE 기준년월일=%s AND 테이블명=%s AND 컬럼명=%s AND 오류여부='Y'

            UNION ALL
            SELECT 'LIST', 인스턴스코드검증값
            FROM DQ_MF_LIST_RESULT{suffix}
            WHERE 기준년월일=%s AND 테이블명=%s AND 컬럼명=%s AND 오류여부='Y'
        ) T
        GROUP BY error_type, sample_value
        ORDER BY cnt DESC
    """

    cur.execute(sql, [
        target_date, table_name, column_name,
        target_date, table_name, column_name,
        target_date, table_name, column_name
    ])
    result = cur.fetchall()

    if not result:
        result = [{"error_type": "-", "sample_value": "오류 없음", "cnt": 0}]

    return jsonify(result)




# ===================== 공통 context (사이드바 등에서 사용) ===================== #
@app.context_processor
def inject_tables():
    """
    layout.html 에서 app_code 별로 grouped_tables 를 쓰고 싶을 때 사용
    """
    grouped = defaultdict(list)
    for t in sample_tables:
        grouped[t["app_code"]].append(t)
    return dict(grouped_tables=grouped)


if __name__ == "__main__":
    # 개발용 실행
    app.run(host="0.0.0.0", port=8000, debug=True)
