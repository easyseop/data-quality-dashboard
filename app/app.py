from flask import Flask,send_file ,render_template, request, jsonify
from sample_data import sample_tables, sample_columns, sample_column_detail
from collections import defaultdict
from services.db import get_connection
import pandas as pd
from io import BytesIO

app = Flask(__name__)



# ===================== Dashboard ( / ) ===================== #
@app.route("/")
def dashboard():
    conn = get_connection()
    cur = conn.cursor()

    # ---- 기준년도 + 검증차수 + 검증구분 조회 ----
    cur.execute("""
        SELECT DISTINCT 기준년월일, 검증차수, 검증구분
        FROM DQ_BASE_DATE_INFO
        ORDER BY 기준년월일 DESC;
    """)
    raw = cur.fetchall()

    if not raw:
        conn.close()
        return "DQ_BASE_DATE_INFO 기준 정보가 없습니다."

    # 기준 데이터 가공
    date_list = [
        {
            "base": r["기준년월일"],           # 예: 20250301
            "year": r["기준년월일"][:4],      # 예: 2025
            "cycle": r["검증차수"],           # 예: 상반기 / 하반기 / 1월 ...
            "type": r["검증구분"]            # 예: 정기 / 수시
        }
        for r in raw
    ]

    # ===== 필터 리스트 생성 =====
    year_list = sorted({d["year"] for d in date_list}, reverse=True)
    dtype_list = sorted({d["type"] for d in date_list})  # 현재는 ['정기']만 있을 수 있음

    # ---- 요청 파라미터 수신 ----
    selected_year = request.args.get("year")
    selected_dtype = request.args.get("dtype")
    selected_cycle = request.args.get("cycle")

    # ---- 년도 보정 ----
    if not selected_year or selected_year not in year_list:
        selected_year = year_list[0]

    # ---- 검증구분 보정 ----
    if not selected_dtype or selected_dtype not in dtype_list:
        selected_dtype = dtype_list[0]

    # ---- 선택된 year + type 기준으로 cycle 목록 생성 ----
    cycle_list = sorted({
        d["cycle"]
        for d in date_list
        if d["year"] == selected_year and d["type"] == selected_dtype
    }, reverse = True)

    # ---- cycle 보정 ----
    if not cycle_list:
        # 이 조합(년도+검증구분)에 해당하는 데이터 자체가 없음
        selected_cycle = None
    else:
        if not selected_cycle or selected_cycle not in cycle_list:
            # URL에 이전 년도의 상반기 같은 값이 남아있으면
            # 현재 year/type에 맞는 첫 번째 값으로 보정
            selected_cycle = cycle_list[0]

    # ---- base_date 결정 ----
    selected_base = None
    if selected_cycle is not None:
        for d in date_list:
            if (
                d["year"] == selected_year
                and d["type"] == selected_dtype
                and d["cycle"] == selected_cycle
            ):
                selected_base = d["base"]
                break

    # 최종 fallback (이론상 여기 올 일 거의 없지만 방어적으로)
    if not selected_base:
        selected_base = date_list[0]["base"]

    # ---- Summary KPI (MF / DW) ----
    summary_sql = """
        SELECT
            db_type,
            (inst_err_cnt + list_err_cnt + ymd_err_cnt +
             inst_pass_cnt + list_pass_cnt + ymd_pass_cnt) AS total_cnt,
            (inst_err_cnt + list_err_cnt + ymd_err_cnt) AS error_cnt,
            (inst_pass_cnt + list_pass_cnt + ymd_pass_cnt) AS normal_cnt
        FROM DQ_SUMMARY_REPORT
        WHERE base_date=%s;
    """
    cur.execute(summary_sql, (selected_base,))
    overall_kpi = cur.fetchall()

    # ---- 품질지수 KPI ----
    quality_sql = """
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
        GROUP BY diagtype;
    """
    cur.execute(quality_sql, (selected_base, selected_base, selected_base))
    qrows = cur.fetchall()

    kpi_inst = {"verified": 0, "error": 0, "quality": 0}
    kpi_date = {"verified": 0, "error": 0, "quality": 0}
    kpi_list = {"verified": 0, "error": 0, "quality": 0}

    for r in qrows:
        v = r["verified"]
        e = r["error"]
        q = round((v - e) / v * 100, 2) if v > 0 else 0

        if r["diagtype"] == "I":
            kpi_inst = {"verified": v, "error": e, "quality": q}
        elif r["diagtype"] == "D":
            kpi_date = {"verified": v, "error": e, "quality": q}
        elif r["diagtype"] == "L":
            kpi_list = {"verified": v, "error": e, "quality": q}

    total_verified = kpi_inst["verified"] + kpi_date["verified"] + kpi_list["verified"]
    total_error = kpi_inst["error"] + kpi_date["error"] + kpi_list["error"]
    total_quality = round((total_verified - total_error) / total_verified * 100, 2) if total_verified else 0

    kpi_all = {"verified": total_verified, "error": total_error, "quality": total_quality}

    # ---- Maintenance 계획 ----
    maint_sql = """
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
        LIMIT 3;
    """
    cur.execute(maint_sql)
    maint_chart = cur.fetchall()

    conn.close()

    return render_template(
        "dashboard.html",
        year_list=year_list,
        dtype_list=dtype_list,
        cycle_list=cycle_list,
        selected_year=selected_year,
        selected_dtype=selected_dtype,
        selected_cycle=selected_cycle,
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

    # ===== 기준년월일 목록 =====
    # ===== 필터용 기준일 데이터 불러오기 =====
    cur.execute("""
        SELECT DISTINCT
            기준년월일,
            검증차수,
            검증구분
        FROM DQ_BASE_DATE_INFO
        ORDER BY 기준년월일 DESC
    """)
    raw_rows = cur.fetchall()

    # 기준데이터 가공
    date_list = [
        {
            "base": r["기준년월일"],
            "year": r["기준년월일"][:4],
            "cycle": r["검증차수"],
            "type": r["검증구분"]
        }
        for r in raw_rows
    ]

    # ===== 필터 리스트 생성 =====
    # DB에 존재하는 year만 추출
    year_list = sorted({d["year"] for d in date_list}, reverse=True)

    # DB에 존재하는 type만 추출 (정기만 있으면 정기만 표시됨)
    dtype_list = sorted({d["type"] for d in date_list})

    # 선택된 값
    selected_year = request.args.get("year", year_list[0])
    selected_dtype = request.args.get("dtype", dtype_list[0])

    # DB 존재 데이터만 cycle을 가져옴
    cycle_list = sorted({
        d["cycle"] for d in date_list
        if d["year"] == selected_year and d["type"] == selected_dtype
    }, reverse = True)

    selected_cycle = request.args.get("cycle", cycle_list[0] if cycle_list else None)



    # base_date 결정 (없는 조합이면 기본값)
    try:
        selected_base = next(
            d["base"] for d in date_list
            if d["year"] == selected_year and d["type"] == selected_dtype and d["cycle"] == selected_cycle
        )
    except StopIteration:
        selected_base = date_list[0]["base"]

    # ===== D1 / D2 / D3 대상 base 목록 선정 =====
    filtered_for_seq = sorted(
        [d["base"] for d in date_list if d["type"] == selected_dtype],
        reverse=True
    )

    d1 = selected_base
    idx = filtered_for_seq.index(selected_base)
    d2 = filtered_for_seq[idx + 1] if idx + 1 < len(filtered_for_seq) else None
    d3 = filtered_for_seq[idx + 2] if idx + 2 < len(filtered_for_seq) else None

    # ===== App 목록 =====
    cur.execute("SELECT DISTINCT 어플리케이션코드 FROM DQ_MF_ASSERTION_LIST ORDER BY 1")
    app_list = [row["어플리케이션코드"] for row in cur.fetchall()]

    # SQL 필터 적용
    app_sql = "" if selected_app == "ALL" else f"AND A.어플리케이션코드='{selected_app}'"

    ## 연속 오류 SQL ##
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
    result = cur.fetchall()
    cur.close()
    conn.close()

    # ===== seq & error type 설정 =====
    rows = []
    for r in result:
        seq = 1
        if r["d1"] == "Y" and r["d2"] == "Y":
            seq = 2
            if r["d3"] == "Y":
                seq = 3

        error_type = "신규오류" if seq == 1 and r["d2"] != "Y" else "연속오류"
        rows.append({**r, "seq": seq, "error_type": error_type})

    # 필터
    if selected_etype == "NEW":
        rows = [r for r in rows if r["error_type"] == "신규오류"]
    elif selected_etype == "SEQ":
        rows = [r for r in rows if r["error_type"] != "신규오류"]

    rows = sorted(rows, key=lambda x: (x["seq"], x["error_type"] == "신규오류"), reverse=True)

    total = len(rows)
    total_pages = (total + per_page - 1) // per_page
    rows = rows[(page - 1) * per_page : page * per_page]

    if not cycle_list:
        return render_template(
            "trend_seq.html",
            rows=[],
            year_list=year_list,
            dtype_list=dtype_list,
            cycle_list=cycle_list,
            selected_year=selected_year,
            selected_dtype=selected_dtype,
            selected_cycle=None,
            selected_app=selected_app,
            selected_etype=selected_etype,
            d1=None, d2=None, d3=None,
            page=page, total_pages=1,
            total_count=0,
            per_page=per_page,
            app_list=app_list
        )

    return render_template(
        "trend_seq.html",
        rows=rows,
        year_list=year_list,
        dtype_list=dtype_list,
        cycle_list=cycle_list,
        selected_year=selected_year,
        selected_dtype=selected_dtype,
        selected_cycle=selected_cycle,
        selected_app=selected_app,
        selected_etype=selected_etype,
        d1=d1, d2=d2, d3=d3,
        page=page, total_pages=total_pages,
        total_count=total,
        per_page=per_page,
        app_list=app_list
    )


@app.route("/owner")
def owner_view():
    page = int(request.args.get("page", 1))
    per_page = 10
    selected_app = request.args.get("app", "ALL")

    conn = get_connection()
    cur = conn.cursor()

    # 1) 기준일 + 검증차수 + 검증구분 (실제 오류 데이터가 있는 일자만)
    cur.execute("""
        SELECT DISTINCT a.기준년월일, b.검증차수, b.검증구분
        FROM DQ_MF_ASSERTION_LIST a
        JOIN DQ_BASE_DATE_INFO b
          ON a.기준년월일 = b.기준년월일
        ORDER BY a.기준년월일 DESC
    """)
    raw = cur.fetchall()

    if not raw:
        conn.close()
        return "오류 담당자 조회를 위한 기준일 정보가 없습니다."

    date_list = [
        {
            "base": r["기준년월일"],           # 20250301
            "year": r["기준년월일"][:4],      # 2025
            "cycle": r["검증차수"],           # 상반기/하반기/…(수시)
            "type": r["검증구분"]            # 정기/수시
        }
        for r in raw
    ]

    # 2) 필터 리스트 생성
    year_list = sorted({d["year"] for d in date_list}, reverse=True)

    # ---- 요청 파라미터 읽기 ----
    selected_year = request.args.get("year")
    selected_dtype = request.args.get("dtype")
    selected_cycle = request.args.get("cycle")

    # ---- 년도 보정 ----
    if not selected_year or selected_year not in year_list:
        selected_year = year_list[0]

    # ---- 검증구분 리스트 (해당 연도에 존재하는 구분만) ----
    dtype_list = sorted({d["type"] for d in date_list if d["year"] == selected_year})
    if not dtype_list:
        # 방어적: 그래도 없으면 전체에서 뽑기
        dtype_list = sorted({d["type"] for d in date_list})

    # ---- 검증구분 보정 ----
    if not selected_dtype or selected_dtype not in dtype_list:
        selected_dtype = dtype_list[0]

    # ---- 차수 리스트 (해당 연도 + 검증구분에서 실제 존재하는 값만) ----
    cycle_list = sorted({
        d["cycle"]
        for d in date_list
        if d["year"] == selected_year and d["type"] == selected_dtype
    }, reverse = True)

    # ---- 차수 보정 ----
    if not cycle_list:
        selected_cycle = None
    else:
        if not selected_cycle or selected_cycle not in cycle_list:
            # ⭐ 여기서 "25년 상반기 → 24년" 바꿀 때 상반기가 남아있으면
            #    24년에 맞는 첫 번째 차수(예: 하반기)로 자동 보정
            selected_cycle = cycle_list[0]

    # ---- 최종 기준일(base) 결정 ----
    selected_date = None
    if selected_cycle:
        for d in date_list:
            if (
                d["year"] == selected_year
                and d["type"] == selected_dtype
                and d["cycle"] == selected_cycle
            ):
                selected_date = d["base"]
                break

    if not selected_date:
        # 방어적으로 첫 행 사용
        selected_date = date_list[0]["base"]

    # 3) AppCode 목록 (선택된 기준일 기준)
    cur.execute("""
        SELECT DISTINCT 어플리케이션코드
        FROM DQ_MF_ASSERTION_LIST
        WHERE 기준년월일 = %s
        ORDER BY 어플리케이션코드
    """, (selected_date,))
    app_list = [row["어플리케이션코드"] for row in cur.fetchall()]

    # 4) 오류 APP + 담당자 조회
    app_sql = "" if selected_app == "ALL" else f"AND A.어플리케이션코드='{selected_app}'"

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
            AND A.컬럼명   = R.컬럼명
            WHERE R.오류여부='Y'
              AND R.기준년월일='{selected_date}'
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

    # 5) 페이징
    total = len(result)
    total_pages = (total + per_page - 1) // per_page
    start = (page - 1) * per_page
    end = page * per_page
    sliced = result[start:end]

    rows_view = []
    for idx, r in enumerate(sliced, start=start + 1):
        rows_view.append({
            "rownum": idx,
            **r
        })

    cur.close()
    conn.close()

    return render_template(
        "owner.html",
        rows=rows_view,
        year_list=year_list,
        dtype_list=dtype_list,
        cycle_list=cycle_list,
        selected_year=selected_year,
        selected_dtype=selected_dtype,
        selected_cycle=selected_cycle,
        selected_app=selected_app,
        app_list=app_list,
        page=page,
        total_pages=total_pages,
        selected_date=selected_date  # 필요하면 화면에 표시용
    )



# ===================== 2) Tables ( /tables ) ===================== #
@app.route("/tables")
def tables_view():
    app_code = request.args.get("app", "ALL")

    conn = get_connection()
    cur = conn.cursor()

    # ===== date base info =====
    cur.execute("""
        SELECT DISTINCT 기준년월일, 검증차수, 검증구분
        FROM DQ_BASE_DATE_INFO
        ORDER BY 기준년월일 DESC
    """)
    raw = cur.fetchall()

    date_list = [
        {
            "base": r["기준년월일"],
            "year": r["기준년월일"][:4],
            "cycle": r["검증차수"],
            "type": r["검증구분"]
        }
        for r in raw
    ]

    # ===== filter lists =====
    year_list = sorted({d["year"] for d in date_list}, reverse=True)
    dtype_list = sorted({d["type"] for d in date_list})

    selected_year = request.args.get("year")
    selected_dtype = request.args.get("dtype")
    selected_cycle = request.args.get("cycle")

    if not selected_year or selected_year not in year_list:
        selected_year = year_list[0]

    if not selected_dtype or selected_dtype not in dtype_list:
        selected_dtype = dtype_list[0]

    cycle_list = sorted({
        d["cycle"]
        for d in date_list
        if d["year"] == selected_year and d["type"] == selected_dtype
    }, reverse = True)

    if not cycle_list:
        selected_cycle = None
    else:
        if not selected_cycle or selected_cycle not in cycle_list:
            selected_cycle = cycle_list[0]

    # ===== base date 결정 =====
    selected_base = None
    for d in date_list:
        if (
            d["year"] == selected_year
            and d["type"] == selected_dtype
            and d["cycle"] == selected_cycle
        ):
            selected_base = d["base"]
            break

    if not selected_base:
        selected_base = date_list[0]["base"]   # fallback

    # ===== app code list =====
    cur.execute("""
        SELECT DISTINCT 어플리케이션코드
        FROM DQ_MF_ASSERTION_LIST
        WHERE 기준년월일 = %s
        ORDER BY 어플리케이션코드
    """, (selected_base,))
    app_code_list = [row["어플리케이션코드"] for row in cur.fetchall()]

    # ===== table summary query =====
    params = [selected_base, selected_base, selected_base, selected_base]

    summary_sql = """
    SELECT
        A.기준년월일,
        A.어플리케이션코드,
        A.테이블명,
        SUM(CASE WHEN R.오류여부 = 'Y' THEN 1 ELSE 0 END) AS error_cnt,
        SUM(CASE WHEN R.오류여부 = 'N' THEN 1 ELSE 0 END) AS normal_cnt,
        ROUND(
            SUM(CASE WHEN R.오류여부 = 'Y' THEN 1 ELSE 0 END) /
            NULLIF(SUM(CASE WHEN R.오류여부 IN ('Y','N') THEN 1 ELSE 0 END), 0) * 100,
            2
        ) AS error_rate
    FROM DQ_MF_ASSERTION_LIST A
    LEFT JOIN (
        SELECT 기준년월일, 서버코드, 테이블명, 컬럼명, 오류여부 FROM DQ_MF_INST_RESULT WHERE 기준년월일 = %s
        UNION ALL
        SELECT 기준년월일, 서버코드, 테이블명, 컬럼명, 오류여부 FROM DQ_MF_DATE_RESULT WHERE 기준년월일 = %s
        UNION ALL
        SELECT 기준년월일, 서버코드, 테이블명, 컬럼명, 오류여부 FROM DQ_MF_LIST_RESULT WHERE 기준년월일 = %s
    ) R
    ON  A.기준년월일 = R.기준년월일
    AND A.서버코드   = R.서버코드
    AND A.테이블명   = R.테이블명
    AND A.컬럼명     = R.컬럼명
    WHERE A.기준년월일 = %s
    AND A.제외여부 = 'N'
    """

    if app_code and app_code != "ALL":
        summary_sql += " AND A.어플리케이션코드 = %s"
        params.append(app_code)

    summary_sql += """
    GROUP BY A.기준년월일, A.어플리케이션코드, A.테이블명
    ORDER BY error_rate DESC;
    """

    cur.execute(summary_sql, params)
    rows = cur.fetchall()

    cur.close()
    conn.close()

    return render_template(
        "tables.html",
        tables=rows,
        date_list=date_list,
        year_list=year_list, dtype_list=dtype_list, cycle_list=cycle_list,
        selected_year=selected_year,
        selected_dtype=selected_dtype,
        selected_cycle=selected_cycle,
        selected_base=selected_base,
        app_code_list=app_code_list,
        selected_app=app_code
    )




# ===================== 3) Detail ( /detail/<table_name> ) ===================== #
@app.route("/detail/<table_name>")
def detail_view(table_name):
    target_date = request.args.get("date")

    conn = get_connection()
    cur = conn.cursor()

    # column summary SQL
    col_sql = """
        SELECT
            A.컬럼명 AS column_name,
            SUM(CASE WHEN R.오류여부='Y' THEN 1 ELSE 0 END) AS error_cnt,
            SUM(CASE WHEN R.오류여부='N' THEN 1 ELSE 0 END) AS normal_cnt,
            ROUND(
                SUM(CASE WHEN R.오류여부='Y' THEN 1 ELSE 0 END) /
                NULLIF(SUM(CASE WHEN R.오류여부 IN ('Y','N') THEN 1 ELSE 0 END),0) * 100,
                2
            ) AS error_rate
        FROM DQ_MF_ASSERTION_LIST A
        LEFT JOIN (
            SELECT 기준년월일, 서버코드, 테이블명, 컬럼명, 오류여부 FROM DQ_MF_INST_RESULT WHERE 기준년월일=%s
            UNION ALL
            SELECT 기준년월일, 서버코드, 테이블명, 컬럼명, 오류여부 FROM DQ_MF_DATE_RESULT WHERE 기준년월일=%s
            UNION ALL
            SELECT 기준년월일, 서버코드, 테이블명, 컬럼명, 오류여부 FROM DQ_MF_LIST_RESULT WHERE 기준년월일=%s
        ) R
        ON A.기준년월일 = R.기준년월일
        AND A.서버코드  = R.서버코드
        AND A.테이블명  = R.테이블명
        AND A.컬럼명    = R.컬럼명
        WHERE A.기준년월일=%s
        AND A.제외여부='N'
        AND A.테이블명=%s
        GROUP BY A.컬럼명
        ORDER BY error_rate DESC;
    """

    cur.execute(col_sql, [target_date, target_date, target_date, target_date, table_name])
    columns = cur.fetchall()
    cur.close()
    conn.close()

    return render_template("detail.html",
                           table_name=table_name,
                           selected_date=target_date,
                           columns=columns)





# ===================== 3) Detail ( /detail/(drillDQwn)column_detail ) ===================== #
@app.route("/detail/drilldown", methods=["POST"])
def detail_drillDQwn():
    data = request.get_json()
    table_name = data["table"]
    column_name = data["column"]
    target_date = data["date"]

    conn = get_connection()
    cur = conn.cursor()

    drill_sql = """
        SELECT error_type, sample_value, COUNT(*) AS cnt
        FROM (
            SELECT 'INST' AS error_type, 인스턴스코드검증값 AS sample_value
            FROM DQ_MF_INST_RESULT
            WHERE 기준년월일=%s AND 테이블명=%s AND 컬럼명=%s AND 오류여부='Y'

            UNION ALL

            SELECT 'DATE' AS error_type, 년월일검증값 AS sample_value
            FROM DQ_MF_DATE_RESULT
            WHERE 기준년월일=%s AND 테이블명=%s AND 컬럼명=%s AND 오류여부='Y'

            UNION ALL

            SELECT 'LIST' AS error_type, 인스턴스코드검증값 AS sample_value
            FROM DQ_MF_LIST_RESULT
            WHERE 기준년월일=%s AND 테이블명=%s AND 컬럼명=%s AND 오류여부='Y'
        ) T
        GROUP BY error_type, sample_value
        ORDER BY cnt DESC;
    """



    cur.execute(drill_sql, [
        target_date, table_name, column_name,
        target_date, table_name, column_name,
        target_date, table_name, column_name
    ])
    records = cur.fetchall()

    if not records:
        records = [{"sample_value": "오류값 없음", "cnt": 0}]

    cur.close()
    conn.close()

    return jsonify(records)



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
