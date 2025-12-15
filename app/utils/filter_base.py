# utils/base_filter.py
from flask import request
from services.db import get_connection

def get_sys_type(app_code):
    return "mf" if app_code in ("APP001","APP002","APP003","APP004","APP005") else "dw"

def get_result_union_sql(suffix=""):
    return f"""
        SELECT 기준년월일, 서버코드, 테이블명, 컬럼명, 오류여부
        FROM DQ_MF_INST_RESULT{suffix} WHERE 기준년월일=%s
        UNION ALL
        SELECT 기준년월일, 서버코드, 테이블명, 컬럼명, 오류여부
        FROM DQ_MF_DATE_RESULT{suffix} WHERE 기준년월일=%s
        UNION ALL
        SELECT 기준년월일, 서버코드, 테이블명, 컬럼명, 오류여부
        FROM DQ_MF_LIST_RESULT{suffix} WHERE 기준년월일=%s
    """

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

    union_sql = get_result_union_sql(table_suffix)

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
            {union_sql}
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

# ---------------------------
# 1) 오류 개선율 계산
# ---------------------------
def compute_improvement_rate(base_date):
    conn = get_connection()
    cur = conn.cursor()

    # --- 1) 정기 오류 컬럼 조회 ---
    union_sql = get_result_union_sql("")
    cur.execute(f"""
        SELECT A.어플리케이션코드 AS app_code, A.테이블명, A.컬럼명
        FROM DQ_MF_ASSERTION_LIST A
        JOIN (
            {union_sql}
        ) R
        ON A.기준년월일 = R.기준년월일
        AND A.테이블명 = R.테이블명
        AND A.컬럼명 = R.컬럼명
        WHERE A.기준년월일=%s AND R.오류여부='Y'
    """, (base_date, base_date, base_date, base_date))
    regular_errors = cur.fetchall()

    # --- 정기 오류 목록 시스템별 count ---
    stats = {
        "mf": {"total":0, "improved":0},
        "dw": {"total":0, "improved":0},
        "total": {"total":0, "improved":0},
    }

    # 정기 오류 total count
    for row in regular_errors:
        s = get_sys_type(row["app_code"])
        stats[s]["total"] += 1
        stats["total"]["total"] += 1

    # --- 2) 최신 수시 기준일 찾기 ---
    cur.execute("""
        SELECT MAX(기준년월일) AS latest
        FROM DQ_BASE_DATE_INFO
        WHERE 정기검증기준년월일 = %s
    """, (base_date,))
    latest = cur.fetchone()["latest"]

    if not latest:
        return {
            "mf":{"rate":0},
            "dw":{"rate":0},
            "total":{"rate":0}
        }

    # --- 3) 최신 수시 오류 조회 ---
    union_sql_occa = get_result_union_sql("_OCCA")
    cur.execute(f"""
        SELECT A.어플리케이션코드 AS app_code, A.테이블명, A.컬럼명, R.오류여부
        FROM DQ_MF_ASSERTION_LIST_OCCA A
        JOIN (
            {union_sql_occa}
        ) R
        ON A.기준년월일 = R.기준년월일
        AND A.테이블명 = R.테이블명
        AND A.컬럼명 = R.컬럼명
        WHERE A.기준년월일=%s
    """, (latest, latest, latest, latest))
    latest_rows = cur.fetchall()

    occa_map = {
        (r["테이블명"], r["컬럼명"]): r["오류여부"]
        for r in latest_rows
    }

    # --- 4) 개선 여부 계산 ---
    for row in regular_errors:
        key = (row["테이블명"], row["컬럼명"])
        s = get_sys_type(row["app_code"])

        if key in occa_map and occa_map[key] == "N":
            stats[s]["improved"] += 1
            stats["total"]["improved"] += 1

    # --- 5) rate 계산 ---
    result = {}
    for s in ("mf","dw","total"):
        total = stats[s]["total"]
        improved = stats[s]["improved"]
        rate = round(improved / total * 100, 2) if total else 0
        result[s] = {
            "rate": rate,
            "improved": improved,
            "total": total
        }

    result["latest_occa"] = latest or None
    return result

def compute_reg_rate(base_date):
    conn = get_connection()
    cur = conn.cursor()

    cur.execute("""
        SELECT app_code, maint_plan_reg
        FROM DQ_MAINT_PLAN_TABLE
        WHERE base_date=%s
    """, (base_date,))
    rows = cur.fetchall()

    stats = {
        "mf": {"total":0, "reg":0},
        "dw": {"total":0, "reg":0},
        "total": {"total":0, "reg":0},
    }

    for r in rows:
        s = get_sys_type(r["app_code"])
        stats[s]["total"] += 1
        stats["total"]["total"] += 1

        if r["maint_plan_reg"] == "Y":
            stats[s]["reg"] += 1
            stats["total"]["reg"] += 1

    result = {}
    for s in ("mf","dw","total"):
        total = stats[s]["total"]
        reg = stats[s]["reg"]
        
        rate = round(reg / total * 100, 2) if total else 0
        
        result[s] = rate
        result[f"{s}_reg_cnt"] = reg
        result[f"{s}_total_cnt"] = total

    return result



def compute_quality_kpi(improve_rate, reg_rate):
    return {
        s: round(
                improve_rate[s]["rate"] * 0.5 +
                reg_rate[s] * 0.5,
                2
            )
        for s in ("mf","dw","total")
    }

def compute_kpi_trend(base_date):
    """
    정기 기준일(base_date)부터 시작하여, 해당 정기 차수에 속하는 수시 차수들의 날짜별 KPI 추이를 계산함.
    KPI = (오류개선율 * 0.5) + (정비계획등록률 * 0.5)
    * 변경사항: 정비계획등록률 날짜별 동적 조회 (데이터 없으면 정기 기준일 값 fallback)
    * 변경사항: Total 뿐만 아니라 MF, DW 각각의 KPI도 계산
    """
    conn = get_connection()
    cur = conn.cursor()

    # 1. 정비계획 등록률 (Baseline - Fallback용)
    reg_rate_base_map = compute_reg_rate(base_date)

    # 2. 날짜 목록 조회 (정기 + 수시)
    cur.execute("""
        SELECT 기준년월일
        FROM DQ_BASE_DATE_INFO
        WHERE 정기검증기준년월일 = %s
           OR 기준년월일 = %s  -- 본인(정기)도 포함
        ORDER BY 기준년월일 ASC
    """, (base_date, base_date))
    rows = cur.fetchall()
    
    date_list = sorted(list(set([r["기준년월일"] for r in rows])))

    # 3. 정기 기준일의 오류 목록 (Baseline) 조회 (+ System Type 구분)
    #    AppCode를 알기 위해 ASSERTION_LIST와 조인 필요
    union_sql = get_result_union_sql("")
    cur.execute(f"""
        SELECT A.어플리케이션코드 as app_code, A.테이블명, A.컬럼명
        FROM DQ_MF_ASSERTION_LIST A
        JOIN (
            {union_sql}
        ) R
        ON A.기준년월일 = R.기준년월일
        AND A.테이블명 = R.테이블명
        AND A.컬럼명 = R.컬럼명
        WHERE A.기준년월일=%s AND R.오류여부='Y'
    """, (base_date, base_date, base_date, base_date))
    
    baseline_rows = cur.fetchall()
    
    # 시스템별 Baseline 오류 개수 및 Key 집합
    baseline_info = {
        "mf": {"count": 0, "keys": set()},
        "dw": {"count": 0, "keys": set()},
        "total": {"count": 0, "keys": set()}
    }

    for r in baseline_rows:
        key = (r["테이블명"], r["컬럼명"])
        sType = get_sys_type(r["app_code"])
        
        baseline_info[sType]["count"] += 1
        baseline_info[sType]["keys"].add(key)
        
        baseline_info["total"]["count"] += 1
        baseline_info["total"]["keys"].add(key)

    trend_result = []

    for d in date_list:
        # --- A. 정비계획 등록률 (Dynamic + Fallback) ---
        reg_map_curr = compute_reg_rate(d)
        
        # 데이터가 아예 없으면(total count=0) 정기 기준일 값(reg_rate_base_map) 사용
        # compute_reg_rate 리턴 포맷: { "mf": rate, "dw": rate, "total": rate } (값은 float)
        # 하지만 내부적으로 stats["total"]["total"] == 0 인지 확인하려면 compute_reg_rate 구조상 
        # 결과값 0이 진짜 0인지 데이터 없음인지 모호할 수 있음.
        # 편의상 등록률이 0이고 해당 날짜에 MAINT 테이블 데이터가 없는 경우를 Fallback으로 간주해야 하나,
        # 여기서는 간단히 쿼리 결과 검증 대신, 외부에서직접 확인하거나 단순 가정 사용.
        # *개선*: compute_reg_rate가 단순 rate만 주므로, 여기서는 "값이 0이면 혹시 데이터가 없는건가?" 의심 가능.
        # 확실한 처리를 위해 check query를 날리거나, compute_reg_rate 로직을 믿음.
        # Fallback 정책: 
        # "해당 일자의 정비계획 테이블 데이터가 존재하지 않으면 Fallback" -> 이게 정확함.
        # 성능상 매번 count하기보다, reg_map_curr가 0이면 fallback? (위험: 진짜 0%일수도)
        # -> 일단 그대로 쓰고, 값이 너무 튀면 Fallback하는 로직보다는
        #    Debug시 데이터가 없었으므로, 여기서는 "해당 일자 데이터 존재 여부"를 체크하는게 맞음.
        
        cur.execute("SELECT 1 FROM DQ_MAINT_PLAN_TABLE WHERE base_date=%s LIMIT 1", (d,))
        has_maint_data = cur.fetchone()

        if has_maint_data:
            current_reg_rates = reg_map_curr
        else:
            current_reg_rates = reg_rate_base_map

        # --- B. 오류 개선율 계산 ---
        # 개선수 카운트
        improved_counts = {"mf": 0, "dw": 0, "total": 0}

        if d == base_date:
            # 첫날은 개선율 0
            pass
        else:
            if baseline_info["total"]["count"] > 0:
                # 수시 오류 현황 조회
                union_sql_occa = get_result_union_sql("_OCCA")
                cur.execute(f"""
                    SELECT 테이블명, 컬럼명, 오류여부
                    FROM (
                        {union_sql_occa}
                    ) T
                """, (d, d, d))
                occa_rows = cur.fetchall()
                occa_map = { (r["테이블명"], r["컬럼명"]): r["오류여부"] for r in occa_rows }

                # 각 시스템별로 개선 여부 체크
                for sType in ["mf", "dw", "total"]: # total도 별도 key set이 있으므로 순회
                    for key in baseline_info[sType]["keys"]:
                        if key in occa_map and occa_map[key] == 'N':
                            improved_counts[sType] += 1

        # --- C. KPI 계산 (System별) ---
        row_data = { "date": d }
        
        for sType in ["mf", "dw", "total"]:
            # 개선율
            base_cnt = baseline_info[sType]["count"]
            if base_cnt > 0:
                imp_rate = (improved_counts[sType] / base_cnt) * 100
            else:
                imp_rate = 0.0
            
            # 등록률
            reg_rate = current_reg_rates[sType] # mf, dw, total 키 존재
            
            # KPI
            kpi = (imp_rate * 0.5) + (reg_rate * 0.5)
            
            # 결과 저장 (키 이름 분기)
            # total -> kpi, improve_rate, reg_rate (기존 호환성)
            # mf -> kpi_mf, improve_mf, reg_mf
            # dw -> kpi_dw, improve_dw, reg_dw
            
            suffix = "" if sType == "total" else f"_{sType}"
            
            row_data[f"kpi{suffix}"] = round(kpi, 2)
            row_data[f"improve_rate{suffix}"] = round(imp_rate, 2)
            row_data[f"reg_rate{suffix}"] = reg_rate

        trend_result.append(row_data)

    cur.close()
    conn.close()
    
    return trend_result
