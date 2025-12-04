from flask import Flask, render_template, request
from sample_data import sample_tables, sample_columns, sample_column_detail
from collections import defaultdict

app = Flask(__name__)


# ===================== 공통 KPI 계산 함수 ===================== #
def calc_kpi(tables):
    """전체건수 / 오류 / 정상 / 오류율 계산"""
    total_error = sum(t["error_cnt"] for t in tables)
    total_normal = sum(t["normal_cnt"] for t in tables)
    total = total_error + total_normal

    if total > 0:
        error_rate = round(total_error / total * 100, 2)
    else:
        error_rate = 0.0

    return {
        "total_cnt": f"{total:,}",
        "error_cnt": f"{total_error:,}",
        "normal_cnt": f"{total_normal:,}",
        "error_rate": error_rate,
    }


# ===================== 1) Dashboard ( / ) ===================== #
@app.route("/")
def dashboard():
    """
    Dashboard KPIs 확장

    - 전체 기준 KPI (기존): 전체건수 / 오류건수 / 정상건수 / 오류율
    - 테이블 기준 KPI
        * 오류 테이블 수: 해당 테이블의 '컬럼들 중' 하나라도 error_cnt > 0 이면 오류 테이블
        * 정상 테이블 수: 검증된 컬럼이 존재하고, 모든 컬럼이 error_cnt == 0 인 테이블
        * 검증 테이블 수: 오류 + 정상 테이블 (즉, sample_columns에 컬럼 정보가 있는 테이블)
    - 컬럼 기준 KPI
        * 오류 컬럼 수: error_cnt > 0
        * 정상 컬럼 수: error_cnt == 0
        * 검증 컬럼 수: 오류 + 정상
    """

    # -------------------- 1) 기존 전체 KPI (테이블 집계) --------------------
    kpi = calc_kpi(sample_tables)

    # -------------------- 2) 테이블 기준 KPI (컬럼 기준으로 재계산) --------------------
    error_tables = 0
    normal_tables = 0
    verified_tables = 0  # 오류 + 정상 테이블

    for t in sample_tables:
        table_name = t["table_name"]
        cols = sample_columns.get(table_name, [])

        # 컬럼 정보가 아예 없으면 "검증 대상 아님"으로 보고 건너뜀
        if not cols:
            continue

        verified_tables += 1  # 검증테이블수: 컬럼이 하나라도 있는 테이블

        # 이 테이블의 컬럼 중 하나라도 error_cnt > 0 이면 오류테이블
        has_error_col = any(col["error_cnt"] > 0 for col in cols)

        if has_error_col:
            error_tables += 1
        else:
            # 컬럼은 있는데, 모든 컬럼이 error_cnt == 0 → 정상 테이블
            normal_tables += 1

    table_kpi = {
        "error": error_tables,
        "normal": normal_tables,
        "verified": verified_tables,
    }

    # -------------------- 3) 컬럼 기준 KPI --------------------
    error_columns = 0
    normal_columns = 0

    for tbl_name, cols in sample_columns.items():
        for col in cols:
            if col["error_cnt"] > 0:
                error_columns += 1
            else:
                normal_columns += 1

    verified_columns = error_columns + normal_columns  # 오류 + 정상

    column_kpi = {
        "error": error_columns,
        "normal": normal_columns,
        "verified": verified_columns,
    }

    # -------------------- 4) 템플릿 렌더링 --------------------
    return render_template(
        "dashboard.html",
        kpi=kpi,
        table_kpi=table_kpi,
        column_kpi=column_kpi,
        table_stats=sample_tables,
    )


# ===================== Trend ( /trend ) - 준비용 ===================== #
@app.route("/trend")
def trend():
    return render_template("trend.html")


# ===================== 2) Tables ( /tables ) ===================== #
@app.route("/tables")
def tables_view():
    """
    - 전체 테이블 목록
    - DataTables로 검색/정렬/페이징
    - app=FIN/CRM/INS 쿼리 파라미터로 필터링
      예) /tables?app=FIN
    """
    app_code = request.args.get("app")  # FIN / CRM / INS / None

    if app_code:
        filtered = [t for t in sample_tables if t["app_code"] == app_code]
    else:
        filtered = sample_tables

    return render_template(
        "tables.html",
        tables=filtered,
        selected_app=app_code or "",
        sample_columns=sample_columns      # 🔥 반드시 추가
    )

# ===================== 3) Detail ( /detail/<table_name> ) ===================== #
@app.route("/detail/<table_name>")
def table_detail(table_name):
    """
    - 좌측: 컬럼 요약 (error_cnt / error_rate)
    - 우측: 컬럼 클릭 시 Drill-down
      → 상세 오류타입 / 샘플값 / 개수 표시
    """
    # sample_columns: { "TSFIN0001": [ {column, error_cnt, ...}, ... ], ... }
    columns = sample_columns.get(table_name, [])

    # detail.html 에서 detailData[tableName][column] 으로 접근하므로
    # {"TSFIN0001": {...}} 형태로 한 번 감싸서 내려준다.
    detail_data = {
        table_name: sample_column_detail.get(table_name, {})
    }

    return render_template(
        "detail.html",
        table_name=table_name,
        columns=columns,
        detail_data=detail_data,
    )


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
