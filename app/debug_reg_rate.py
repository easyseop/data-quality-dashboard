import sys
sys.path.append('/app')
from services.db import get_connection
from utils.filter_base import compute_reg_rate

def debug_reg_rate():
    base_date = '20250901'
    print(f"--- Debugging Registry Rate for {base_date} ---")
    
    # 1. Check raw table data count
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("SELECT count(*) as cnt FROM DQ_MAINT_PLAN_TABLE WHERE base_date=%s", (base_date,))
    row = cur.fetchone()
    print(f"Raw Count directly from DB: {row['cnt']}")
    
    cur.execute("SELECT app_code, maint_plan_reg FROM DQ_MAINT_PLAN_TABLE WHERE base_date=%s LIMIT 5", (base_date,))
    rows = cur.fetchall()
    print(f"Sample Rows: {rows}")
    conn.close()

    # 2. Check function output
    res = compute_reg_rate(base_date)
    print(f"compute_reg_rate result: {res}")

if __name__ == "__main__":
    debug_reg_rate()
