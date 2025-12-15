import sys
import os

# Ensure /app is in path
sys.path.append('/app')

from services.db import get_connection

def inspect_data():
    conn = get_connection()
    cur = conn.cursor()
    
    print("--- DQ_MAINT_PLAN_TABLE (Dates) ---")
    cur.execute("SELECT DISTINCT base_date FROM DQ_MAINT_PLAN_TABLE ORDER BY base_date DESC")
    rows = cur.fetchall()
    for r in rows:
        print(r)
    
    print("--- DQ_BASE_DATE_INFO (Linked to 20250901) ---")
    cur.execute("SELECT 기준년월일, 검증구분 FROM DQ_BASE_DATE_INFO WHERE 정기검증기준년월일='20250901' OR 기준년월일='20250901' ORDER BY 기준년월일")
    for r in cur.fetchall():
        print(r)
        
    conn.close()

if __name__ == "__main__":
    inspect_data()
