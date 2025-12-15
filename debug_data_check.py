import sys
import os

# App context hack
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), 'app')))

from services.db import get_connection

def inspect_data():
    conn = get_connection()
    cur = conn.cursor()
    
    print("--- DQ_BASE_DATE_INFO (All Rows) ---")
    cur.execute("SELECT * FROM DQ_BASE_DATE_INFO")
    rows = cur.fetchall()
    for r in rows:
        print(r)
        
    conn.close()

if __name__ == "__main__":
    inspect_data()
