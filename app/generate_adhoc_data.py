import sys
import os
import pymysql

# Add app directory to path to use db connection
sys.path.append('/app')
from services.db import get_connection

def generate_data():
    conn = get_connection()
    cur = conn.cursor()

    # 1st Half (Already done, but can keep or comment out)
    # reg_base = '20250301'
    # target_dates = ['20250610', '20250710', '20250810']
    # degree = '상반기'

    # 2nd Half
    reg_base = '20250901'
    target_dates = ['20251010', '20251110', '20251210']
    degree = '하반기'

    try:
        for occa_date in target_dates:
            print(f"Generating data for {occa_date}...")

            # 1. DQ_BASE_DATE_INFO Insert (Required for dashboard to pick it up)
            # Check if exists first
            cur.execute("SELECT 1 FROM DQ_BASE_DATE_INFO WHERE 기준년월일=%s", (occa_date,))
            if not cur.fetchone():
                print(f"  Inserting into DQ_BASE_DATE_INFO for {occa_date}")
                cur.execute("""
                    INSERT INTO DQ_BASE_DATE_INFO 
                    (기준년월일, 검증차수, 검증구분, 정기검증기준년월일, 작업년월일)
                    VALUES (%s, %s, '수시', %s, %s)
                """, (occa_date, degree, reg_base, occa_date))
            else:
                print(f"  DQ_BASE_DATE_INFO already has {occa_date}")

            # 2. DQ_MF_ASSERTION_LIST_OCCA Insert
            # Deleting existing to allow re-run
            cur.execute("DELETE FROM DQ_MF_ASSERTION_LIST_OCCA WHERE 기준년월일=%s", (occa_date,))
            
            print(f"  Inserting into DQ_MF_ASSERTION_LIST_OCCA...")
            cur.execute("""
                INSERT INTO `DQ_MF_ASSERTION_LIST_OCCA` (
                  `그룹회사코드`, `기준년월일`, `작업자수`, `서버코드`, `테이블명`, `컬럼명`, `어플리케이션코드`,
                  `작업년월일`, `작업구분`, `인스턴스식별자`, `작업그룹번호`, `제외여부`
                )
                SELECT
                    A.`그룹회사코드`,
                    %s,     -- occa_date
                    A.`작업자수`,
                    A.`서버코드`,
                    A.`테이블명`,
                    A.`컬럼명`,
                    A.`어플리케이션코드`,
                    %s,     -- work_date (same as occa)
                    A.`작업구분`,
                    A.`인스턴스식별자`,
                    A.`작업그룹번호`,
                    'N' AS `제외여부`
                FROM `DQ_MF_ASSERTION_LIST` A
                LEFT JOIN `DQ_MF_INST_RESULT` IR 
                       ON IR.그룹회사코드 = A.그룹회사코드 AND IR.기준년월일 = A.기준년월일 AND IR.테이블명 = A.테이블명 AND IR.컬럼명 = A.컬럼명
                LEFT JOIN `DQ_MF_DATE_RESULT` DR
                       ON DR.그룹회사코드 = A.그룹회사코드 AND DR.기준년월일 = A.기준년월일 AND DR.테이블명 = A.테이블명 AND DR.컬럼명 = A.컬럼명
                LEFT JOIN `DQ_MF_LIST_RESULT` LR
                       ON LR.그룹회사코드 = A.그룹회사코드 AND LR.기준년월일 = A.기준년월일 AND LR.테이블명 = A.테이블명 AND LR.컬럼명 = A.컬럼명
                WHERE A.`기준년월일` = %s
                  AND (IR.오류여부 = 'Y' OR DR.오류여부 = 'Y' OR LR.오류여부 = 'Y')
                GROUP BY A.`그룹회사코드`, A.`기준년월일`, A.`서버코드`, A.`테이블명`, A.`컬럼명`
            """, (occa_date, occa_date, reg_base))

            # 3. DQ_MF_INST_RESULT_OCCA
            cur.execute("DELETE FROM DQ_MF_INST_RESULT_OCCA WHERE 기준년월일=%s", (occa_date,))
            print(f"  Inserting into DQ_MF_INST_RESULT_OCCA...")
            cur.execute("""
                INSERT INTO `DQ_MF_INST_RESULT_OCCA` (
                    `그룹회사코드`, `기준년월일`, `작업자수`, `서버코드`, `테이블명`, `컬럼명`,
                    `인스턴스코드검증값`, `인스턴스코드별집계수`, `오류여부`, `인스턴스코드식별자`,
                    `PK여부`, `구간데이터여부`, `작업년월일`, `시스템최종처리일시`
                )
                SELECT
                    IR.`그룹회사코드`,
                    %s, -- occa_date
                    IR.`작업자수`,
                    IR.`서버코드`,
                    IR.`테이블명`,
                    IR.`컬럼명`,
                    IR.`인스턴스코드검증값`,
                    IR.`인스턴스코드별집계수`,
                    CASE WHEN RAND() < 0.5 THEN 'Y' ELSE 'N' END,
                    IR.`인스턴스코드식별자`,
                    IR.`PK여부`,
                    IR.`구간데이터여부`,
                    %s, -- occa_date
                    CONCAT(%s, ' 13:00:00')
                FROM `DQ_MF_INST_RESULT` IR
                INNER JOIN (SELECT * FROM `DQ_MF_ASSERTION_LIST_OCCA` WHERE 기준년월일 = %s) AO
                 ON AO.`테이블명` = IR.`테이블명` AND AO.`컬럼명` = IR.`컬럼명` AND AO.`작업구분` = '1'
                WHERE IR.`기준년월일` = %s AND IR.`오류여부` = 'Y'
            """, (occa_date, occa_date, occa_date, occa_date, reg_base))

            # 4. DQ_MF_LIST_RESULT_OCCA
            cur.execute("DELETE FROM DQ_MF_LIST_RESULT_OCCA WHERE 기준년월일=%s", (occa_date,))
            print(f"  Inserting into DQ_MF_LIST_RESULT_OCCA...")
            cur.execute("""
                INSERT INTO `DQ_MF_LIST_RESULT_OCCA` (
                    `그룹회사코드`, `기준년월일`, `작업자수`, `서버코드`, `테이블명`, `컬럼명`,
                    `인스턴스코드검증값`, `인스턴스코드별집계수`, `인스턴스코드식별자`, `오류여부`,
                    `PK여부`, `구간데이터여부`, `작업년월일`, `시스템최종처리일시`
                )
                SELECT
                    LR.`그룹회사코드`,
                    %s,
                    LR.`작업자수`,
                    LR.`서버코드`,
                    LR.`테이블명`,
                    LR.`컬럼명`,
                    LR.`인스턴스코드검증값`,
                    LR.`인스턴스코드별집계수`,
                    LR.`인스턴스코드식별자`,
                    CASE WHEN RAND() < 0.5 THEN 'Y' ELSE 'N' END,
                    LR.`PK여부`,
                    LR.`구간데이터여부`,
                    %s,
                    CONCAT(%s, ' 15:00:00')
                FROM `DQ_MF_LIST_RESULT` LR
                JOIN (SELECT * FROM `DQ_MF_ASSERTION_LIST_OCCA` WHERE 기준년월일 = %s) AO
                  ON AO.`그룹회사코드` = LR.`그룹회사코드` AND AO.`테이블명` = LR.`테이블명` AND AO.`컬럼명` = LR.`컬럼명` AND AO.`작업구분` = '3'
                WHERE LR.`기준년월일` = %s AND LR.`오류여부` = 'Y'
            """, (occa_date, occa_date, occa_date, occa_date, reg_base))

            # 5. DQ_MF_DATE_RESULT_OCCA
            cur.execute("DELETE FROM DQ_MF_DATE_RESULT_OCCA WHERE 기준년월일=%s", (occa_date,))
            print(f"  Inserting into DQ_MF_DATE_RESULT_OCCA...")
            cur.execute("""
                INSERT INTO `DQ_MF_DATE_RESULT_OCCA` (
                    `그룹회사코드`, `기준년월일`, `작업자수`, `서버코드`, `테이블명`, `컬럼명`,
                    `년월일검증값`, `년월일별집계수`, `오류여부`, `PK여부`, `구간데이터여부`,
                    `작업년월일`, `시스템최종처리일시`
                )
                SELECT
                    DR.`그룹회사코드`,
                    %s,
                    DR.`작업자수`,
                    DR.`서버코드`,
                    DR.`테이블명`,
                    DR.`컬럼명`,
                    DR.`년월일검증값`,
                    DR.`년월일별집계수`,
                    CASE WHEN RAND() < 0.5 THEN 'Y' ELSE 'N' END,
                    DR.`PK여부`,
                    DR.`구간데이터여부`,
                    %s,
                    CONCAT(%s, ' 14:00:00')
                FROM `DQ_MF_DATE_RESULT` DR
                JOIN (SELECT * FROM `DQ_MF_ASSERTION_LIST_OCCA` WHERE 기준년월일 = %s) AO
                  ON AO.`그룹회사코드` = DR.`그룹회사코드` AND AO.`테이블명` = DR.`테이블명` AND AO.`컬럼명` = DR.`컬럼명` AND AO.`작업구분` = '2'
                WHERE DR.`기준년월일` = %s AND DR.`오류여부` = 'Y'
            """, (occa_date, occa_date, occa_date, occa_date, reg_base))

        conn.commit()
        print("Done!")
    except Exception as e:
        conn.rollback()
        print(f"Error: {e}")
        raise
    finally:
        conn.close()

if __name__ == "__main__":
    generate_data()
