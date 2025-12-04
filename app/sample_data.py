sample_tables = [
    {"app_code": "FIN", "table_name": "TSFIN0001", "error_cnt": 120, "normal_cnt": 880, "error_rate": 12.0},
    {"app_code": "FIN", "table_name": "TSFIN0002", "error_cnt": 82, "normal_cnt": 700, "error_rate": 10.5},
    {"app_code": "FIN", "table_name": "TSFIN0003", "error_cnt": 60, "normal_cnt": 900, "error_rate": 6.2},
    {"app_code": "FIN", "table_name": "TSFIN1000", "error_cnt": 0, "normal_cnt": 1000, "error_rate": 0.0},   # 정상테이블

    {"app_code": "CRM", "table_name": "TSCRM12A1", "error_cnt": 150, "normal_cnt": 780, "error_rate": 16.1},
    {"app_code": "CRM", "table_name": "TSCRM98B2", "error_cnt": 45, "normal_cnt": 850, "error_rate": 5.0},
    {"app_code": "CRM", "table_name": "TSCRM77A9", "error_cnt": 32, "normal_cnt": 900, "error_rate": 3.4},
    {"app_code": "CRM", "table_name": "TSCRM1000", "error_cnt": 0, "normal_cnt": 1200, "error_rate": 0.0},  # 정상테이블

    {"app_code": "INS", "table_name": "TSINSQW12", "error_cnt": 71, "normal_cnt": 690, "error_rate": 9.3},
    {"app_code": "INS", "table_name": "TSINSLK88", "error_cnt": 21, "normal_cnt": 960, "error_rate": 2.1},
    {"app_code": "INS", "table_name": "TSINSPL14", "error_cnt": 12, "normal_cnt": 995, "error_rate": 1.1},
    {"app_code": "INS", "table_name": "TSINS1000", "error_cnt": 0, "normal_cnt": 1500, "error_rate": 0.0},  # 정상테이블
]

sample_columns = {
    "TSFIN0001": [
        {"column": "CUST_ID", "error_cnt": 15, "normal_cnt": 885, "error_rate": 1.6},
        {"column": "AGE", "error_cnt": 5, "normal_cnt": 895, "error_rate": 0.5},
        {"column": "JOIN_DT", "error_cnt": 4, "normal_cnt": 896, "error_rate": 0.4},
    ],
    "TSFIN0002": [
        {"column": "CUST_ID", "error_cnt": 12, "normal_cnt": 770, "error_rate": 1.5},
        {"column": "CREDIT_SCORE", "error_cnt": 8, "normal_cnt": 782, "error_rate": 0.9},
        {"column": "LIMIT_AMT", "error_cnt": 2, "normal_cnt": 788, "error_rate": 0.2},
    ],
    "TSFIN0003": [
        {"column": "BRANCH_CODE", "error_cnt": 6, "normal_cnt": 894, "error_rate": 0.6},
        {"column": "MGR_ID", "error_cnt": 10, "normal_cnt": 890, "error_rate": 1.1},
    ],
    "TSFIN1000": [   # 정상 컬럼만
        {"column": "CUST_ID", "error_cnt": 0, "normal_cnt": 1000, "error_rate": 0.0},
        {"column": "ACCT_AMT", "error_cnt": 0, "normal_cnt": 1000, "error_rate": 0.0},
    ],

    "TSCRM12A1": [
        {"column": "CUST_ID", "error_cnt": 5, "normal_cnt": 775, "error_rate": 0.6},
        {"column": "EVENT_CD", "error_cnt": 3, "normal_cnt": 777, "error_rate": 0.4},
    ],
    "TSCRM98B2": [
        {"column": "SALES_AMT", "error_cnt": 9, "normal_cnt": 841, "error_rate": 1.0},
        {"column": "REG_DT", "error_cnt": 2, "normal_cnt": 848, "error_rate": 0.2},
    ],
    "TSCRM77A9": [
        {"column": "REGION_CD", "error_cnt": 2, "normal_cnt": 898, "error_rate": 0.2},
        {"column": "OFFICE_ID", "error_cnt": 1, "normal_cnt": 899, "error_rate": 0.1},
    ],
    "TSCRM1000": [   # 정상 컬럼만
        {"column": "CUSTOMER_SEG", "error_cnt": 0, "normal_cnt": 1200, "error_rate": 0.0},
        {"column": "GRADE", "error_cnt": 0, "normal_cnt": 1200, "error_rate": 0.0},
    ],

    "TSINSQW12": [
        {"column": "ACCNT_ID", "error_cnt": 4, "normal_cnt": 686, "error_rate": 0.6},
        {"column": "INS_AMT", "error_cnt": 1, "normal_cnt": 689, "error_rate": 0.1},
    ],
    "TSINSLK88": [
        {"column": "POLICY_ID", "error_cnt": 11, "normal_cnt": 949, "error_rate": 1.1},
        {"column": "SIGN_DT", "error_cnt": 7, "normal_cnt": 953, "error_rate": 0.7},
    ],
    "TSINSPL14": [
        {"column": "CUST_ID", "error_cnt": 3, "normal_cnt": 992, "error_rate": 0.3},
        {"column": "INS_CAT", "error_cnt": 4, "normal_cnt": 991, "error_rate": 0.4},
    ],
    "TSINS1000": [   # 정상 컬럼만
        {"column": "ACCNT_ID", "error_cnt": 0, "normal_cnt": 1500, "error_rate": 0.0},
        {"column": "INS_AMT", "error_cnt": 0, "normal_cnt": 1500, "error_rate": 0.0},
    ],
}



sample_column_detail = {
    "TSFIN0001": {
        "CUST_ID": [
            {"error_type": "Null", "sample_value": "NULL", "cnt": 15},
            {"error_type": "Invalid Format", "sample_value": "12A####", "cnt": 7},
        ],
        "AGE": [
            {"error_type": "Out of Range", "sample_value": "-3", "cnt": 3},
            {"error_type": "Out of Range", "sample_value": "999", "cnt": 2},
        ],
        "JOIN_DT": [
            {"error_type": "Invalid Date", "sample_value": "20250230", "cnt": 4},
        ]
    },

    "TSFIN0002": {
        "CUST_ID": [
            {"error_type": "Null", "sample_value": "NULL", "cnt": 12}
        ],
        "CREDIT_SCORE": [
            {"error_type": "Out of Range", "sample_value": "1200", "cnt": 8}
        ],
        "LIMIT_AMT": [
            {"error_type": "Negative", "sample_value": "-50000", "cnt": 2}
        ]
    },

    "TSFIN0003": {
        "BRANCH_CODE": [
            {"error_type": "Invalid Format", "sample_value": "ZZZ99", "cnt": 6}
        ],
        "MGR_ID": [
            {"error_type": "Null", "sample_value": "NULL", "cnt": 10}
        ]
    },

    "TSCRM12A1": {
        "CUST_ID": [
            {"error_type": "Null", "sample_value": "NULL", "cnt": 5}
        ],
        "EVENT_CD": [
            {"error_type": "Invalid Code", "sample_value": "EVT999", "cnt": 3}
        ]
    },

    "TSCRM98B2": {
        "SALES_AMT": [
            {"error_type": "Negative", "sample_value": "-150000", "cnt": 9}
        ],
        "REG_DT": [
            {"error_type": "Invalid Date", "sample_value": "20240231", "cnt": 2}
        ]
    },

    "TSCRM77A9": {
        "REGION_CD": [
            {"error_type": "Invalid Code", "sample_value": "KR_!!", "cnt": 2}
        ],
        "OFFICE_ID": [
            {"error_type": "Invalid Format", "sample_value": "#@!$", "cnt": 1}
        ]
    },

    "TSINSQW12": {
        "ACCNT_ID": [
            {"error_type": "Null", "sample_value": "NULL", "cnt": 4}
        ],
        "INS_AMT": [
            {"error_type": "Out of Range", "sample_value": "-30000", "cnt": 1}
        ]
    },

    "TSINSLK88": {
        "POLICY_ID": [
            {"error_type": "Duplicate", "sample_value": "POL1234", "cnt": 11}
        ],
        "SIGN_DT": [
            {"error_type": "Invalid Date", "sample_value": "99999999", "cnt": 7}
        ]
    },

    "TSINSPL14": {
        "CUST_ID": [
            {"error_type": "Null", "sample_value": "NULL", "cnt": 3}
        ],
        "INS_CAT": [
            {"error_type": "Invalid Code", "sample_value": "XX", "cnt": 4}
        ]
    },
}

