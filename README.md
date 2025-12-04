
# 🧹 Data Quality Dashboard (Flask + Docker)

데이터 품질 검증 결과를 모니터링하기 위한 **웹 기반 대시보드**입니다.  

- 품질 검증 결과(오류/정상 건수, 오류율)를 테이블 단위로 조회
- 각 테이블의 컬럼별 오류 현황 및 **상세 오류 샘플 Drill-down**
- 대시보드/테이블 목록/테이블 상세 화면으로 구성된 **3단 구조 UI**
- 초기에는 `sample_data.py` 기반 샘플 데이터로 동작하며, 추후 MySQL 등 실제 DQ 결과 테이블로 교체 가능

---

## 📁 기술 스택

- **Backend**
  - Python 3.8.x
  - Flask
  - Gunicorn (Docker 실행용 WSGI 서버)

- **Frontend**
  - Bootstrap 5
  - jQuery 3
  - DataTables (검색/정렬/페이징)

- **Infra**
  - Docker
  - docker-compose

---

## 🏗 디렉토리 구조

```bash
data-quality-dashboard/
│
├── app.py                # Flask 메인 앱 (라우팅 + 화면 렌더링)
├── sample_data.py        # 샘플 데이터 정의 (테이블/컬럼/컬럼 상세)
├── requirements.txt      # Python 의존성 정의
├── Dockerfile            # 웹 애플리케이션 이미지 빌드용
├── docker-compose.yml    # web 서비스 정의 (포트, 커맨드 등)
│
└── templates/
    ├── layout.html       # 공통 레이아웃(사이드바 + 공통 JS/CSS)
    ├── dashboard.html    # '/' 대시보드 화면
    ├── tables.html       # '/tables' 테이블 목록 화면
    └── detail.html       # '/detail/<table_name>' 상세 Drill-down 화면
