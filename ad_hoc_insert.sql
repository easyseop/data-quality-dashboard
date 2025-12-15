-- 수시검증 DML
-- 수시검증 대상 리스트 생성
SET @row := 0;

INSERT INTO `DQ_MF_ASSERTION_LIST_OCCA` (
  `그룹회사코드`,
  `기준년월일`,
  `작업자수`,
  `서버코드`,
  `테이블명`,
  `컬럼명`,
  `어플리케이션코드`,
  `작업년월일`,
  `작업구분`,
  `인스턴스식별자`,
  `작업그룹번호`,
  `제외여부`
)
SELECT
    A.`그룹회사코드`,
    A.`기준년월일`,
    A.`작업자수`,
    A.`서버코드`,
    A.`테이블명`,
    A.`컬럼명`,
    A.`어플리케이션코드`,
    A.`작업년월일`,
    A.`작업구분`,
    A.`인스턴스식별자`,
    A.`작업그룹번호`,
    'N' AS `제외여부`
FROM `DQ_MF_ASSERTION_LIST` A
LEFT JOIN `DQ_MF_INST_RESULT` IR 
       ON IR.그룹회사코드 = A.그룹회사코드
      AND IR.기준년월일 = A.기준년월일
      AND IR.테이블명 = A.테이블명
      AND IR.컬럼명 = A.컬럼명
LEFT JOIN `DQ_MF_DATE_RESULT` DR
       ON DR.그룹회사코드 = A.그룹회사코드
      AND DR.기준년월일 = A.기준년월일
      AND DR.테이블명 = A.테이블명
      AND DR.컬럼명 = A.컬럼명
LEFT JOIN `DQ_MF_LIST_RESULT` LR
       ON LR.그룹회사코드 = A.그룹회사코드
      AND LR.기준년월일 = A.기준년월일
      AND LR.테이블명 = A.테이블명
      AND LR.컬럼명 = A.컬럼명
WHERE A.`기준년월일` = '20250301'
  AND (
        IR.오류여부 = 'Y'
     OR DR.오류여부 = 'Y'
     OR LR.오류여부 = 'Y'
  )
GROUP BY
    A.`그룹회사코드`, A.`기준년월일`, A.`서버코드`,
    A.`테이블명`, A.`컬럼명`;


# 수시검증 날짜를 임의로 변경 여기서 6,7,8월 추가 필요 
UPDATE `DQ_MF_ASSERTION_LIST_OCCA`
SET 기준년월일 = '20250510'
WHERE 기준년월일 = '20250301';

-- 정기검증 기준일
SET @reg_base  := '20250301';

-- 수시검증 기준일 (정기와 동일하게 쓸 거면 같은 값, 다르게 쓸 거면 다른 날짜)
SET @occa_base := '20250510';


INSERT INTO `DQ_MF_INST_RESULT_OCCA` (
    `그룹회사코드`,
    `기준년월일`,
    `작업자수`,
    `서버코드`,
    `테이블명`,
    `컬럼명`,
    `인스턴스코드검증값`,
    `인스턴스코드별집계수`,
    `오류여부`,
    `인스턴스코드식별자`,
    `PK여부`,
    `구간데이터여부`,
    `작업년월일`,
    `시스템최종처리일시`
)
SELECT
    IR.`그룹회사코드`,
    @occa_base                                      AS `기준년월일`,
    IR.`작업자수`,
    IR.`서버코드`,
    IR.`테이블명`,
    IR.`컬럼명`,
    IR.`인스턴스코드검증값`,                        -- ★ 정기 때 검증값 그대로 사용
    IR.`인스턴스코드별집계수`,
    CASE WHEN RAND() < 0.5 THEN 'Y' ELSE 'N' END    AS `오류여부`, -- 수시 결과
    IR.`인스턴스코드식별자`,
    IR.`PK여부`,
    IR.`구간데이터여부`,
    @occa_base                                      AS `작업년월일`,
    CONCAT(@occa_base, ' 13:00:00')                 AS `시스템최종처리일시`
FROM `DQ_MF_INST_RESULT` IR
INNER JOIN (SELECT * FROM `DQ_MF_ASSERTION_LIST_OCCA`  WHERE 기준년월일 = @occa_base) AO

 ON AO.`테이블명`       = IR.`테이블명`
 AND AO.`컬럼명`         = IR.`컬럼명`
 AND AO.`작업구분`       = '1'
WHERE IR.`기준년월일`    = @reg_base
  AND IR.`오류여부`      = 'Y';

INSERT INTO `DQ_MF_LIST_RESULT_OCCA` (
    `그룹회사코드`,
    `기준년월일`,
    `작업자수`,
    `서버코드`,
    `테이블명`,
    `컬럼명`,
    `인스턴스코드검증값`,
    `인스턴스코드별집계수`,
    `인스턴스코드식별자`,
    `오류여부`,
    `PK여부`,
    `구간데이터여부`,
    `작업년월일`,
    `시스템최종처리일시`
)
SELECT
    LR.`그룹회사코드`,
    @occa_base                                      AS `기준년월일`,
    LR.`작업자수`,
    LR.`서버코드`,
    LR.`테이블명`,
    LR.`컬럼명`,
    LR.`인스턴스코드검증값`,                        -- 정기 때 값 재사용
    LR.`인스턴스코드별집계수`,
    LR.`인스턴스코드식별자`,
    CASE WHEN RAND() < 0.5 THEN 'Y' ELSE 'N' END    AS `오류여부`,
    LR.`PK여부`,
    LR.`구간데이터여부`,
    @occa_base                                      AS `작업년월일`,
    CONCAT(@occa_base, ' 15:00:00')                 AS `시스템최종처리일시`
FROM `DQ_MF_LIST_RESULT` LR
JOIN (SELECT * FROM `DQ_MF_ASSERTION_LIST_OCCA`  WHERE 기준년월일 = @occa_base) AO
  ON AO.`그룹회사코드`   = LR.`그룹회사코드`
 AND AO.`테이블명`       = LR.`테이블명`
 AND AO.`컬럼명`         = LR.`컬럼명`
 AND AO.`작업구분`       = '3'
WHERE LR.`기준년월일`    = @reg_base
  AND LR.`오류여부`      = 'Y';




INSERT INTO `DQ_MF_DATE_RESULT_OCCA` (
    `그룹회사코드`,
    `기준년월일`,
    `작업자수`,
    `서버코드`,
    `테이블명`,
    `컬럼명`,
    `년월일검증값`,
    `년월일별집계수`,
    `오류여부`,
    `PK여부`,
    `구간데이터여부`,
    `작업년월일`,
    `시스템최종처리일시`
)
SELECT
    DR.`그룹회사코드`,
    @occa_base                                      AS `기준년월일`,
    DR.`작업자수`,
    DR.`서버코드`,
    DR.`테이블명`,
    DR.`컬럼명`,
    DR.`년월일검증값`,                              -- 정기 때 값 재사용
    DR.`년월일별집계수`,
    CASE WHEN RAND() < 0.5 THEN 'Y' ELSE 'N' END    AS `오류여부`,
    DR.`PK여부`,
    DR.`구간데이터여부`,
    @occa_base                                      AS `작업년월일`,
    CONCAT(@occa_base, ' 14:00:00')                 AS `시스템최종처리일시`
FROM `DQ_MF_DATE_RESULT` DR
JOIN (SELECT * FROM `DQ_MF_ASSERTION_LIST_OCCA`  WHERE 기준년월일 = @occa_base) AO
  ON AO.`그룹회사코드`   = DR.`그룹회사코드`
 AND AO.`테이블명`       = DR.`테이블명`
 AND AO.`컬럼명`         = DR.`컬럼명`
 AND AO.`작업구분`       = '2'
WHERE DR.`기준년월일`    = @reg_base
AND DR.`오류여부`      = 'Y';
