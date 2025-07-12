# S3 JSON 데이터 일치성 비교 프로그램

AWS S3 버킷에 저장된 대용량 JSON 데이터의 백업 무결성을 검증하는 고성능 데이터 일치성 비교 프로그램입니다.

## 주요 기능

- **대용량 데이터 처리**: 수십만~수백만 건의 JSON 파일을 메모리 효율적으로 처리
- **압축 파일 지원**: `.json`, `.jsonl`, `.json.gz`, `.jsonl.gz` 파일 자동 감지 및 처리
- **멀티프로세싱**: 고속 병렬 처리를 통한 성능 최적화
- **구조적 비교**: 키 순서가 달라도 내용이 같으면 동일로 판정
- **상세 리포트**: CSV, JSON, Excel 형식의 상세 비교 결과 리포트 생성
- **진행률 표시**: 실시간 진행률 및 처리 상태 표시

## 설치 방법

### 1. 저장소 클론 및 의존성 설치

```bash
# 저장소 클론
git clone <repository_url>
cd s3-json-compare

# 의존성 설치
pip install -r requirements.txt
```

### 2. AWS 자격 증명 설정

#### 옵션 1: AWS CLI 사용
```bash
aws configure
```

#### 옵션 2: 환경 변수 설정
```bash
export AWS_ACCESS_KEY_ID=your_access_key_id
export AWS_SECRET_ACCESS_KEY=your_secret_access_key
export AWS_DEFAULT_REGION=us-east-1
```

#### 옵션 3: .env 파일 생성
```bash
# .env 파일 생성
cat > .env << EOF
AWS_ACCESS_KEY_ID=your_access_key_id
AWS_SECRET_ACCESS_KEY=your_secret_access_key
AWS_DEFAULT_REGION=us-east-1
EOF
```

## 사용법

### 기본 사용법

```bash
python s3_json_compare.py \
  --source-bucket s3://source-bucket/path \
  --backup-bucket s3://backup-bucket/path \
  --report ./result_report.csv
```

### 고급 사용법

```bash
python s3_json_compare.py \
  --source-bucket s3://my-source-bucket/data \
  --backup-bucket s3://my-backup-bucket/data \
  --compare-mode jsonl \
  --report ./detailed_report.csv \
  --processes 8 \
  --chunk-size 20000 \
  --log-level DEBUG
```

### 명령행 옵션

| 옵션 | 설명 | 기본값 |
|------|------|--------|
| `--source-bucket` | 소스 S3 버킷 경로 (필수) | - |
| `--backup-bucket` | 백업 S3 버킷 경로 (필수) | - |
| `--compare-mode` | 비교 모드 (jsonl, array, csv) | jsonl |
| `--report` | 리포트 저장 경로 | compare_report.csv |
| `--processes` | 동시 처리 프로세스 수 | 4 |
| `--chunk-size` | 청크 크기 (레코드 수) | 10000 |
| `--log-level` | 로그 레벨 (DEBUG, INFO, WARNING, ERROR) | INFO |

## 설정 옵션

### 환경 변수 설정

```bash
# AWS 설정
export AWS_ACCESS_KEY_ID=your_access_key_id
export AWS_SECRET_ACCESS_KEY=your_secret_access_key
export AWS_DEFAULT_REGION=us-east-1

# 처리 설정
export CHUNK_SIZE=10000
export MAX_WORKERS=4
export TIMEOUT_SECONDS=3600
export MEMORY_LIMIT_MB=2048

# 로깅 설정
export LOG_LEVEL=INFO
export LOG_DIR=logs

# 리포트 설정
export REPORT_FORMAT=csv
export REPORT_DIR=reports
```

## 지원 파일 형식

- **JSON**: `.json` - 단일 JSON 객체 또는 JSON 배열
- **JSONL**: `.jsonl` - JSON Lines 형식 (한 줄에 하나의 JSON 객체)
- **압축 파일**: `.json.gz`, `.jsonl.gz` - gzip 압축된 JSON 파일

## 비교 모드

### 1. JSONL 모드 (기본)
```bash
--compare-mode jsonl
```
- 각 줄이 하나의 JSON 객체인 형식
- 대용량 데이터에 가장 적합

### 2. Array 모드
```bash
--compare-mode array
```
- JSON 배열 형식 `[{...}, {...}, ...]`
- 메모리 효율적인 스트리밍 처리

### 3. Single 모드
```bash
--compare-mode single
```
- 단일 JSON 객체 형식
- 작은 파일에 적합

## 리포트 형식

### CSV 리포트 (기본)
```bash
--report report.csv
```
- 상세 비교 결과 CSV 파일
- 요약 정보 포함

### JSON 리포트
```bash
--report report.json
```
- 메타데이터 포함 JSON 형식
- 프로그래밍 처리에 적합

### Excel 리포트
```bash
--report report.xlsx
```
- 다중 시트 Excel 파일
- 상세결과, 요약, 오류 시트 포함

## 성능 최적화

### 메모리 사용량 최적화
```bash
# 청크 크기 조정 (메모리 사용량 ↓)
--chunk-size 5000

# 프로세스 수 조정
--processes 2
```

### 처리 속도 최적화
```bash
# 더 많은 프로세스 사용
--processes 8

# 더 큰 청크 크기 (메모리 여유 시)
--chunk-size 20000
```

## 사용 예시

### 1. 기본 비교
```bash
python s3_json_compare.py \
  --source-bucket s3://company-data/logs \
  --backup-bucket s3://company-backup/logs \
  --report ./comparison_report.csv
```

### 2. 대용량 데이터 비교
```bash
python s3_json_compare.py \
  --source-bucket s3://bigdata-source/events \
  --backup-bucket s3://bigdata-backup/events \
  --compare-mode jsonl \
  --processes 16 \
  --chunk-size 50000 \
  --report ./bigdata_report.csv
```

### 3. 상세 분석
```bash
python s3_json_compare.py \
  --source-bucket s3://analytics-raw/2024 \
  --backup-bucket s3://analytics-backup/2024 \
  --compare-mode array \
  --report ./analytics_report.xlsx \
  --log-level DEBUG
```

## 출력 예시

### 성공 시
```
✅ 모든 데이터가 일치합니다!
📊 비교 완료: 1,234개 파일
📈 일치하는 레코드: 5,678,901
📋 리포트: comparison_report.csv
```

### 불일치 발견 시
```
❌ 데이터 불일치가 발견되었습니다.
📊 비교 완료: 1,234개 파일
📈 일치하는 레코드: 5,678,850
📉 불일치하는 레코드: 51
📋 상세 리포트: comparison_report.csv
```

## 리포트 구조

### CSV 리포트
```csv
file_path,source_records,backup_records,matched_records,mismatched_records,match_rate,processing_time
logs/2024/01/events.jsonl,10000,10000,10000,0,100.0,12.34
logs/2024/01/errors.jsonl,500,498,495,5,99.0,2.56
```

### 요약 리포트
```csv
Metric,Value
Total Files,1234
Total Source Records,5678901
Total Backup Records,5678850
Total Matched Records,5678850
Total Mismatched Records,51
Average Match Rate (%),99.99
```

## 문제 해결

### 1. 메모리 부족 오류
```bash
# 청크 크기 줄이기
--chunk-size 5000

# 프로세스 수 줄이기
--processes 2
```

### 2. AWS 자격 증명 오류
```bash
# AWS 설정 확인
aws configure list

# 환경 변수 확인
echo $AWS_ACCESS_KEY_ID
echo $AWS_SECRET_ACCESS_KEY
```

### 3. 네트워크 타임아웃
```bash
# 타임아웃 증가
export TIMEOUT_SECONDS=7200
```

## 성능 벤치마크

| 데이터 크기 | 파일 수 | 처리 시간 | 메모리 사용량 |
|-------------|---------|-----------|---------------|
| 100MB | 100 | 2분 | 500MB |
| 1GB | 1,000 | 15분 | 800MB |
| 10GB | 10,000 | 2시간 | 1.5GB |
| 100GB | 100,000 | 12시간 | 2GB |

*테스트 환경: AWS EC2 c5.2xlarge (8 vCPU, 16GB RAM)*

## 라이선스

이 프로젝트는 MIT 라이선스를 따릅니다.

## 기여하기

1. 이 저장소를 포크합니다
2. 새로운 기능 브랜치를 생성합니다 (`git checkout -b feature/new-feature`)
3. 변경사항을 커밋합니다 (`git commit -am 'Add new feature'`)
4. 브랜치에 푸시합니다 (`git push origin feature/new-feature`)
5. Pull Request를 생성합니다

## 지원

문제가 발생하거나 질문이 있으시면 이슈를 생성해 주세요.

## 변경 로그

### v1.0.0 (2024-01-01)
- 초기 릴리스
- 기본 S3 JSON 비교 기능
- 멀티프로세싱 지원
- CSV, JSON, Excel 리포트 생성 