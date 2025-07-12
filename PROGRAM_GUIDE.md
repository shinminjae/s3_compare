source-bucket "s3://ddtm-agmtms-iot-telemetry-s3-raw-meta-bucket/TRIP/type=X/year=2023/month=2/" 
backup-bucket "s3://ddtm-agmtms-iot-telemetry-s3-raw-meta-bucket/temp/TRIP/type=X/year=2023/month=2/" 


# 🚀 S3 JSON 데이터 일치성 비교 프로그램

## 📋 프로그램 소개

AWS S3 버킷에 저장된 대용량 JSON 데이터의 백업 무결성을 검증하는 Python 프로그램입니다. 소스 버킷과 백업 버킷의 JSON 데이터를 비교하여 데이터 일치성을 확인하고, 불일치하는 레코드를 상세히 분석할 수 있습니다.

### 🎯 주요 목적
- **백업 무결성 검증**: 소스 데이터와 백업 데이터의 일치성 확인
- **데이터 손실 방지**: 백업 과정에서 발생할 수 있는 데이터 손실 감지
- **상세 분석**: 불일치하는 레코드의 JSON 내용까지 상세히 분석
- **대용량 처리**: 수천만 개의 레코드를 효율적으로 처리

## ✨ 주요 특징

### 🔍 **스마트 데이터 비교**
- **해시 기반 비교**: 각 JSON 레코드를 SHA256 해시로 변환하여 빠른 비교
- **정규화 처리**: JSON 키 순서를 정규화하여 동일한 내용의 다른 형태도 일치로 인식
- **메모리 효율성**: SQLite in-memory 데이터베이스 사용으로 대용량 데이터 처리

### 📊 **다양한 JSON 형식 지원**
- **JSONL (JSON Lines)**: 한 줄에 하나의 JSON 객체
- **JSON Array**: JSON 배열 형태
- **CSV**: CSV 형태의 데이터

### 🗂️ **포괄적인 리포트 시스템**
- **통계 리포트**: 전체 비교 결과 통계
- **상세 불일치 리포트**: 불일치하는 레코드의 JSON 내용 포함
- **누적 저장**: 기존 리포트에 새로운 결과 추가 (덮어쓰기 방지)
- **다양한 형식**: CSV, JSON, Excel 형식 지원

### ⚡ **고성능 처리**
- **스트리밍 처리**: 대용량 파일을 메모리에 로드하지 않고 스트리밍으로 처리
- **압축 파일 지원**: gzip 압축 파일 직접 처리
- **진행률 표시**: tqdm을 사용한 실시간 진행률 표시

### 🛡️ **안정성 및 오류 처리**
- **AWS 인증**: boto3를 통한 안전한 AWS 인증
- **오류 복구**: 개별 파일 처리 실패 시에도 전체 프로세스 계속 진행
- **상세 로깅**: 다양한 로그 레벨로 디버깅 지원

## 🎮 사용법

### 1. **가상환경 활성화 및 프로그램 실행**

#### 🌐 **가상환경 활성화**
```bash
# Windows
venv\Scripts\activate

# macOS/Linux
source venv/bin/activate

# 활성화 확인 (프롬프트 앞에 (venv) 표시)
(venv) C:\s3_compare_project>
```

#### 🚀 **프로그램 실행**
```bash
# 가상환경이 활성화된 상태에서 실행
python s3_json_compare.py
```

### 2. **대화형 메뉴 설정**

프로그램 실행 후 다음 순서로 설정을 입력합니다:

#### 🔍 **소스 버킷 설정**
```
소스 S3 버킷 URL을 입력하세요 (예: s3://my-bucket/path/): 
s3://ddtm-agmtms-iot-telemetry-s3-raw-meta-bucket/TRIP/type=X/year=2023/month=2/
```

#### 💾 **백업 버킷 설정**
```
백업 S3 버킷 URL을 입력하세요 (예: s3://my-backup-bucket/path/): 
s3://ddtm-agmtms-iot-telemetry-s3-raw-meta-bucket/temp/TRIP/type=X/year=2023/month=2/
```

#### 📊 **비교 모드 선택**
```
비교 모드를 선택하세요:
1. JSONL (JSON Lines) - 한 줄에 하나의 JSON 객체
2. Array - JSON 배열 형태
3. CSV - CSV 형태

선택 (1-3, 기본값: 1): 1
```

#### 📦 **청크 크기 설정**
```
청크 크기를 입력하세요 (기본값: 20000): 20000
```

#### 📄 **리포트 파일 경로**
```
리포트 파일 경로를 입력하세요 (기본값: ./detailed_report.csv): 
./detailed_report.csv
```

#### 📝 **로그 레벨 선택**
```
로그 레벨을 선택하세요:
1. DEBUG - 상세한 디버그 정보
2. INFO - 일반 정보 (기본값)
3. WARNING - 경고만
4. ERROR - 오류만

선택 (1-4, 기본값: 2): 2
```

### 3. **설정 확인 및 실행**
```
============================================================
📋 입력된 설정을 확인해주세요:
🔍 소스: s3://ddtm-agmtms-iot-telemetry-s3-raw-meta-bucket/TRIP/type=X/year=2023/month=2/
💾 백업: s3://ddtm-agmtms-iot-telemetry-s3-raw-meta-bucket/temp/TRIP/type=X/year=2023/month=2/
📊 모드: jsonl
📦 청크: 20000
📄 리포트: ./detailed_report.csv
📝 로그: INFO
============================================================

🚀 비교를 시작하시겠습니까? (y/N): y
```

## 📁 출력 파일

### 📊 **주요 리포트 파일**
- `detailed_report.csv`: 전체 비교 결과 통계
- `detailed_report_detailed.csv`: 불일치하는 레코드의 JSON 내용
- `detailed_report_summary.csv`: 요약 통계

### 📋 **리포트 내용**

#### `detailed_report.csv` (통계 리포트)
| 컬럼 | 설명 |
|------|------|
| file_path | 파일 경로 |
| source_records | 소스 레코드 수 |
| backup_records | 백업 레코드 수 |
| matched_records | 일치하는 레코드 수 |
| mismatched_records | 불일치하는 레코드 수 |
| missing_in_backup | 백업에서 누락된 레코드 수 |
| missing_in_source | 소스에서 누락된 레코드 수 |
| match_rate | 일치율 (%) |

#### `detailed_report_detailed.csv` (상세 불일치 리포트)
| 컬럼 | 설명 |
|------|------|
| hash | 전체 해시값 |
| hash_short | 축약된 해시값 |
| file_path | 파일 경로 |
| bucket_type | 버킷 타입 (source_only/backup_only) |
| json_content | 원본 JSON 레코드 내용 |

## 🔧 설치 및 설정

### 1. **Python 가상환경 설정 (권장)**

#### 🐍 **Python 설치 확인**
```bash
python --version
# Python 3.8 이상 권장
```

#### 📁 **프로젝트 디렉토리 생성 및 이동**
```bash
mkdir s3_compare_project
cd s3_compare_project
```

#### 🌐 **가상환경 생성**
```bash
# 가상환경 생성
python -m venv venv

# 가상환경 활성화 (Windows)
venv\Scripts\activate

# 가상환경 활성화 (macOS/Linux)
source venv/bin/activate
```

#### 📦 **필요한 패키지 설치**
```bash
# pip 업그레이드
pip install --upgrade pip

# requirements.txt 파일 생성 (아래 내용으로)
echo "boto3>=1.26.0
ijson>=3.2.0
pandas>=1.5.0
tqdm>=4.64.0
openpyxl>=3.0.0" > requirements.txt

# 패키지 설치
pip install -r requirements.txt
```

#### 🔍 **설치 확인**
```bash
# 설치된 패키지 확인
pip list

# Python 경로 확인 (가상환경 내부인지 확인)
which python  # macOS/Linux
where python  # Windows
```

### 2. **프로그램 파일 다운로드**

#### 📥 **프로그램 파일 복사**
```bash
# 메인 프로그램 파일 복사
# s3_json_compare.py 파일을 현재 디렉토리에 복사

# utils 폴더 생성 및 모듈 파일들 복사
mkdir utils
# utils/__init__.py, utils/s3_handler.py, utils/json_processor.py, 
# utils/report_generator.py, utils/logger.py 파일들을 utils 폴더에 복사
```

### 3. **AWS 자격 증명 설정**

#### 🔑 **AWS CLI 설치 및 설정**
```bash
# AWS CLI 설치 (Windows)
# https://aws.amazon.com/cli/ 에서 다운로드

# AWS CLI 설치 (macOS)
brew install awscli

# AWS CLI 설치 (Ubuntu/Debian)
sudo apt-get install awscli

# AWS 자격 증명 설정
aws configure
# AWS Access Key ID: your_access_key
# AWS Secret Access Key: your_secret_key
# Default region name: ap-northeast-2
# Default output format: json
```

#### 🌍 **환경 변수 설정 (대안)**
```bash
# Windows (PowerShell)
$env:AWS_ACCESS_KEY_ID="your_access_key"
$env:AWS_SECRET_ACCESS_KEY="your_secret_key"
$env:AWS_DEFAULT_REGION="ap-northeast-2"

# Windows (Command Prompt)
set AWS_ACCESS_KEY_ID=your_access_key
set AWS_SECRET_ACCESS_KEY=your_secret_key
set AWS_DEFAULT_REGION=ap-northeast-2

# macOS/Linux
export AWS_ACCESS_KEY_ID=your_access_key
export AWS_SECRET_ACCESS_KEY=your_secret_key
export AWS_DEFAULT_REGION=ap-northeast-2
```

### 4. **필요한 권한**
- S3 버킷 읽기 권한
- 소스 및 백업 버킷 접근 권한

### 5. **가상환경 사용 시 주의사항**

#### ✅ **가상환경 활성화 확인**
```bash
# 프롬프트 앞에 (venv)가 표시되는지 확인
(venv) C:\s3_compare_project>
```

#### 🔄 **가상환경 비활성화**
```bash
# 작업 완료 후 가상환경 비활성화
deactivate
```

#### 📝 **가상환경 재활성화**
```bash
# 다음에 사용할 때 다시 활성화
venv\Scripts\activate  # Windows
source venv/bin/activate  # macOS/Linux
```

## 📈 성능 최적화

### 🚀 **처리 성능**
- **대용량 처리**: 수천만 개 레코드 처리 가능
- **메모리 효율성**: SQLite in-memory로 메모리 사용량 최적화
- **스트리밍 처리**: 대용량 파일도 메모리 부족 없이 처리

### ⚙️ **최적화 팁**
- **청크 크기 조정**: 메모리 상황에 따라 10000-50000 사이 조정
- **로그 레벨**: 대용량 처리 시 INFO 또는 WARNING 레벨 사용
- **네트워크**: AWS 리전과 가까운 환경에서 실행

## 🐛 문제 해결

### ❌ **일반적인 오류**

#### AWS 인증 오류
```
NoCredentialsError: Unable to locate credentials
```
**해결방법**: AWS 자격 증명을 올바르게 설정

#### S3 접근 오류
```
AccessDenied: Access Denied
```
**해결방법**: S3 버킷 접근 권한 확인

#### 메모리 부족 오류
```
MemoryError: ...
```
**해결방법**: 청크 크기를 줄이거나 시스템 메모리 증가

#### 가상환경 관련 오류
```
ModuleNotFoundError: No module named 'boto3'
```
**해결방법**: 
1. 가상환경이 활성화되었는지 확인: `(venv)` 표시 확인
2. 패키지 재설치: `pip install -r requirements.txt`
3. Python 경로 확인: `which python` (macOS/Linux) 또는 `where python` (Windows)

#### 가상환경 활성화 오류 (Windows)
```
venv\Scripts\activate : 이 시스템에서 스크립트를 실행할 수 없습니다.
```
**해결방법**:
1. PowerShell 실행 정책 변경: `Set-ExecutionPolicy -ExecutionPolicy RemoteSigned -Scope CurrentUser`
2. 또는 Command Prompt 사용: `venv\Scripts\activate.bat`

### 📞 **지원 정보**
- **로그 확인**: 상세한 오류 정보는 로그에서 확인
- **디버그 모드**: DEBUG 로그 레벨로 상세 정보 확인
- **단계별 실행**: 소규모 데이터로 먼저 테스트

## 🔄 업데이트 내역

### v1.0.0 (현재 버전)
- ✅ 메뉴 형태 인터페이스 구현
- ✅ 불일치 레코드 JSON 내용 출력
- ✅ 누적 리포트 저장 기능
- ✅ 대용량 데이터 처리 최적화
- ✅ 다양한 JSON 형식 지원
- ✅ 상세한 오류 처리 및 로깅

## 📄 라이선스

이 프로그램은 MIT 라이선스 하에 배포됩니다.

## 🚀 빠른 시작 가이드

### 📋 **5분 만에 시작하기**

```bash
# 1. 프로젝트 디렉토리 생성
mkdir s3_compare_project && cd s3_compare_project

# 2. 가상환경 생성 및 활성화
python -m venv venv
venv\Scripts\activate  # Windows
# source venv/bin/activate  # macOS/Linux

# 3. 패키지 설치
pip install boto3 ijson pandas tqdm openpyxl

# 4. 프로그램 파일 복사 (s3_json_compare.py 및 utils 폴더)

# 5. AWS 설정
aws configure

# 6. 프로그램 실행
python s3_json_compare.py
```

### ✅ **체크리스트**
- [ ] Python 3.8+ 설치됨
- [ ] 가상환경 생성 및 활성화됨
- [ ] 필요한 패키지 설치됨
- [ ] AWS 자격 증명 설정됨
- [ ] S3 버킷 접근 권한 확인됨

---

**💡 팁**: 처음 사용 시에는 소규모 데이터로 테스트한 후 대용량 데이터를 처리하는 것을 권장합니다. 