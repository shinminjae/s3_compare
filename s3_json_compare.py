#!/usr/bin/env python3
"""
S3 JSON 데이터 일치성 비교 프로그램

AWS S3 버킷에 저장된 대용량 JSON 데이터의 백업 무결성을 검증하는 프로그램
"""

import asyncio
import concurrent.futures
import gzip
import hashlib
import json
import logging
import os
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Generator, List, Optional, Tuple, Union

import boto3
import ijson
from botocore.exceptions import ClientError, NoCredentialsError
from tqdm import tqdm

from utils.s3_handler import S3Handler
from utils.json_processor import JSONProcessor
from utils.report_generator import ReportGenerator
from utils.logger import setup_logger


@dataclass
class CompareResult:
    """비교 결과를 저장하는 데이터 클래스"""
    file_path: str
    source_records: int
    backup_records: int
    matched_records: int
    mismatched_records: int
    missing_in_backup: int
    missing_in_source: int
    errors: List[str]
    processing_time: float


class S3JSONComparer:
    """S3 JSON 데이터 비교 클래스"""
    
    def __init__(self, source_bucket: str, backup_bucket: str, 
                 processes: int = 4, chunk_size: int = 10000):
        self.source_bucket = source_bucket
        self.backup_bucket = backup_bucket
        self.processes = processes
        self.chunk_size = chunk_size
        self.logger = setup_logger(__name__)
        
        # S3 핸들러 초기화
        self.s3_handler = S3Handler()
        self.json_processor = JSONProcessor(chunk_size)
        self.report_generator = ReportGenerator()
        
        # 결과 저장
        self.compare_results: List[CompareResult] = []
        
    def get_file_list(self, bucket: str, prefix: str = "") -> List[str]:
        """S3 버킷에서 파일 목록을 가져옵니다"""
        try:
            return self.s3_handler.list_files(bucket, prefix)
        except Exception as e:
            self.logger.error(f"파일 목록 가져오기 실패: {e}")
            return []
    
    def compare_file_pair(self, file_path: str) -> CompareResult:
        """개별 파일 쌍을 비교합니다"""
        start_time = time.time()
        result = CompareResult(
            file_path=file_path,
            source_records=0,
            backup_records=0,
            matched_records=0,
            mismatched_records=0,
            missing_in_backup=0,
            missing_in_source=0,
            errors=[],
            processing_time=0
        )
        
        try:
            # 소스 데이터 해시 생성
            source_hashes = self._generate_file_hashes(
                self.source_bucket, file_path
            )
            result.source_records = len(source_hashes)
            
            # 백업 데이터 해시 생성
            backup_hashes = self._generate_file_hashes(
                self.backup_bucket, file_path
            )
            result.backup_records = len(backup_hashes)
            
            # 해시 비교
            source_set = set(source_hashes)
            backup_set = set(backup_hashes)
            
            result.matched_records = len(source_set.intersection(backup_set))
            result.missing_in_backup = len(source_set - backup_set)
            result.missing_in_source = len(backup_set - source_set)
            result.mismatched_records = (
                result.missing_in_backup + result.missing_in_source
            )
            
        except Exception as e:
            error_msg = f"파일 비교 중 오류 발생: {str(e)}"
            result.errors.append(error_msg)
            self.logger.error(error_msg)
        
        result.processing_time = time.time() - start_time
        return result
    
    def _generate_file_hashes(self, bucket: str, file_path: str) -> List[str]:
        """파일에서 레코드별 해시를 생성합니다"""
        hashes = []
        
        try:
            # S3에서 스트림으로 파일 읽기
            stream = self.s3_handler.get_file_stream(bucket, file_path)
            
            # 압축 파일 처리
            if file_path.endswith('.gz'):
                gzip_stream = gzip.GzipFile(fileobj=stream)
                # gzip 스트림을 텍스트로 디코딩
                import io
                text_stream = io.TextIOWrapper(gzip_stream, encoding='utf-8')
            else:
                # 바이너리 스트림을 텍스트로 디코딩
                import io
                text_stream = io.TextIOWrapper(stream, encoding='utf-8')
            
            # JSON 처리 모드에 따라 다른 방식으로 처리
            for record in self.json_processor.process_stream(text_stream):
                # 레코드를 정규화하고 해시 생성
                normalized_record = self._normalize_record(record)
                record_hash = self._generate_record_hash(normalized_record)
                hashes.append(record_hash)
                
        except Exception as e:
            raise Exception(f"파일 해시 생성 실패 ({file_path}): {str(e)}")
        
        return hashes
    
    def _normalize_record(self, record: Dict) -> Dict:
        """레코드를 정규화합니다 (키 순서 정렬)"""
        if isinstance(record, dict):
            return {k: self._normalize_record(v) for k, v in sorted(record.items())}
        elif isinstance(record, list):
            return [self._normalize_record(item) for item in record]
        else:
            return record
    
    def _generate_record_hash(self, record: Dict) -> str:
        """레코드의 해시를 생성합니다"""
        record_json = json.dumps(record, sort_keys=True, ensure_ascii=False)
        return hashlib.sha256(record_json.encode('utf-8')).hexdigest()
    
    def _find_record_by_hash(self, bucket: str, file_path: str, target_hash: str) -> Optional[Dict]:
        """특정 해시에 해당하는 원본 JSON 레코드를 찾습니다"""
        try:
            # S3에서 스트림으로 파일 읽기
            stream = self.s3_handler.get_file_stream(bucket, file_path)
            
            # 압축 파일 처리
            if file_path.endswith('.gz'):
                gzip_stream = gzip.GzipFile(fileobj=stream)
                import io
                text_stream = io.TextIOWrapper(gzip_stream, encoding='utf-8')
            else:
                import io
                text_stream = io.TextIOWrapper(stream, encoding='utf-8')
            
            # JSON 처리 모드에 따라 다른 방식으로 처리
            for record in self.json_processor.process_stream(text_stream):
                # 레코드를 정규화하고 해시 생성
                normalized_record = self._normalize_record(record)
                record_hash = self._generate_record_hash(normalized_record)
                
                if record_hash == target_hash:
                    return record  # 원본 레코드 반환 (정규화되지 않은)
                    
        except Exception as e:
            self.logger.error(f"해시로 레코드 찾기 실패 ({file_path}, {target_hash[:16]}...): {str(e)}")
        
        return None
    
    def compare_buckets(self, source_prefix: str = "", backup_prefix: str = "",
                       report_path: str = "compare_report.csv") -> bool:
        """두 버킷의 모든 파일을 비교합니다 - SQLite in-memory (단일 프로세스)"""
        import sqlite3
        import tempfile
        
        self.logger.info("S3 버킷 비교 시작 (SQLite in-memory + 단일 프로세스)")
        
        # 파일 목록 가져오기
        source_files = self.get_file_list(self.source_bucket, source_prefix)
        backup_files = self.get_file_list(self.backup_bucket, backup_prefix)
        
        self.logger.info(f"소스 버킷 파일 수: {len(source_files)}")
        self.logger.info(f"백업 버킷 파일 수: {len(backup_files)}")
        
        # SQLite in-memory 데이터베이스 생성
        conn = sqlite3.connect(':memory:')
        cursor = conn.cursor()
        
        # 해시 테이블 생성
        cursor.execute('''
            CREATE TABLE source_hashes (
                hash TEXT PRIMARY KEY,
                file_path TEXT,
                record_count INTEGER
            )
        ''')
        
        cursor.execute('''
            CREATE TABLE backup_hashes (
                hash TEXT PRIMARY KEY,
                file_path TEXT,
                record_count INTEGER
            )
        ''')
        
        # 인덱스 생성 (성능 향상)
        cursor.execute('CREATE INDEX idx_source_hash ON source_hashes(hash)')
        cursor.execute('CREATE INDEX idx_backup_hash ON backup_hashes(hash)')
        
        # 소스 파일 처리 (단일 프로세스)
        self.logger.info("소스 버킷 파일 내용 해시화 시작...")
        source_total_records = 0
        
        with tqdm(total=len(source_files), desc="소스 파일 처리") as pbar:
            for file_path in source_files:
                try:
                    file_hashes = self._generate_file_hashes(self.source_bucket, file_path)
                    source_total_records += len(file_hashes)
                    
                    # SQLite에 배치 삽입
                    if file_hashes:
                        hash_data = [(hash_val, file_path, 1) for hash_val in file_hashes]
                        cursor.executemany(
                            'INSERT OR IGNORE INTO source_hashes (hash, file_path, record_count) VALUES (?, ?, ?)',
                            hash_data
                        )
                    
                    pbar.set_postfix({'current': file_path, 'records': len(file_hashes)})
                    
                except Exception as e:
                    self.logger.error(f"소스 파일 처리 실패 ({file_path}): {e}")
                    result = CompareResult(
                        file_path=file_path,
                        source_records=0,
                        backup_records=0,
                        matched_records=0,
                        mismatched_records=0,
                        missing_in_backup=0,
                        missing_in_source=0,
                        errors=[f"소스 파일 처리 실패: {str(e)}"],
                        processing_time=0
                    )
                    self.compare_results.append(result)
                pbar.update(1)
        
        # 백업 파일 처리 (단일 프로세스)
        self.logger.info("백업 버킷 파일 내용 해시화 시작...")
        backup_total_records = 0
        
        with tqdm(total=len(backup_files), desc="백업 파일 처리") as pbar:
            for file_path in backup_files:
                try:
                    file_hashes = self._generate_file_hashes(self.backup_bucket, file_path)
                    backup_total_records += len(file_hashes)
                    
                    # SQLite에 배치 삽입
                    if file_hashes:
                        hash_data = [(hash_val, file_path, 1) for hash_val in file_hashes]
                        cursor.executemany(
                            'INSERT OR IGNORE INTO backup_hashes (hash, file_path, record_count) VALUES (?, ?, ?)',
                            hash_data
                        )
                    
                    pbar.set_postfix({'current': file_path, 'records': len(file_hashes)})
                    
                except Exception as e:
                    self.logger.error(f"백업 파일 처리 실패 ({file_path}): {e}")
                    result = CompareResult(
                        file_path=file_path,
                        source_records=0,
                        backup_records=0,
                        matched_records=0,
                        mismatched_records=0,
                        missing_in_backup=0,
                        missing_in_source=0,
                        errors=[f"백업 파일 처리 실패: {str(e)}"],
                        processing_time=0
                    )
                    self.compare_results.append(result)
                pbar.update(1)
        
        # SQL을 사용한 효율적인 비교
        self.logger.info("SQLite를 사용한 전체 내용 비교 시작...")
        
        # 일치하는 해시 수
        cursor.execute('''
            SELECT COUNT(*) FROM source_hashes s
            INNER JOIN backup_hashes b ON s.hash = b.hash
        ''')
        matched_records = cursor.fetchone()[0]
        
        # 소스에서만 있는 해시 수
        cursor.execute('''
            SELECT COUNT(*) FROM source_hashes s
            LEFT JOIN backup_hashes b ON s.hash = b.hash
            WHERE b.hash IS NULL
        ''')
        missing_in_backup = cursor.fetchone()[0]
        
        # 백업에서만 있는 해시 수
        cursor.execute('''
            SELECT COUNT(*) FROM backup_hashes b
            LEFT JOIN source_hashes s ON b.hash = s.hash
            WHERE s.hash IS NULL
        ''')
        missing_in_source = cursor.fetchone()[0]
        
        # 불일치하는 레코드 정보 수집
        mismatched_records = []
        
        if missing_in_backup > 0 or missing_in_source > 0:
            self.logger.info("불일치하는 레코드의 JSON 내역을 출력합니다...")
            
            # 소스에서만 있는 레코드 샘플 출력 (최대 10개)
            if missing_in_backup > 0:
                cursor.execute('''
                    SELECT s.hash, s.file_path FROM source_hashes s
                    LEFT JOIN backup_hashes b ON s.hash = b.hash
                    WHERE b.hash IS NULL
                    LIMIT 10
                ''')
                source_only_hashes = cursor.fetchall()
                
                self.logger.info(f"소스에서만 있는 레코드 샘플 ({len(source_only_hashes)}개):")
                for hash_val, file_path in source_only_hashes:
                    # 해당 해시의 원본 JSON 레코드 찾기
                    original_record = self._find_record_by_hash(self.source_bucket, file_path, hash_val)
                    if original_record:
                        self.logger.info(f"  해시: {hash_val[:16]}...")
                        self.logger.info(f"  파일: {file_path}")
                        self.logger.info(f"  JSON: {json.dumps(original_record, ensure_ascii=False, indent=2)}")
                        self.logger.info("  " + "-" * 50)
                        
                        # 불일치 레코드 정보 수집
                        mismatched_records.append({
                            'hash': hash_val,
                            'file_path': file_path,
                            'bucket_type': 'source_only',
                            'json_content': json.dumps(original_record, ensure_ascii=False),
                            'hash_short': hash_val[:16] + '...'
                        })
            
            # 백업에서만 있는 레코드 샘플 출력 (최대 10개)
            if missing_in_source > 0:
                cursor.execute('''
                    SELECT b.hash, b.file_path FROM backup_hashes b
                    LEFT JOIN source_hashes s ON b.hash = s.hash
                    WHERE s.hash IS NULL
                    LIMIT 10
                ''')
                backup_only_hashes = cursor.fetchall()
                
                self.logger.info(f"백업에서만 있는 레코드 샘플 ({len(backup_only_hashes)}개):")
                for hash_val, file_path in backup_only_hashes:
                    # 해당 해시의 원본 JSON 레코드 찾기
                    original_record = self._find_record_by_hash(self.backup_bucket, file_path, hash_val)
                    if original_record:
                        self.logger.info(f"  해시: {hash_val[:16]}...")
                        self.logger.info(f"  파일: {file_path}")
                        self.logger.info(f"  JSON: {json.dumps(original_record, ensure_ascii=False, indent=2)}")
                        self.logger.info("  " + "-" * 50)
                        
                        # 불일치 레코드 정보 수집
                        mismatched_records.append({
                            'hash': hash_val,
                            'file_path': file_path,
                            'bucket_type': 'backup_only',
                            'json_content': json.dumps(original_record, ensure_ascii=False),
                            'hash_short': hash_val[:16] + '...'
                        })
        
        total_mismatched_records = missing_in_backup + missing_in_source
        
        self.logger.info(f"소스 총 레코드: {source_total_records}")
        self.logger.info(f"백업 총 레코드: {backup_total_records}")
        self.logger.info(f"일치하는 레코드: {matched_records}")
        self.logger.info(f"불일치하는 레코드: {total_mismatched_records}")
        self.logger.info(f"  - 소스에서만: {missing_in_backup}")
        self.logger.info(f"  - 백업에서만: {missing_in_source}")
        
        # 전체 비교 결과 생성
        overall_result = CompareResult(
            file_path="OVERALL_COMPARISON",
            source_records=source_total_records,
            backup_records=backup_total_records,
            matched_records=matched_records,
            mismatched_records=total_mismatched_records,
            missing_in_backup=missing_in_backup,
            missing_in_source=missing_in_source,
            errors=[],
            processing_time=0
        )
        self.compare_results.append(overall_result)
        
        # 개별 파일 통계 (샘플링)
        cursor.execute('''
            SELECT file_path, COUNT(*) as record_count 
            FROM source_hashes 
            GROUP BY file_path 
            ORDER BY record_count DESC 
            LIMIT 10
        ''')
        source_file_stats = cursor.fetchall()
        
        for file_path, record_count in source_file_stats:
            result = CompareResult(
                file_path=f"SOURCE: {file_path}",
                source_records=record_count,
                backup_records=0,
                matched_records=0,
                mismatched_records=0,
                missing_in_backup=0,
                missing_in_source=0,
                errors=[],
                processing_time=0
            )
            self.compare_results.append(result)
        
        cursor.execute('''
            SELECT file_path, COUNT(*) as record_count 
            FROM backup_hashes 
            GROUP BY file_path 
            ORDER BY record_count DESC 
            LIMIT 10
        ''')
        backup_file_stats = cursor.fetchall()
        
        for file_path, record_count in backup_file_stats:
            result = CompareResult(
                file_path=f"BACKUP: {file_path}",
                source_records=0,
                backup_records=record_count,
                matched_records=0,
                mismatched_records=0,
                missing_in_backup=0,
                missing_in_source=0,
                errors=[],
                processing_time=0
            )
            self.compare_results.append(result)
        
        # 연결 종료
        conn.close()
        
        # 리포트 생성
        self.report_generator.generate_report(self.compare_results, report_path)
        
        # 불일치하는 레코드 상세 리포트 생성
        if mismatched_records:
            detailed_report_path = report_path.replace('.csv', '_detailed.csv')
            self.report_generator.generate_detailed_mismatch_report_with_json(
                mismatched_records, detailed_report_path
            )
        
        # 결과 요약
        self.logger.info(f"비교 완료: 소스 {len(source_files)}개, 백업 {len(backup_files)}개 파일")
        self.logger.info(f"일치하는 레코드: {matched_records}")
        self.logger.info(f"불일치하는 레코드: {total_mismatched_records}")
        
        return total_mismatched_records == 0


def main():
    """메인 함수 - 메뉴 형태"""
    print("=" * 60)
    print("🚀 S3 JSON 데이터 일치성 비교 프로그램")
    print("=" * 60)
    
    # S3 URL 파싱 함수
    def parse_s3_url(url: str) -> Tuple[str, str]:
        """S3 URL을 파싱합니다"""
        if not url.startswith("s3://"):
            raise ValueError("S3 URL은 s3://로 시작해야 합니다")
        
        url = url[5:]  # s3:// 제거
        parts = url.split("/", 1)
        bucket = parts[0]
        prefix = parts[1] if len(parts) > 1 else ""
        
        return bucket, prefix
    
    try:
        # 메뉴 입력 받기
        print("\n📋 설정 정보를 입력해주세요:")
        
        # 소스 버킷 입력
        while True:
            source_bucket_url = input("\n🔍 소스 S3 버킷 URL을 입력하세요 (예: s3://my-bucket/path/): ").strip()
            if source_bucket_url:
                try:
                    source_bucket, source_prefix = parse_s3_url(source_bucket_url)
                    print(f"✅ 소스 버킷: {source_bucket}")
                    print(f"✅ 소스 경로: {source_prefix if source_prefix else '(루트)'}")
                    break
                except ValueError as e:
                    print(f"❌ 오류: {e}")
            else:
                print("❌ 소스 버킷 URL을 입력해주세요.")
        
        # 백업 버킷 입력
        while True:
            backup_bucket_url = input("\n💾 백업 S3 버킷 URL을 입력하세요 (예: s3://my-backup-bucket/path/): ").strip()
            if backup_bucket_url:
                try:
                    backup_bucket, backup_prefix = parse_s3_url(backup_bucket_url)
                    print(f"✅ 백업 버킷: {backup_bucket}")
                    print(f"✅ 백업 경로: {backup_prefix if backup_prefix else '(루트)'}")
                    break
                except ValueError as e:
                    print(f"❌ 오류: {e}")
            else:
                print("❌ 백업 버킷 URL을 입력해주세요.")
        
        # 비교 모드 선택
        print("\n📊 비교 모드를 선택하세요:")
        print("1. JSONL (JSON Lines) - 한 줄에 하나의 JSON 객체")
        print("2. Array - JSON 배열 형태")
        print("3. CSV - CSV 형태")
        
        while True:
            mode_choice = input("선택 (1-3, 기본값: 1): ").strip()
            if not mode_choice:
                compare_mode = "jsonl"
                break
            elif mode_choice in ["1", "2", "3"]:
                mode_map = {"1": "jsonl", "2": "array", "3": "csv"}
                compare_mode = mode_map[mode_choice]
                break
            else:
                print("❌ 1, 2, 3 중에서 선택해주세요.")
        
        print(f"✅ 비교 모드: {compare_mode}")
        
        # 청크 크기 입력
        while True:
            chunk_size_input = input("\n📦 청크 크기를 입력하세요 (기본값: 20000): ").strip()
            if not chunk_size_input:
                chunk_size = 20000
                break
            try:
                chunk_size = int(chunk_size_input)
                if chunk_size > 0:
                    break
                else:
                    print("❌ 청크 크기는 0보다 커야 합니다.")
            except ValueError:
                print("❌ 숫자를 입력해주세요.")
        
        print(f"✅ 청크 크기: {chunk_size}")
        
        # 리포트 파일 경로 입력
        report_path = input("\n📄 리포트 파일 경로를 입력하세요 (기본값: ./detailed_report.csv): ").strip()
        if not report_path:
            report_path = "./detailed_report.csv"
        
        print(f"✅ 리포트 경로: {report_path}")
        
        # 로그 레벨 선택
        print("\n📝 로그 레벨을 선택하세요:")
        print("1. DEBUG - 상세한 디버그 정보")
        print("2. INFO - 일반 정보 (기본값)")
        print("3. WARNING - 경고만")
        print("4. ERROR - 오류만")
        
        while True:
            log_choice = input("선택 (1-4, 기본값: 2): ").strip()
            if not log_choice:
                log_level = "INFO"
                break
            elif log_choice in ["1", "2", "3", "4"]:
                level_map = {"1": "DEBUG", "2": "INFO", "3": "WARNING", "4": "ERROR"}
                log_level = level_map[log_choice]
                break
            else:
                print("❌ 1, 2, 3, 4 중에서 선택해주세요.")
        
        print(f"✅ 로그 레벨: {log_level}")
        
        # 설정 확인
        print("\n" + "=" * 60)
        print("📋 입력된 설정을 확인해주세요:")
        print(f"🔍 소스: {source_bucket_url}")
        print(f"💾 백업: {backup_bucket_url}")
        print(f"📊 모드: {compare_mode}")
        print(f"📦 청크: {chunk_size}")
        print(f"📄 리포트: {report_path}")
        print(f"📝 로그: {log_level}")
        print("=" * 60)
        
        confirm = input("\n🚀 비교를 시작하시겠습니까? (y/N): ").strip().lower()
        if confirm not in ['y', 'yes']:
            print("❌ 비교가 취소되었습니다.")
            sys.exit(0)
        
        # 로그 설정
        logging.basicConfig(
            level=getattr(logging, log_level),
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        
        print("\n🚀 S3 JSON 데이터 비교를 시작합니다...")
        
        # 비교 프로그램 초기화
        comparer = S3JSONComparer(
            source_bucket=source_bucket,
            backup_bucket=backup_bucket,
            processes=1,  # 단일 프로세스 사용
            chunk_size=chunk_size
        )
        
        # 비교 실행
        success = comparer.compare_buckets(
            source_prefix=source_prefix,
            backup_prefix=backup_prefix,
            report_path=report_path
        )
        
        print("\n" + "=" * 60)
        if success:
            print("✅ 모든 데이터가 일치합니다!")
            sys.exit(0)
        else:
            print("❌ 데이터 불일치가 발견되었습니다. 리포트를 확인하세요.")
            sys.exit(1)
            
    except Exception as e:
        print(f"❌ 프로그램 실행 중 오류 발생: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main() 