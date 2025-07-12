#!/usr/bin/env python3
"""
S3 JSON 데이터 일치성 비교 프로그램

AWS S3 버킷에 저장된 대용량 JSON 데이터의 백업 무결성을 검증하는 프로그램
"""

import argparse
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
                stream = gzip.GzipFile(fileobj=stream)
            
            # JSON 처리 모드에 따라 다른 방식으로 처리
            for record in self.json_processor.process_stream(stream):
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
    
    def compare_buckets(self, source_prefix: str = "", backup_prefix: str = "",
                       report_path: str = "compare_report.csv") -> bool:
        """두 버킷의 모든 파일을 비교합니다"""
        self.logger.info("S3 버킷 비교 시작")
        
        # 파일 목록 가져오기
        source_files = set(self.get_file_list(self.source_bucket, source_prefix))
        backup_files = set(self.get_file_list(self.backup_bucket, backup_prefix))
        
        # 공통 파일 및 누락 파일 확인
        common_files = source_files.intersection(backup_files)
        missing_in_backup = source_files - backup_files
        missing_in_source = backup_files - source_files
        
        self.logger.info(f"공통 파일: {len(common_files)}")
        self.logger.info(f"백업에서 누락: {len(missing_in_backup)}")
        self.logger.info(f"소스에서 누락: {len(missing_in_source)}")
        
        # 누락 파일 리포트에 추가
        for file_path in missing_in_backup:
            result = CompareResult(
                file_path=file_path,
                source_records=0,
                backup_records=0,
                matched_records=0,
                mismatched_records=0,
                missing_in_backup=1,
                missing_in_source=0,
                errors=["백업 버킷에서 파일 누락"],
                processing_time=0
            )
            self.compare_results.append(result)
        
        for file_path in missing_in_source:
            result = CompareResult(
                file_path=file_path,
                source_records=0,
                backup_records=0,
                matched_records=0,
                mismatched_records=0,
                missing_in_backup=0,
                missing_in_source=1,
                errors=["소스 버킷에서 파일 누락"],
                processing_time=0
            )
            self.compare_results.append(result)
        
        # 공통 파일 비교 (멀티프로세싱)
        if common_files:
            self.logger.info("공통 파일 비교 시작")
            
            with concurrent.futures.ProcessPoolExecutor(
                max_workers=self.processes
            ) as executor:
                # 진행률 표시
                with tqdm(total=len(common_files), desc="파일 비교") as pbar:
                    future_to_file = {
                        executor.submit(self.compare_file_pair, file_path): file_path
                        for file_path in common_files
                    }
                    
                    for future in concurrent.futures.as_completed(future_to_file):
                        file_path = future_to_file[future]
                        try:
                            result = future.result()
                            self.compare_results.append(result)
                            pbar.set_postfix({
                                'current': file_path,
                                'matched': result.matched_records,
                                'mismatched': result.mismatched_records
                            })
                        except Exception as e:
                            self.logger.error(f"파일 비교 실패 ({file_path}): {e}")
                        
                        pbar.update(1)
        
        # 리포트 생성
        self.report_generator.generate_report(self.compare_results, report_path)
        
        # 결과 요약
        total_files = len(self.compare_results)
        total_matched = sum(r.matched_records for r in self.compare_results)
        total_mismatched = sum(r.mismatched_records for r in self.compare_results)
        
        self.logger.info(f"비교 완료: {total_files}개 파일")
        self.logger.info(f"일치하는 레코드: {total_matched}")
        self.logger.info(f"불일치하는 레코드: {total_mismatched}")
        
        return total_mismatched == 0


def main():
    """메인 함수"""
    parser = argparse.ArgumentParser(
        description="S3 JSON 데이터 일치성 비교 프로그램",
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    
    parser.add_argument(
        "--source-bucket",
        required=True,
        help="소스 S3 버킷 (예: s3://my-source-bucket/path)"
    )
    
    parser.add_argument(
        "--backup-bucket",
        required=True,
        help="백업 S3 버킷 (예: s3://my-backup-bucket/path)"
    )
    
    parser.add_argument(
        "--compare-mode",
        choices=["jsonl", "array", "csv"],
        default="jsonl",
        help="비교 모드 (기본값: jsonl)"
    )
    
    parser.add_argument(
        "--report",
        default="compare_report.csv",
        help="리포트 저장 경로 (기본값: compare_report.csv)"
    )
    
    parser.add_argument(
        "--processes",
        type=int,
        default=4,
        help="동시 처리 프로세스 수 (기본값: 4)"
    )
    
    parser.add_argument(
        "--chunk-size",
        type=int,
        default=10000,
        help="청크 크기 (기본값: 10000)"
    )
    
    parser.add_argument(
        "--log-level",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        default="INFO",
        help="로그 레벨 (기본값: INFO)"
    )
    
    args = parser.parse_args()
    
    # 로그 설정
    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # S3 URL 파싱
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
        # S3 URL 파싱
        source_bucket, source_prefix = parse_s3_url(args.source_bucket)
        backup_bucket, backup_prefix = parse_s3_url(args.backup_bucket)
        
        # 비교 프로그램 초기화
        comparer = S3JSONComparer(
            source_bucket=source_bucket,
            backup_bucket=backup_bucket,
            processes=args.processes,
            chunk_size=args.chunk_size
        )
        
        # 비교 실행
        success = comparer.compare_buckets(
            source_prefix=source_prefix,
            backup_prefix=backup_prefix,
            report_path=args.report
        )
        
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