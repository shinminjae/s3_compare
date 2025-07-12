"""
리포트 생성기 모듈

비교 결과를 다양한 형식으로 저장하는 클래스
"""

import csv
import json
import logging
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Any

import pandas as pd


class ReportGenerator:
    """비교 결과 리포트를 생성하는 클래스"""
    
    def __init__(self):
        """ReportGenerator 초기화"""
        self.logger = logging.getLogger(__name__)
    
    def generate_report(self, results: List[Any], 
                       output_path: str = "compare_report.csv",
                       format_type: str = "csv") -> bool:
        """
        비교 결과 리포트를 생성합니다
        
        Args:
            results: 비교 결과 리스트
            output_path: 출력 파일 경로
            format_type: 리포트 형식 ("csv", "json", "excel")
            
        Returns:
            성공 여부
        """
        try:
            if format_type.lower() == "csv":
                return self._generate_csv_report(results, output_path)
            elif format_type.lower() == "json":
                return self._generate_json_report(results, output_path)
            elif format_type.lower() == "excel":
                return self._generate_excel_report(results, output_path)
            else:
                raise ValueError(f"지원하지 않는 형식: {format_type}")
                
        except Exception as e:
            self.logger.error(f"리포트 생성 실패: {e}")
            return False
    
    def _generate_csv_report(self, results: List[Any], 
                           output_path: str) -> bool:
        """
        CSV 형식의 리포트를 생성합니다
        
        Args:
            results: 비교 결과 리스트
            output_path: 출력 파일 경로
            
        Returns:
            성공 여부
        """
        try:
            # 결과 데이터를 DataFrame으로 변환
            data = []
            for result in results:
                row = {
                    'file_path': result.file_path,
                    'source_records': result.source_records,
                    'backup_records': result.backup_records,
                    'matched_records': result.matched_records,
                    'mismatched_records': result.mismatched_records,
                    'missing_in_backup': result.missing_in_backup,
                    'missing_in_source': result.missing_in_source,
                    'errors': '; '.join(result.errors) if result.errors else '',
                    'processing_time': round(result.processing_time, 2),
                    'match_rate': self._calculate_match_rate(result)
                }
                data.append(row)
            
            # DataFrame 생성 및 CSV 저장 (기존 파일에 추가)
            df = pd.DataFrame(data)
            
            # 기존 파일이 있으면 헤더 없이 추가, 없으면 헤더 포함하여 생성
            if Path(output_path).exists():
                df.to_csv(output_path, mode='a', header=False, index=False, encoding='utf-8-sig')
                self.logger.info(f"기존 파일에 {len(data)}개 레코드 추가: {output_path}")
            else:
                df.to_csv(output_path, index=False, encoding='utf-8-sig')
                self.logger.info(f"새 파일 생성: {output_path}")
            
            # 요약 정보 생성
            summary_path = output_path.replace('.csv', '_summary.csv')
            self._generate_summary_report(results, summary_path)
            
            self.logger.info(f"CSV 리포트 생성 완료: {output_path}")
            return True
            
        except Exception as e:
            self.logger.error(f"CSV 리포트 생성 실패: {e}")
            return False
    
    def _generate_json_report(self, results: List[Any], 
                            output_path: str) -> bool:
        """
        JSON 형식의 리포트를 생성합니다
        
        Args:
            results: 비교 결과 리스트
            output_path: 출력 파일 경로
            
        Returns:
            성공 여부
        """
        try:
            # 결과 데이터를 딕셔너리로 변환
            report_data = {
                'metadata': {
                    'generated_at': datetime.now().isoformat(),
                    'total_files': len(results),
                    'total_source_records': sum(r.source_records for r in results),
                    'total_backup_records': sum(r.backup_records for r in results),
                    'total_matched_records': sum(r.matched_records for r in results),
                    'total_mismatched_records': sum(r.mismatched_records for r in results),
                    'total_processing_time': sum(r.processing_time for r in results)
                },
                'results': []
            }
            
            for result in results:
                result_data = {
                    'file_path': result.file_path,
                    'source_records': result.source_records,
                    'backup_records': result.backup_records,
                    'matched_records': result.matched_records,
                    'mismatched_records': result.mismatched_records,
                    'missing_in_backup': result.missing_in_backup,
                    'missing_in_source': result.missing_in_source,
                    'errors': result.errors,
                    'processing_time': result.processing_time,
                    'match_rate': self._calculate_match_rate(result)
                }
                report_data['results'].append(result_data)
            
            # JSON 파일 저장
            with open(output_path, 'w', encoding='utf-8') as f:
                json.dump(report_data, f, ensure_ascii=False, indent=2)
            
            self.logger.info(f"JSON 리포트 생성 완료: {output_path}")
            return True
            
        except Exception as e:
            self.logger.error(f"JSON 리포트 생성 실패: {e}")
            return False
    
    def _generate_excel_report(self, results: List[Any], 
                             output_path: str) -> bool:
        """
        Excel 형식의 리포트를 생성합니다
        
        Args:
            results: 비교 결과 리스트
            output_path: 출력 파일 경로
            
        Returns:
            성공 여부
        """
        try:
            # 결과 데이터를 DataFrame으로 변환
            data = []
            for result in results:
                row = {
                    'file_path': result.file_path,
                    'source_records': result.source_records,
                    'backup_records': result.backup_records,
                    'matched_records': result.matched_records,
                    'mismatched_records': result.mismatched_records,
                    'missing_in_backup': result.missing_in_backup,
                    'missing_in_source': result.missing_in_source,
                    'errors': '; '.join(result.errors) if result.errors else '',
                    'processing_time': round(result.processing_time, 2),
                    'match_rate': self._calculate_match_rate(result)
                }
                data.append(row)
            
            # DataFrame 생성 및 Excel 저장
            df = pd.DataFrame(data)
            
            with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
                # 세부 결과 시트
                df.to_excel(writer, sheet_name='상세결과', index=False)
                
                # 요약 시트
                summary_df = self._generate_summary_dataframe(results)
                summary_df.to_excel(writer, sheet_name='요약', index=False)
                
                # 오류 시트
                error_df = self._generate_error_dataframe(results)
                if not error_df.empty:
                    error_df.to_excel(writer, sheet_name='오류', index=False)
            
            self.logger.info(f"Excel 리포트 생성 완료: {output_path}")
            return True
            
        except Exception as e:
            self.logger.error(f"Excel 리포트 생성 실패: {e}")
            return False
    
    def _generate_summary_report(self, results: List[Any], 
                               output_path: str) -> bool:
        """
        요약 리포트를 생성합니다
        
        Args:
            results: 비교 결과 리스트
            output_path: 출력 파일 경로
            
        Returns:
            성공 여부
        """
        try:
            summary_data = {
                'Generated At': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                'Total Files': len(results),
                'Total Source Records': sum(r.source_records for r in results),
                'Total Backup Records': sum(r.backup_records for r in results),
                'Total Matched Records': sum(r.matched_records for r in results),
                'Total Mismatched Records': sum(r.mismatched_records for r in results),
                'Files with Errors': len([r for r in results if r.errors]),
                'Total Processing Time (seconds)': round(sum(r.processing_time for r in results), 2),
                'Average Match Rate (%)': round(
                    sum(self._calculate_match_rate(r) for r in results) / len(results) if results else 0, 2
                )
            }
            
            # CSV 형식으로 저장
            with open(output_path, 'w', newline='', encoding='utf-8-sig') as f:
                writer = csv.writer(f)
                writer.writerow(['Metric', 'Value'])
                for key, value in summary_data.items():
                    writer.writerow([key, value])
            
            self.logger.info(f"요약 리포트 생성 완료: {output_path}")
            return True
            
        except Exception as e:
            self.logger.error(f"요약 리포트 생성 실패: {e}")
            return False
    
    def _generate_summary_dataframe(self, results: List[Any]) -> pd.DataFrame:
        """
        요약 데이터를 DataFrame으로 생성합니다
        
        Args:
            results: 비교 결과 리스트
            
        Returns:
            요약 DataFrame
        """
        summary_data = {
            'Metric': [
                'Total Files',
                'Total Source Records',
                'Total Backup Records',
                'Total Matched Records',
                'Total Mismatched Records',
                'Files with Errors',
                'Total Processing Time (seconds)',
                'Average Match Rate (%)'
            ],
            'Value': [
                len(results),
                sum(r.source_records for r in results),
                sum(r.backup_records for r in results),
                sum(r.matched_records for r in results),
                sum(r.mismatched_records for r in results),
                len([r for r in results if r.errors]),
                round(sum(r.processing_time for r in results), 2),
                round(sum(self._calculate_match_rate(r) for r in results) / len(results) if results else 0, 2)
            ]
        }
        
        return pd.DataFrame(summary_data)
    
    def _generate_error_dataframe(self, results: List[Any]) -> pd.DataFrame:
        """
        오류 데이터를 DataFrame으로 생성합니다
        
        Args:
            results: 비교 결과 리스트
            
        Returns:
            오류 DataFrame
        """
        error_data = []
        
        for result in results:
            if result.errors:
                for error in result.errors:
                    error_data.append({
                        'file_path': result.file_path,
                        'error_message': error,
                        'processing_time': result.processing_time
                    })
        
        return pd.DataFrame(error_data)
    
    def _calculate_match_rate(self, result: Any) -> float:
        """
        일치율을 계산합니다
        
        Args:
            result: 비교 결과
            
        Returns:
            일치율 (%)
        """
        total_records = max(result.source_records, result.backup_records)
        if total_records == 0:
            return 0.0
        
        return round((result.matched_records / total_records) * 100, 2)
    
    def generate_detailed_mismatch_report(self, results: List[Any], 
                                        output_path: str) -> bool:
        """
        상세 불일치 리포트를 생성합니다
        
        Args:
            results: 비교 결과 리스트
            output_path: 출력 파일 경로
            
        Returns:
            성공 여부
        """
        try:
            mismatched_files = [r for r in results if r.mismatched_records > 0]
            
            if not mismatched_files:
                self.logger.info("불일치 파일이 없습니다.")
                return True
            
            # 불일치 파일 정보 저장
            data = []
            for result in mismatched_files:
                row = {
                    'file_path': result.file_path,
                    'source_records': result.source_records,
                    'backup_records': result.backup_records,
                    'missing_in_backup': result.missing_in_backup,
                    'missing_in_source': result.missing_in_source,
                    'match_rate': self._calculate_match_rate(result),
                    'errors': '; '.join(result.errors) if result.errors else ''
                }
                data.append(row)
            
            # DataFrame 생성 및 저장 (기존 파일에 추가)
            df = pd.DataFrame(data)
            
            # 기존 파일이 있으면 헤더 없이 추가, 없으면 헤더 포함하여 생성
            if Path(output_path).exists():
                df.to_csv(output_path, mode='a', header=False, index=False, encoding='utf-8-sig')
                self.logger.info(f"기존 파일에 {len(data)}개 불일치 파일 추가: {output_path}")
            else:
                df.to_csv(output_path, index=False, encoding='utf-8-sig')
                self.logger.info(f"새 불일치 파일 리포트 생성: {output_path}")
            
            self.logger.info(f"상세 불일치 리포트 생성 완료: {output_path}")
            return True
            
        except Exception as e:
            self.logger.error(f"상세 불일치 리포트 생성 실패: {e}")
            return False
    
    def generate_detailed_mismatch_report_with_json(self, mismatched_records: List[Dict], 
                                                   output_path: str) -> bool:
        """
        불일치하는 레코드의 JSON 내역을 포함한 상세 리포트를 생성합니다
        
        Args:
            mismatched_records: 불일치하는 레코드 정보 리스트
            output_path: 출력 파일 경로
            
        Returns:
            성공 여부
        """
        try:
            if not mismatched_records:
                self.logger.info("불일치 레코드가 없습니다.")
                return True
            
            # 불일치 레코드 정보를 DataFrame으로 변환
            data = []
            for record in mismatched_records:
                row = {
                    'hash': record['hash'],
                    'hash_short': record['hash_short'],
                    'file_path': record['file_path'],
                    'bucket_type': record['bucket_type'],
                    'json_content': record['json_content']
                }
                data.append(row)
            
            # DataFrame 생성 및 CSV 저장 (기존 파일에 추가)
            df = pd.DataFrame(data)
            
            # 기존 파일이 있으면 헤더 없이 추가, 없으면 헤더 포함하여 생성
            if Path(output_path).exists():
                df.to_csv(output_path, mode='a', header=False, index=False, encoding='utf-8-sig')
                self.logger.info(f"기존 파일에 {len(data)}개 불일치 레코드 추가: {output_path}")
            else:
                df.to_csv(output_path, index=False, encoding='utf-8-sig')
                self.logger.info(f"새 불일치 레코드 파일 생성: {output_path}")
            
            self.logger.info(f"불일치 레코드 JSON 상세 리포트 생성 완료: {output_path}")
            self.logger.info(f"총 {len(mismatched_records)}개의 불일치 레코드가 기록되었습니다.")
            return True
            
        except Exception as e:
            self.logger.error(f"불일치 레코드 JSON 상세 리포트 생성 실패: {e}")
            return False 