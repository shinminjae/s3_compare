"""
S3 핸들러 모듈

AWS S3와의 상호작용을 담당하는 클래스
"""

import io
import logging
from typing import Generator, List, Optional

import boto3
from botocore.exceptions import ClientError, NoCredentialsError


class S3Handler:
    """S3 작업을 처리하는 핸들러 클래스"""
    
    def __init__(self, aws_access_key_id: Optional[str] = None,
                 aws_secret_access_key: Optional[str] = None,
                 region_name: str = "us-east-1"):
        """
        S3Handler 초기화
        
        Args:
            aws_access_key_id: AWS 액세스 키 ID
            aws_secret_access_key: AWS 시크릿 액세스 키
            region_name: AWS 리전명
        """
        self.logger = logging.getLogger(__name__)
        
        # S3 클라이언트 초기화
        try:
            if aws_access_key_id and aws_secret_access_key:
                self.s3_client = boto3.client(
                    's3',
                    aws_access_key_id=aws_access_key_id,
                    aws_secret_access_key=aws_secret_access_key,
                    region_name=region_name
                )
            else:
                # 환경변수나 IAM 역할을 통한 인증
                self.s3_client = boto3.client('s3', region_name=region_name)
                
            # 연결 테스트
            self.s3_client.list_buckets()
            self.logger.info("S3 클라이언트 초기화 완료")
            
        except NoCredentialsError:
            self.logger.error("AWS 자격 증명을 찾을 수 없습니다")
            raise
        except Exception as e:
            self.logger.error(f"S3 클라이언트 초기화 실패: {e}")
            raise
    
    def list_files(self, bucket_name: str, prefix: str = "", 
                   suffix: str = "") -> List[str]:
        """
        S3 버킷에서 파일 목록을 가져옵니다
        
        Args:
            bucket_name: S3 버킷명
            prefix: 접두사 필터
            suffix: 접미사 필터
            
        Returns:
            파일 경로 목록
        """
        files = []
        
        try:
            paginator = self.s3_client.get_paginator('list_objects_v2')
            page_iterator = paginator.paginate(
                Bucket=bucket_name,
                Prefix=prefix
            )
            
            for page in page_iterator:
                if 'Contents' in page:
                    for obj in page['Contents']:
                        file_path = obj['Key']
                        
                        # 접미사 필터 적용
                        if suffix and not file_path.endswith(suffix):
                            continue
                            
                        # 디렉터리는 제외
                        if not file_path.endswith('/'):
                            files.append(file_path)
                            
        except ClientError as e:
            self.logger.error(f"파일 목록 가져오기 실패: {e}")
            raise
        
        self.logger.info(f"버킷 '{bucket_name}'에서 {len(files)}개 파일 발견")
        return files
    
    def get_file_stream(self, bucket_name: str, file_path: str) -> io.BytesIO:
        """
        S3 파일을 스트림으로 가져옵니다
        
        Args:
            bucket_name: S3 버킷명
            file_path: 파일 경로
            
        Returns:
            파일 스트림
        """
        try:
            response = self.s3_client.get_object(
                Bucket=bucket_name,
                Key=file_path
            )
            
            # 스트림 데이터 읽기
            file_stream = io.BytesIO(response['Body'].read())
            file_stream.seek(0)  # 스트림 포인터를 처음으로 이동
            
            return file_stream
            
        except ClientError as e:
            self.logger.error(f"파일 스트림 가져오기 실패 ({file_path}): {e}")
            raise
    
    def file_exists(self, bucket_name: str, file_path: str) -> bool:
        """
        S3 파일이 존재하는지 확인합니다
        
        Args:
            bucket_name: S3 버킷명
            file_path: 파일 경로
            
        Returns:
            파일 존재 여부
        """
        try:
            self.s3_client.head_object(Bucket=bucket_name, Key=file_path)
            return True
        except ClientError as e:
            if e.response['Error']['Code'] == '404':
                return False
            else:
                self.logger.error(f"파일 존재 확인 실패 ({file_path}): {e}")
                raise
    
    def get_file_size(self, bucket_name: str, file_path: str) -> int:
        """
        S3 파일의 크기를 가져옵니다
        
        Args:
            bucket_name: S3 버킷명
            file_path: 파일 경로
            
        Returns:
            파일 크기 (바이트)
        """
        try:
            response = self.s3_client.head_object(
                Bucket=bucket_name,
                Key=file_path
            )
            return response['ContentLength']
            
        except ClientError as e:
            self.logger.error(f"파일 크기 가져오기 실패 ({file_path}): {e}")
            raise
    
    def get_file_metadata(self, bucket_name: str, file_path: str) -> dict:
        """
        S3 파일의 메타데이터를 가져옵니다
        
        Args:
            bucket_name: S3 버킷명
            file_path: 파일 경로
            
        Returns:
            파일 메타데이터
        """
        try:
            response = self.s3_client.head_object(
                Bucket=bucket_name,
                Key=file_path
            )
            
            return {
                'size': response['ContentLength'],
                'last_modified': response['LastModified'],
                'etag': response['ETag'].strip('"'),
                'content_type': response.get('ContentType', ''),
                'metadata': response.get('Metadata', {})
            }
            
        except ClientError as e:
            self.logger.error(f"파일 메타데이터 가져오기 실패 ({file_path}): {e}")
            raise 