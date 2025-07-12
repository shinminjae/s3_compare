"""
로거 설정 모듈

애플리케이션의 로깅 설정을 담당하는 모듈
"""

import logging
import os
import time
from datetime import datetime
from logging.handlers import RotatingFileHandler
from pathlib import Path

import colorama
from colorama import Fore, Style


def setup_logger(name: str, log_level: str = "INFO", 
                log_dir: str = "logs") -> logging.Logger:
    """
    로거를 설정합니다
    
    Args:
        name: 로거 이름
        log_level: 로그 레벨
        log_dir: 로그 디렉터리
        
    Returns:
        설정된 로거
    """
    # 컬러 초기화
    colorama.init()
    
    # 로거 생성
    logger = logging.getLogger(name)
    logger.setLevel(getattr(logging, log_level.upper()))
    
    # 기존 핸들러 제거 (중복 방지)
    if logger.handlers:
        logger.handlers.clear()
    
    # 로그 디렉터리 생성
    log_path = Path(log_dir)
    log_path.mkdir(exist_ok=True)
    
    # 포맷터 생성
    formatter = logging.Formatter(
        fmt='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    
    # 콘솔 핸들러 생성
    console_handler = logging.StreamHandler()
    console_handler.setLevel(getattr(logging, log_level.upper()))
    console_handler.setFormatter(ColoredFormatter())
    
    # 파일 핸들러 생성 (회전 로그)
    log_file = log_path / f"{name}_{datetime.now().strftime('%Y%m%d')}.log"
    file_handler = RotatingFileHandler(
        log_file,
        maxBytes=10*1024*1024,  # 10MB
        backupCount=5,
        encoding='utf-8'
    )
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(formatter)
    
    # 오류 전용 파일 핸들러
    error_log_file = log_path / f"{name}_error_{datetime.now().strftime('%Y%m%d')}.log"
    error_handler = RotatingFileHandler(
        error_log_file,
        maxBytes=10*1024*1024,  # 10MB
        backupCount=5,
        encoding='utf-8'
    )
    error_handler.setLevel(logging.ERROR)
    error_handler.setFormatter(formatter)
    
    # 핸들러 추가
    logger.addHandler(console_handler)
    logger.addHandler(file_handler)
    logger.addHandler(error_handler)
    
    return logger


class ColoredFormatter(logging.Formatter):
    """컬러 포맷터 클래스"""
    
    COLORS = {
        'DEBUG': Fore.CYAN,
        'INFO': Fore.GREEN,
        'WARNING': Fore.YELLOW,
        'ERROR': Fore.RED,
        'CRITICAL': Fore.MAGENTA
    }
    
    def __init__(self):
        super().__init__(
            fmt='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
    
    def format(self, record):
        # 레벨에 따른 컬러 적용
        if record.levelname in self.COLORS:
            record.levelname = (
                self.COLORS[record.levelname] + 
                record.levelname + 
                Style.RESET_ALL
            )
        
        return super().format(record)


def get_logger(name: str) -> logging.Logger:
    """
    기존 로거를 반환하거나 새로 생성합니다
    
    Args:
        name: 로거 이름
        
    Returns:
        로거 인스턴스
    """
    return logging.getLogger(name)


def set_log_level(logger: logging.Logger, level: str):
    """
    로거의 레벨을 설정합니다
    
    Args:
        logger: 로거 인스턴스
        level: 로그 레벨
    """
    logger.setLevel(getattr(logging, level.upper()))
    
    # 핸들러 레벨도 변경
    for handler in logger.handlers:
        if isinstance(handler, logging.StreamHandler) and not isinstance(handler, RotatingFileHandler):
            handler.setLevel(getattr(logging, level.upper()))


def log_performance(logger: logging.Logger, operation: str, 
                   duration: float, details: str = ""):
    """
    성능 로그를 기록합니다
    
    Args:
        logger: 로거 인스턴스
        operation: 작업 이름
        duration: 소요 시간 (초)
        details: 추가 정보
    """
    message = f"Performance: {operation} took {duration:.2f} seconds"
    if details:
        message += f" - {details}"
    
    logger.info(message)


def log_memory_usage(logger: logging.Logger, operation: str, 
                    memory_mb: float):
    """
    메모리 사용량 로그를 기록합니다
    
    Args:
        logger: 로거 인스턴스
        operation: 작업 이름
        memory_mb: 메모리 사용량 (MB)
    """
    logger.info(f"Memory: {operation} used {memory_mb:.2f} MB")


def log_progress(logger: logging.Logger, current: int, 
                total: int, operation: str = "Processing"):
    """
    진행 상황 로그를 기록합니다
    
    Args:
        logger: 로거 인스턴스
        current: 현재 진행량
        total: 전체 작업량
        operation: 작업 이름
    """
    percentage = (current / total) * 100 if total > 0 else 0
    logger.info(f"{operation}: {current}/{total} ({percentage:.1f}%)")


def cleanup_old_logs(log_dir: str = "logs", days_to_keep: int = 30):
    """
    오래된 로그 파일을 정리합니다
    
    Args:
        log_dir: 로그 디렉터리
        days_to_keep: 보관할 일 수
    """
    import time
    
    log_path = Path(log_dir)
    if not log_path.exists():
        return
    
    cutoff_time = time.time() - (days_to_keep * 24 * 60 * 60)
    
    for log_file in log_path.glob("*.log*"):
        if log_file.stat().st_mtime < cutoff_time:
            try:
                log_file.unlink()
                print(f"Deleted old log file: {log_file}")
            except Exception as e:
                print(f"Failed to delete log file {log_file}: {e}")


class LogContext:
    """로그 컨텍스트 매니저"""
    
    def __init__(self, logger: logging.Logger, operation: str, 
                 log_start: bool = True, log_end: bool = True):
        self.logger = logger
        self.operation = operation
        self.log_start = log_start
        self.log_end = log_end
        self.start_time = None
    
    def __enter__(self):
        if self.log_start:
            self.logger.info(f"Started: {self.operation}")
        self.start_time = time.time()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        duration = time.time() - self.start_time if self.start_time else 0
        
        if exc_type is not None:
            self.logger.error(f"Failed: {self.operation} after {duration:.2f}s - {exc_val}")
        elif self.log_end:
            self.logger.info(f"Completed: {self.operation} in {duration:.2f}s") 