"""
JSON 처리기 모듈

대용량 JSON 파일을 메모리 효율적으로 처리하는 클래스
"""

import json
import logging
from typing import Any, Dict, Generator, List, TextIO, Union

import ijson


class JSONProcessor:
    """JSON 데이터를 처리하는 클래스"""
    
    def __init__(self, chunk_size: int = 10000):
        """
        JSONProcessor 초기화
        
        Args:
            chunk_size: 청크 크기 (레코드 수)
        """
        self.chunk_size = chunk_size
        self.logger = logging.getLogger(__name__)
    
    def process_stream(self, stream: TextIO, 
                      mode: str = "jsonl") -> Generator[Dict[str, Any], None, None]:
        """
        스트림에서 JSON 레코드를 읽어 처리합니다
        
        Args:
            stream: 입력 스트림
            mode: 처리 모드 ("jsonl", "array", "single")
            
        Yields:
            JSON 레코드
        """
        try:
            if mode == "jsonl":
                yield from self._process_jsonl_stream(stream)
            elif mode == "array":
                yield from self._process_array_stream(stream)
            elif mode == "single":
                yield from self._process_single_stream(stream)
            else:
                raise ValueError(f"지원하지 않는 모드: {mode}")
                
        except Exception as e:
            self.logger.error(f"JSON 스트림 처리 중 오류: {e}")
            raise
    
    def _process_jsonl_stream(self, stream: TextIO) -> Generator[Dict[str, Any], None, None]:
        """
        JSONL (JSON Lines) 형식의 스트림을 처리합니다
        
        Args:
            stream: 입력 스트림
            
        Yields:
            JSON 레코드
        """
        line_count = 0
        
        for line in stream:
            line = line.strip()
            if not line:
                continue
                
            try:
                record = json.loads(line)
                yield record
                line_count += 1
                
                # 메모리 효율을 위해 주기적으로 로그 출력
                if line_count % self.chunk_size == 0:
                    self.logger.debug(f"JSONL 레코드 {line_count}개 처리됨")
                    
            except json.JSONDecodeError as e:
                self.logger.warning(f"JSON 파싱 오류 (line {line_count + 1}): {e}")
                continue
    
    def _process_array_stream(self, stream: TextIO) -> Generator[Dict[str, Any], None, None]:
        """
        JSON 배열 형식의 스트림을 처리합니다
        
        Args:
            stream: 입력 스트림
            
        Yields:
            JSON 레코드
        """
        try:
            # ijson을 사용하여 메모리 효율적으로 배열 처리
            parser = ijson.parse(stream)
            
            record_count = 0
            current_record = {}
            in_array = False
            current_key = None
            
            for prefix, event, value in parser:
                if prefix == '' and event == 'start_array':
                    in_array = True
                    continue
                elif prefix == '' and event == 'end_array':
                    in_array = False
                    break
                
                if in_array:
                    if event == 'start_map':
                        current_record = {}
                    elif event == 'end_map':
                        if current_record:
                            yield current_record
                            record_count += 1
                            
                            # 메모리 효율을 위해 주기적으로 로그 출력
                            if record_count % self.chunk_size == 0:
                                self.logger.debug(f"배열 레코드 {record_count}개 처리됨")
                                
                    elif event == 'map_key':
                        current_key = value
                    elif event in ['string', 'number', 'boolean', 'null']:
                        if current_key:
                            current_record[current_key] = value
                            current_key = None
                            
        except Exception as e:
            self.logger.error(f"JSON 배열 처리 중 오류: {e}")
            raise
    
    def _process_single_stream(self, stream: TextIO) -> Generator[Dict[str, Any], None, None]:
        """
        단일 JSON 객체 형식의 스트림을 처리합니다
        
        Args:
            stream: 입력 스트림
            
        Yields:
            JSON 레코드
        """
        try:
            # 전체 JSON을 메모리에 로드
            content = stream.read()
            data = json.loads(content)
            
            if isinstance(data, dict):
                yield data
            elif isinstance(data, list):
                for item in data:
                    if isinstance(item, dict):
                        yield item
            else:
                self.logger.warning(f"지원하지 않는 JSON 형식: {type(data)}")
                
        except json.JSONDecodeError as e:
            self.logger.error(f"JSON 파싱 오류: {e}")
            raise
        except Exception as e:
            self.logger.error(f"단일 JSON 처리 중 오류: {e}")
            raise
    
    def process_file_chunks(self, file_path: str, 
                          mode: str = "jsonl") -> Generator[List[Dict[str, Any]], None, None]:
        """
        파일을 청크 단위로 처리합니다
        
        Args:
            file_path: 파일 경로
            mode: 처리 모드
            
        Yields:
            레코드 청크 (리스트)
        """
        try:
            with open(file_path, 'r', encoding='utf-8') as file:
                chunk = []
                
                for record in self.process_stream(file, mode):
                    chunk.append(record)
                    
                    if len(chunk) >= self.chunk_size:
                        yield chunk
                        chunk = []
                
                # 남은 레코드 처리
                if chunk:
                    yield chunk
                    
        except Exception as e:
            self.logger.error(f"파일 청크 처리 중 오류 ({file_path}): {e}")
            raise
    
    def validate_json_structure(self, data: Union[Dict, List]) -> bool:
        """
        JSON 구조가 유효한지 검증합니다
        
        Args:
            data: 검증할 데이터
            
        Returns:
            유효성 여부
        """
        try:
            # JSON 직렬화/역직렬화 테스트
            json_str = json.dumps(data, ensure_ascii=False)
            parsed_data = json.loads(json_str)
            
            return isinstance(parsed_data, (dict, list))
            
        except (TypeError, json.JSONDecodeError):
            return False
    
    def normalize_json_keys(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """
        JSON 키를 정규화합니다 (정렬)
        
        Args:
            data: 정규화할 데이터
            
        Returns:
            정규화된 데이터
        """
        if isinstance(data, dict):
            return {k: self.normalize_json_keys(v) for k, v in sorted(data.items())}
        elif isinstance(data, list):
            return [self.normalize_json_keys(item) for item in data]
        else:
            return data
    
    def estimate_memory_usage(self, data: Union[Dict, List]) -> int:
        """
        데이터의 메모리 사용량을 추정합니다
        
        Args:
            data: 추정할 데이터
            
        Returns:
            추정 메모리 사용량 (바이트)
        """
        try:
            json_str = json.dumps(data, ensure_ascii=False)
            return len(json_str.encode('utf-8'))
        except Exception:
            return 0 