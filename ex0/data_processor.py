from typing import Any
from abc import ABC, abstractmethod

class DataProcessor(ABC):
    def __init__(self) -> None:
        self._storage: list[str] = []
        self._total: int = 0  #pour le rang
    @abstractmethod
    def validate(self, data: Any)->bool:
        pass
    @abstractmethod
    def ingest(self, data: Any)->None:
        pass
    def output(self)->tuple[int, str]:
        if not self._storage:
            raise IndexError("No data left")
        rank = self._total - len(self._storage)
        data = self._storage.pop(0)
        return (rank, data)
class NumericProcessor(DataProcessor):
    def validate(self, data: Any) -> bool:
        if isinstance(data, (int, float)):
            return True
        if isinstance(data, list):
            return all(isinstance(item, (int, float)) for item in data)
        return False
    def ingest(self, data: int | float | list[int | float])->None:
        if not self.validate(data):
            raise ValueError("Improper numeric data")
        if isinstance(data, list):
            for _ in data:
                self._storage.append(str(_))
                self._total += 1
        else:
            self._storage.append(str(data))
            self._total += 1
class TextProcessor(DataProcessor):
   def validate(self, data: Any)->bool:
       if isinstance(data, str):
           return True
       if isinstance(data, list):
           return all(isinstance(item, (str)) for item in data)
       return False
   def ingest(self, data: str | list[str])->None:
       if not self.validate(data):
           raise ValueError("Improper text data")
       if isinstance(data, list):
           for _ in data:
               self._storage.append(_)
               self._total += 1
       else:
           self._storage.append(data)
           self._total += 1
class LogProcessor(DataProcessor):
    def validate(self, data: Any)->bool:
        if isinstance(data, dict):
            return all(isinstance(k, str) and isinstance(v, str) for k, v in data.items())
        if isinstance(data, list):
            return all(isinstance(item, dict) for item in data)
        return False
    def ingest(self, data: dict | list[dict]):
        if not self.validate(data):
            raise ValueError("Improper log type value")
        if isinstance(data, list):
            for _ in data:
                self._storage.append(f"{_['log_level']}: {_['log_message']}")
                self._total += 1
        else:
            self._storage.append(f"{data['log_level']}: {data['log_message']}")
            self._total += 1


if __name__ == "__main__":
    print("=== Code Nexus - Data Processor ===")

    print("\nTesting Numeric Processor...")
    num = NumericProcessor()
    print(f"Trying to validate input '42': {num.validate(42)}")
    print(f"Trying to validate input 'Hello': {num.validate('Hello')}")

    print("Test invalid ingestion of string 'foo' without prior validation:")
    try:
        num.ingest("foo")  # type: ignore
    except ValueError as e:
        print(f"Got exception: {e}")

    num.ingest([1, 2, 3, 4, 5])
    print(f"Processing data: [1, 2, 3, 4, 5]")
    print("Extracting 3 values...")
    for _ in range(3):
        rank, val = num.output()
        print(f"Numeric value {rank}: {val}")

    print("\nTesting Text Processor...")
    txt = TextProcessor()
    print(f"Trying to validate input '42': {txt.validate(42)}")

    txt.ingest(['Hello', 'Nexus', 'World'])
    print(f"Processing data: ['Hello', 'Nexus', 'World']")
    print("Extracting 1 value...")
    rank, val = txt.output()
    print(f"Text value {rank}: {val}")

    print("\nTesting Log Processor...")
    log = LogProcessor()
    print(f"Trying to validate input 'Hello': {log.validate('Hello')}")

    logs = [
        {'log_level': 'NOTICE', 'log_message': 'Connection to server'},
        {'log_level': 'ERROR', 'log_message': 'Unauthorized access!!'}
    ]
    log.ingest(logs)
    print(f"Processing data: {logs}")
    print("Extracting 2 values...")
    for _ in range(2):
        rank, val = log.output()
        print(f"Log entry {rank}: {val}")
