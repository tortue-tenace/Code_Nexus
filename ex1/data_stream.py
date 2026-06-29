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
class DataStream(DataProcessor):
    def __init__(self) -> None:
        super().__init__()
        self._processors: list[DataProcessor] = []

    def register_processor(self, proc: DataProcessor) -> None:
        self._processors.append(proc)

    def validate(self, data: Any) -> bool:
        return any(proc.validate(data) for proc in self._processors)

    def ingest(self, data: Any) -> None:
        for proc in self._processors:
            if proc.validate(data):
                proc.ingest(data)
                return
        raise ValueError(f"No processor can handle: {data}")
    def process_stream(self, stream: list[Any]) -> None:
        for value in stream:
            handled = False
            for proc in self._processors:
                if proc.validate(value):
                    proc.ingest(value)
                    handled = True
                    break                     
            if not handled:
                print(f"DataStream error - Can't process element in stream: {value}")

    def print_processors_stats(self) -> None:
        print("== DataStream statistics ==")
        if not self._processors:
            print("No processor found, no data")
            return
        for proc in self._processors:
            name = proc.__class__.__name__.replace("Processor", " Processor")
            print(f"{name}: total {proc._total} "
                  f"items processed, remaining {len(proc._storage)} on processor")


if __name__ == "__main__":
    print("=== Code Nexus - Data Stream ===")
    print("Initialize Data Stream...")
    ds = DataStream()
    ds.print_processors_stats()

    print("Registering Numeric Processor")
    ds.register_processor(NumericProcessor())

    batch = [
        'Hello world',
        [3.14, -1, 2.71],
        [{'log_level': 'WARNING', 'log_message': 'Telnet access! Use ssh instead'},
         {'log_level': 'INFO', 'log_message': 'User wil is connected'}],
        42,
        ['Hi', 'five'],
    ]
    print(f"Send first batch of data on stream: {batch}")
    ds.process_stream(batch)
    ds.print_processors_stats()

    print("Registering other data processors")
    ds.register_processor(TextProcessor())
    ds.register_processor(LogProcessor())

    print("Send the same batch again")
    ds.process_stream(batch)
    ds.print_processors_stats()

    print("Consume some elements from the data processors: Numeric 3, Text 2, Log 1")
    num_proc, text_proc, log_proc = ds._processors
    for _ in range(3):
        num_proc.output()
    for _ in range(2):
        text_proc.output()
    log_proc.output()

    ds.print_processors_stats()

