from __future__ import annotations
from typing import Any, Protocol
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

    def output_pipeline(self, nb: int, plugin: ExportPlugin) -> None:
        for proc in self._processors:
            collected: list[tuple[int, str]] = []
            for _ in range(nb):
                try:
                    collected.append(proc.output())
                except IndexError:
                    break
            if collected:
                plugin.process_output(collected)

    def print_processors_stats(self) -> None:
        print("== DataStream statistics ==")
        if not self._processors:
            print("No processor found, no data")
            return
        for proc in self._processors:
            name = proc.__class__.__name__.replace("Processor", " Processor")
            print(f"{name}: total {proc._total} "
                  f"items processed, remaining {len(proc._storage)} on processor")


class ExportPlugin(Protocol):
    def process_output(self, data: list[tuple[int, str]]) -> None:
        ...


class CSVExportPlugin:
    def process_output(self, data: list[tuple[int, str]]) -> None:
        print("CSV Output:")
        print(",".join(value for _, value in data))


class JSONExportPlugin:
    def process_output(self, data: list[tuple[int, str]]) -> None:
        print("JSON Output:")
        pairs = []
        for rank, value in data:
            escaped = value.replace("\\", "\\\\").replace('"', '\\"')
            pairs.append(f'"item_{rank}": "{escaped}"')
        print("{" + ", ".join(pairs) + "}")


if __name__ == "__main__":
    print("=== Code Nexus - Data Pipeline ===")
    print()
    print("Initialize Data Stream...")
    print()

    stream = DataStream()
    stream.print_processors_stats()
    print()

    print("Registering Processors")
    stream.register_processor(NumericProcessor())
    stream.register_processor(TextProcessor())
    stream.register_processor(LogProcessor())
    print()

    batch1: list[Any] = [
        "Hello world",
        [3.14, -1, 2.71],
        [
            {
                "log_level": "WARNING",
                "log_message": "Telnet access! Use ssh instead",
            },
            {"log_level": "INFO", "log_message": "User wil is connected"},
        ],
        42,
        ["Hi", "five"],
    ]
    print(f"Send first batch of data on stream: {batch1}")
    stream.process_stream(batch1)
    print()
    stream.print_processors_stats()
    print()

    print("Send 3 processed data from each processor to a CSV plugin:")
    stream.output_pipeline(3, CSVExportPlugin())
    print()
    stream.print_processors_stats()
    print()

    batch2: list[Any] = [
        21,
        ["I love AI", "LLMs are wonderful", "Stay healthy"],
        [
            {"log_level": "ERROR", "log_message": "500 server crash"},
            {
                "log_level": "NOTICE",
                "log_message": "Certificate expires in 10 days",
            },
        ],
        [32, 42, 64, 84, 128, 168],
        "World hello",
    ]
    print(f"Send another batch of data: {batch2}")
    stream.process_stream(batch2)
    print()
    stream.print_processors_stats()
    print()

    print("Send 5 processed data from each processor to a JSON plugin:")
    stream.output_pipeline(5, JSONExportPlugin())
    print()
    stream.print_processors_stats()
