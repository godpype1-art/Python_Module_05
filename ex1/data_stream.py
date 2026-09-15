from abc import ABC, abstractmethod
from typing import Any


class DataError(Exception):
    ...


class DataProcessor(ABC):

    def __init__(self) -> None:
        self._data: list[str] = []
        self._rank: int = 0
        self._processed: int = 0

    @abstractmethod
    def validate(self, data: Any) -> bool:
        ...

    @abstractmethod
    def ingest(self, data: Any) -> None:
        ...

    def output(self) -> tuple[int, str]:
        if not self._data:
            raise IndexError("No data left")
        value: str = self._data.pop(0)
        rank = self._rank
        self._rank += 1
        return (rank, value)


class NumericProcessor(DataProcessor):

    def validate(self, data: Any) -> bool:
        if isinstance(data, (int, float)):
            return True
        elif isinstance(data, list):
            return all(isinstance(item, (int, float)) for item in data)
        return False

    def ingest(self, data: int | float | list[int | float]) -> None:
        if not self.validate(data):
            raise TypeError("Improper numeric data")
        print(f"Processing data: {data}")
        if isinstance(data, list):
            for item in data:
                self._data.append(str(item))
                self._processed += 1
        else:
            self._data.append(str(data))
            self._processed += 1


class TextProcessor(DataProcessor):

    def validate(self, data: Any) -> bool:
        if isinstance(data, str):
            return True
        elif isinstance(data, list):
            return all(isinstance(item, str) for item in data)
        return False

    def ingest(self, data: str | list[str]) -> None:
        if not self.validate(data):
            raise TypeError("Improper text data")
        print(f"Processing data: {data}")
        if isinstance(data, list):
            for item in data:
                self._data.append(str(item))
                self._processed += 1
        else:
            self._data.append(str(data))
            self._processed += 1


class LogProcessor(DataProcessor):

    def validate(self, data: Any) -> bool:
        if isinstance(data, dict):
            return all(
                isinstance(key, str)
                and isinstance(value, str)
                for key, value in data.items()
                )
        elif isinstance(data, list):
            return all(
                isinstance(item, dict)
                and all(
                    isinstance(key, str) and isinstance(value, str)
                    for key, value in item.items()
                )
                for item in data)
        return False

    def ingest(self, data: dict[str, str] | list[dict[str, str]]) -> None:
        if not self.validate(data):
            raise TypeError("Improper log data")
        print(f"Processing data: {data}")
        if isinstance(data, list):
            for item in data:
                self._data.append(
                    f"{item.get('log_level', 'UNKNOWN')}: {
                        item.get('log_message', 'UNKNOWN')}"
                    )
                self._processed += 1
        else:
            self._data.append(
                f"{data.get('log_level', 'UNKNOWN')}: {
                    data.get('log_message', 'UNKNOWN')}"
                )
            self._processed += 1


class DataStream():
    def __init__(self):
        self._processors: list[DataProcessor] = []

    def register_processor(self, proc: DataProcessor) -> None:
        self._processors.append(proc)

    def process_stream(self, stream: list[Any]) -> None:
        for data in stream:
            valid: bool = False
            try:
                for processor in self._processors:
                    if processor.validate(data):
                        valid = True
                        processor.ingest(data)
                        continue
                if not valid:
                    raise DataError(f"Can't process element in stream: {data}")
            except DataError as error:
                print(f"DataStream error - {error}")

    def print_processors_stats(self) -> None:
        print("=== DataStream Statistics ===")
        if not self._processors:
            print("No processor found, no data")
        else:
            for processor in self._processors:
                print(f"{processor.__class__.__name__}: total {processor._processed} items processed, remaning {len(processor._data)} on processor")


def main() -> None:
    print("=== Code Nexus - Data stream ===")
    print()
    print("Inicialize Data stream...")
    stream: DataStream = DataStream()
    stream.print_processors_stats()
    print()
    print("Registering Numeric Processor")
    numeric: NumericProcessor = NumericProcessor()
    stream.register_processor(numeric)
    print()
    testing_data: list[Any] = ['Hello world', [3.14, -1, 2.71], [{'log_level': 'WARNING', 'log_message': 'Telnet access! Use ssh instead'}, {'log_level': 'INFO', 'log_message': 'User wil is connected'}], 42, ['Hi', 'five']]
    print(f"Send first batch of data on stream: {testing_data}")
    stream.process_stream(testing_data)
    stream.print_processors_stats()
    print()
    print("Registering other data processors")
    text: TextProcessor = TextProcessor()
    stream.register_processor(text)
    logs: LogProcessor = LogProcessor()
    stream.register_processor(logs)
    print("Send the same batch again")
    stream.process_stream(testing_data)
    stream.print_processors_stats()
    print()
    print("Consume some elements from the data processors: Numeric 3, Text 2, Log 1")
    numeric.output()
    numeric.output()
    numeric.output()
    text.output()
    text.output()
    logs.output()
    stream.print_processors_stats()

if __name__ == "__main__":
    main()
