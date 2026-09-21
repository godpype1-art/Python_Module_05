from abc import ABC, abstractmethod
from typing import Protocol, Any


class DataError(Exception):
    ...


class ExportPlugin(Protocol):
    def process_output(self, data: list[tuple[int, str]]) -> None:
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

    def __str__(self) -> str:
        result: str = ""
        for i, char in enumerate(self.__class__.__name__):
            if char.isupper() and i != 0:
                result += " "
            result += char
        return result


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
    def __init__(self) -> None:
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
                        break
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
                print(f"{processor}: total "
                      f"{processor._processed} items processed, "
                      f"remaining {len(processor._data)} on processor")

    def output_pipeline(self, nb: int, plugin: ExportPlugin) -> None:
        if not nb:
            print("Specify how much data to export!")
            return
        for processor in self._processors:
            result: list[tuple[int, str]] = []
            try:
                for i in range(1, nb + 1):
                    result.append(processor.output())
            except IndexError:
                pass
            plugin.process_output(result)


class CSVExportPlugin():
    def process_output(self, data: list[tuple[int, str]]) -> None:
        if data:
            print("CSV Output:")
            print(",".join(value for i, value in data))


class JSONExportPlugin():
    def process_output(self, data: list[tuple[int, str]]) -> None:
        if data:
            print("JSON Output:")
            result: str = ", ".join(
                f"\"item_{i}\": \"{value}\"" for i, value in data
                )
            print(f"[{result}]")


def main() -> None:
    print("=== Code Nexus - Data Pipeline ===")
    print()
    print("Initialize Data stream...")
    stream: DataStream = DataStream()
    stream.print_processors_stats()
    print()
    print("Registering Processors")
    numeric: NumericProcessor = NumericProcessor()
    stream.register_processor(numeric)
    text: TextProcessor = TextProcessor()
    stream.register_processor(text)
    logs: LogProcessor = LogProcessor()
    stream.register_processor(logs)
    print()
    testing_data: list[Any] = [
                        'Hello world', [3.14, -1, 2.71],
                        [{'log_level': 'WARNING', 'log_message':
                          'Telnet access! Use ssh instead'},
                         {'log_level': 'INFO', 'log_message':
                          'User wil is connected'}],
                        42, ['Hi', 'five']
                        ]
    print(f"Send first batch of data on stream: {testing_data}")
    stream.process_stream(testing_data)
    stream.print_processors_stats()
    print()
    csv: CSVExportPlugin = CSVExportPlugin()
    json: JSONExportPlugin = JSONExportPlugin()
    print("Send 3 processed data to a CSV plugin:")
    stream.output_pipeline(3, csv)
    print()
    stream.print_processors_stats()
    print()
    testing_data = [
        21, ['I love AI', 'LLMs are wonderful', 'Stay healthy'],
        [{'log_level': 'ERROR', 'log_message': '500 server crash'},
         {'log_level': 'NOTICE', 'log_message':
         'Certificate expires in 10 days'}],
        [32, 42, 64, 84, 128, 168], 'World hello'
        ]
    print(f"Send another batch of data: {testing_data}")
    print()
    stream.process_stream(testing_data)
    stream.print_processors_stats()
    print()
    print("Send 5 processed data to a JSON plugin:")
    stream.output_pipeline(5, json)
    print()
    stream.print_processors_stats()


if __name__ == "__main__":
    main()
