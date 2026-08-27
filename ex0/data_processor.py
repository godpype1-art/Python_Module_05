from abc import ABC, abstractmethod
from typing import Any


class DataProcessor(ABC):

    def __init__(self) -> None:
        self._data: list[str] = []
        self._rank: int = 0

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
        else:
            self._data.append(str(data))


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
        else:
            self._data.append(str(data))


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
                    f"{item.get("log_level", "UKNOWN")}: {
                        item.get("log_message", "UKNOWN")}"
                    )
        else:
            self._data.append(
                f"{data.get("log_level", "UKNOWN")}: {
                    data.get("log_message", "UKNOWN")}"
                )


def main() -> None:
    print("=== Code Nexus - Data Processor ===")
    print()
    print("Testing Numeric Processor...")
    numeric: NumericProcessor = NumericProcessor()
    testing_data: Any = "42"
    is_valid: bool = numeric.validate(testing_data)
    print(f"Trying to validate testing_data '{testing_data}': {is_valid}")
    testing_data = "Hello"
    is_valid = numeric.validate(testing_data)
    print(f"Trying to validate testing_data '{testing_data}': {is_valid}")
    print(
        f"Test invalid ingestion of string '{testing_data}'"
        "without prior validation:")
    testing_data = "foo"
    try:
        numeric.ingest(testing_data)
    except TypeError as error:
        print(f"Got exception: {error}")
    testing_data = [1, 2, 3, 4, 5]
    numeric.ingest(testing_data)
    print("Extracting 3 values...")
    for i in range(0, 3):
        result: tuple[int, str] = numeric.output()
        rank, value = result
        print(f"Numeric value {rank}: {value}")
    print()
    print("Testing Text Processor...")
    text: TextProcessor = TextProcessor()
    testing_data = 42
    is_valid = text.validate(testing_data)
    print(f"Trying to validate testing_data '{testing_data}': {is_valid}")
    testing_data = ["Hello", "Nexus", "World"]
    try:
        text.ingest(testing_data)
    except TypeError as error:
        print(f"Got exception: {error}")
    print("Extracting 1 value...")
    result = text.output()
    rank, value = result
    print(f"Text value {rank}: {value}")
    print()
    print("Testing Log Processor...")
    logs: LogProcessor = LogProcessor()
    testing_data = "Hello"
    is_valid = logs.validate(testing_data)
    print(f"Trying to validate testing_data '{testing_data}': {is_valid}")
    testing_data = [
        {
            "log_level": "NOTICE",
            "log_message": "Connection to server"
        },
        {
            "log_level": "Error",
            "log_message": "Unauthorized access!!"
        }
    ]
    try:
        logs.ingest(testing_data)
    except TypeError as error:
        print(f"Got exception: {error}")
    print("Extracting 2 values...")
    for i in range(0, 2):
        result = logs.output()
        rank, value = result
        print(f"Log value {rank}: {value}")


if __name__ == "__main__":
    main()
