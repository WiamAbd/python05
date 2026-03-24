from abc import ABC, abstractmethod
from typing import Any, List, Tuple


class DataProcessor(ABC):

    @abstractmethod
    def process(self, data: Any) -> str:
        pass

    @abstractmethod
    def validate(self, data: Any) -> bool:
        pass

    def format_output(self, result: str) -> str:
        return f"Output: {result}"


class NumericProcessor(DataProcessor):

    def validate(self, data: Any) -> bool:
        if type(data) is not list:
            return False
        for x in data:
            if type(x) not in [int, float]:
                return False
        return True

    def process(self, data: Any) -> str:
        total: float = sum(data)
        avg: float = total / len(data)
        result: str = (
            f"Processed {len(data)} numeric values, "
            f"sum={total}, avg={avg}"
        )
        return result


class TextProcessor(DataProcessor):

    def validate(self, data: Any) -> bool:
        return type(data) is str

    def process(self, data: Any) -> str:
        length: int = len(data)
        words: int = len(data.split())
        result: str = (
            f"Processed text: {length} characters, {words} words"
        )
        return result


class LogProcessor(DataProcessor):

    def validate(self, data: Any) -> bool:
        return type(data) is str and ":" in data

    def process(self, data: Any) -> str:
        level: str
        message: str
        level, message = data.split(":", 1)
        result: str = (
            f"[ALERT] {level.strip()} level detected: "
            f"{message.strip()}"
        )
        return result


if __name__ == "__main__":
    print("=== CODE NEXUS - DATA PROCESSOR FOUNDATION ===\n")

    numeric: NumericProcessor = NumericProcessor()
    print("\nInitializing Numeric Processor...")
    data: List[int] = [1, 2, 3, 4, 5]
    print(f'Processing data: "{data}"')
    if numeric.validate(data):
        print("Validation: Numeric data verified")
    else:
        print("Validation failed")
    print(numeric.format_output(numeric.process(data)))

    # Text Processor
    text: TextProcessor = TextProcessor()
    print("\nInitializing Text Processor...")
    data = "Hello Nexus World"
    print(f'Processing data: "{data}"')
    if text.validate(data):
        print("Validation: Text data verified")
    else:
        print("Validation failed")
    print(text.format_output(text.process(data)))

    # Log Processor
    log: LogProcessor = LogProcessor()
    print("\nInitializing Log Processor...")
    data = "ERROR: Connection timeout"
    print(f'Processing data: "{data}"')
    if log.validate(data):
        print("Validation: Log entry verified")
    else:
        print("Validation failed")
    print(log.format_output(log.process(data)))

    print("\n=== Polymorphic Processing Demo ===")
    print("Processing multiple data types through same interface...")

    demo_processors: List[Tuple[DataProcessor, Any]] = [
        (NumericProcessor(), [1, 2, 3]),
        (TextProcessor(), "Hello World"),
        (LogProcessor(), "INFO: System ready"),
    ]

    for i, (p, d) in enumerate(demo_processors, 1):
        print(f"Result {i}: {p.process(d)}")

    print(
        "\nFoundation systems online. Nexus ready for advanced streams."
    )
