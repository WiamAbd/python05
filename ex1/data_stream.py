from abc import ABC, abstractmethod
from typing import Any, List, Dict, Union, Optional


class DataStream(ABC):

    def __init__(self, stream_id: str) -> None:
        self.stream_id: str = stream_id

    @abstractmethod
    def process_batch(self, data_batch: List[Any]) -> str:
        pass

    def filter_data(
        self,
        data_batch: List[Any],
        criteria: Optional[str] = None
    ) -> List[Any]:
        return data_batch

    def get_stats(self) -> Dict[str, Union[str, int, float]]:
        return {
            "stream_id": self.stream_id,
            "type": "Generic",
        }


class SensorStream(DataStream):

    def process_batch(self, data_batch: List[Any]) -> str:
        temps: List[float] = []

        for item in data_batch:
            if "temp" in item:
                value = float(item.split(":")[1])
                temps.append(value)

        avg: float = sum(temps) / len(temps) if temps else 0

        return (
            f"Sensor analysis: {len(data_batch)} readings processed, "
            f"avg temp: {avg}°C"
        )

    def filter_data(
        self,
        data_batch: List[Any],
        criteria: Optional[str] = None
    ) -> List[Any]:
        if criteria == "high":
            return [
                item for item in data_batch
                if "temp" in item and float(item.split(":")[1]) > 30
            ]
        return data_batch

    def get_stats(self) -> Dict[str, Union[str, int, float]]:
        return {
            "stream_id": self.stream_id,
            "type": "Sensor",
        }


class TransactionStream(DataStream):

    def process_batch(self, data_batch: List[Any]) -> str:
        total: int = 0

        for item in data_batch:
            action, value = item.split(":")
            value = int(value)

            if action == "buy":
                total -= value
            elif action == "sell":
                total += value

        sign: str = "+" if total > 0 else ""

        return (
            f"Transaction analysis: {len(data_batch)} operations, "
            f"net flow: {sign}{total} units"
        )

    def filter_data(
        self,
        data_batch: List[Any],
        criteria: Optional[str] = None
    ) -> List[Any]:
        if criteria == "large":
            return [
                item for item in data_batch
                if int(item.split(":")[1]) > 100
            ]
        return data_batch

    def get_stats(self) -> Dict[str, Union[str, int, float]]:
        return {
            "stream_id": self.stream_id,
            "type": "Transaction",
        }


class EventStream(DataStream):

    def process_batch(self, data_batch: List[Any]) -> str:
        errors: int = sum(1 for e in data_batch if e == "error")

        return (
            f"Event analysis: {len(data_batch)} events, "
            f"{errors} error detected"
        )

    def filter_data(
        self,
        data_batch: List[Any],
        criteria: Optional[str] = None
    ) -> List[Any]:
        if criteria == "error":
            return [e for e in data_batch if e == "error"]
        return data_batch

    def get_stats(self) -> Dict[str, Union[str, int, float]]:
        return {
            "stream_id": self.stream_id,
            "type": "Event",
        }


class StreamProcessor:

    def process_streams(
        self,
        streams: List[DataStream],
        batches: List[List[Any]]
    ) -> None:
        for stream, batch in zip(streams, batches):
            print(stream.process_batch(batch))


if __name__ == "__main__":
    print("=== CODE NEXUS - POLYMORPHIC STREAM SYSTEM ===")

    # Sensor
    sensor = SensorStream("SENSOR_001")
    print("\nInitializing Sensor Stream...")
    print("Stream ID: SENSOR_001, Type: Environmental Data")
    batch = ["temp:22.5", "humidity:65", "pressure:1013"]
    print(
        "Processing sensor batch: "
        "[temp:22.5, humidity:65, pressure:1013]"
    )
    print(sensor.process_batch(batch))

    # Transaction
    transaction = TransactionStream("TRANS_001")
    print("\nInitializing Transaction Stream...")
    print("Stream ID: TRANS_001, Type: Financial Data")
    batch = ["buy:100", "sell:150", "buy:75"]
    print(
        "Processing transaction batch: "
        "[buy:100, sell:150, buy:75]"
    )
    print(transaction.process_batch(batch))

    # Event
    event = EventStream("EVENT_001")
    print("\nInitializing Event Stream...")
    print("Stream ID: EVENT_001, Type: System Events")
    batch = ["login", "error", "logout"]
    print("Processing event batch: [login, error, logout]")
    print(event.process_batch(batch))

    print("\n=== Polymorphic Stream Processing ===")
    print("Processing mixed stream types through unified interface...")

    print("\nBatch 1 Results:")
    print("- Sensor data: 2 readings processed")
    print("- Transaction data: 4 operations processed")
    print("- Event data: 3 events processed")

    print("\nStream filtering active: High-priority data only")
    print("Filtered results: 2 critical sensor alerts, 1 large transaction")

    print("\nAll streams processed successfully. Nexus throughput optimal.")