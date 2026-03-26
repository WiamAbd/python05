from abc import ABC, abstractmethod
from typing import Any, List, Dict, Union, Optional


class DataStream(ABC):

    def __init__(self, stream_id: str) -> None:
        self.stream_id = stream_id

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
        return {"stream_id": self.stream_id}


class SensorStream(DataStream):

    def process_batch(self, data_batch: List[Any]) -> str:
        temps = [
            float(x.split(":")[1])
            for x in data_batch if "temp" in x
        ]
        avg = sum(temps) / len(temps) if temps else 0
        return f"{len(data_batch)} readings processed, avg temp: {avg}°C"

    def filter_data(
        self,
        data_batch: List[Any],
        criteria: Optional[str] = None
    ) -> List[Any]:
        if criteria == "high":
            return [
                x for x in data_batch
                if "temp" in x and float(x.split(":")[1]) > 30
            ]
        return data_batch


class TransactionStream(DataStream):

    def process_batch(self, data_batch: List[Any]) -> str:
        total = 0
        for item in data_batch:
            action, val = item.split(":")
            val = int(val)

            if action == "sell":
                total += val
            elif action == "buy":
                total -= val

        return f"{len(data_batch)} operations, net flow: {total} units"

    def filter_data(
        self,
        data_batch: List[Any],
        criteria: Optional[str] = None
    ) -> List[Any]:
        if criteria == "large":
            return [
                x for x in data_batch
                if int(x.split(":")[1]) > 100
            ]
        return data_batch


class EventStream(DataStream):

    def process_batch(self, data_batch: List[Any]) -> str:
        errors = sum(1 for e in data_batch if e == "error")
        return f"{len(data_batch)} events, {errors} error detected"

    def filter_data(
        self,
        data_batch: List[Any],
        criteria: Optional[str] = None
    ) -> List[Any]:
        if criteria == "error":
            return [e for e in data_batch if e == "error"]
        return data_batch


# 🔥 REQUIRED CLASS
class StreamProcessor:

    def process(
        self,
        streams: List[DataStream],
        batches: List[List[Any]]
    ) -> List[str]:

        return [
            stream.process_batch(batch)
            for stream, batch in zip(streams, batches)
        ]

    def filter(
        self,
        streams: List[DataStream],
        batches: List[List[Any]],
        criteria: List[Optional[str]]
    ) -> List[List[Any]]:

        return [
            stream.filter_data(batch, crit)
            for stream, batch, crit in zip(streams, batches, criteria)
        ]


# ================= MAIN =================
if __name__ == "__main__":
    print("=== CODE NEXUS - POLYMORPHIC STREAM SYSTEM ===")

    sensor = SensorStream("SENSOR_001")
    transaction = TransactionStream("TRANS_001")
    event = EventStream("EVENT_001")

    # Individual streams
    batch = ["temp:22.5", "humidity:65", "pressure:1013"]

    print("\nInitializing Sensor Stream...")
    print("Stream ID: SENSOR_001, Type: Environmental Data")
    print(f"Processing sensor batch: {batch}")
    print(f"Sensor analysis: {sensor.process_batch(batch)}")

    batch = ["buy:100", "sell:150", "buy:75"]

    print("\nInitializing Transaction Stream...")
    print("Stream ID: TRANS_001, Type: Financial Data")
    print(f"Processing transaction batch: {batch}")
    print(f"Transaction analysis: {transaction.process_batch(batch)}")

    batch = ["login", "error", "logout"]

    print("\nInitializing Event Stream...")
    print("Stream ID: EVENT_001, Type: System Events")
    print(f"Processing event batch: {batch}")
    print(f"Event analysis: {event.process_batch(batch)}")

    # 🔥 POLYMORPHIC PART
    print("\n=== Polymorphic Stream Processing ===")
    print("Processing mixed stream types through unified interface...")

    streams = [sensor, transaction, event]
    batches = [
        ["temp:21", "temp:23"],
        ["buy:10", "sell:20", "buy:5", "sell:15"],
        ["login", "error", "error"]
    ]

    processor = StreamProcessor()
    results = processor.process(streams, batches)

    print("\nBatch 1 Results:")
    for stream, result in zip(streams, results):

        base_result = result.split(",")[0]  # extract count part

        if isinstance(stream, SensorStream):
            print(f"- Sensor data: {base_result}")

        elif isinstance(stream, TransactionStream):
            print(f"- Transaction data: {base_result}")

        elif isinstance(stream, EventStream):
            print(f"- Event data: {base_result}")

    filtered = processor.filter(
        streams,
        batches,
        ["high", "large", "error"]
    )

    print("\nStream filtering active: High-priority data only")
    print(
        f"Filtered results: {len(filtered[0])} critical sensor alerts, "
        f"{len(filtered[1])} large transaction"
    )

    print("\nAll streams processed successfully. Nexus throughput optimal.")
