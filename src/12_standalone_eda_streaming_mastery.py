"""
Standalone Mastery Module 1: Event-Driven Architecture (EDA), Pub/Sub Consumer Groups,
Stateful Stream Processing (Tumbling Windows + DLQ), Event Sourcing, and Real-Time OLAP.
Runs 100% locally in WSL2 Ubuntu with zero cloud accounts required.
"""

from pathlib import Path
import hashlib
import json
import shutil
from datetime import datetime, timedelta
import duckdb
import pandas as pd
import matplotlib.pyplot as plt
from matplotlib.patches import FancyBboxPatch


# =============================================================================
# 1. STANDALONE PARTITIONED LOG BROKER (Exact Apache Kafka / Redpanda Mechanics)
# =============================================================================
class StandaloneKafkaBroker:
    """
    A transparent, disk-backed implementation of an Apache Kafka Broker.
    Stores topics as physical partition log files (partition-0.jsonl, partition-1.jsonl...)
    and persists Consumer Group offsets in __consumer_offsets.json.
    """

    def __init__(self, broker_dir: Path):
        self.broker_dir = broker_dir
        self.offsets_file = broker_dir / "__consumer_offsets.json"
        if broker_dir.exists():
            shutil.rmtree(broker_dir)
        broker_dir.mkdir(parents=True, exist_ok=True)
        self.offsets_file.write_text(json.dumps({}, indent=2))

    def create_topic(self, topic_name: str, num_partitions: int = 3):
        topic_path = self.broker_dir / topic_name
        topic_path.mkdir(parents=True, exist_ok=True)
        for p in range(num_partitions):
            (topic_path / f"partition-{p}.jsonl").touch()
        print(f"[BROKER] Created Topic '{topic_name}' with {num_partitions} physical partitions.")

    def _choose_partition(self, key: str, num_partitions: int) -> int:
        # Exactly like Kafka's default partitioner: deterministic hash(key) % num_partitions
        h = int(hashlib.md5(key.encode("utf-8")).hexdigest(), 16)
        return h % num_partitions

    def produce(self, topic_name: str, key: str, value: dict, event_time: str):
        topic_path = self.broker_dir / topic_name
        partitions = sorted(topic_path.glob("partition-*.jsonl"))
        p_idx = self._choose_partition(key, len(partitions))
        p_file = topic_path / f"partition-{p_idx}.jsonl"

        # Calculate next sequential offset in this partition
        existing_lines = p_file.read_text().splitlines() if p_file.stat().st_size > 0 else []
        next_offset = len(existing_lines)

        record = {
            "topic": topic_name,
            "partition": p_idx,
            "offset": next_offset,
            "timestamp_event": event_time,
            "key": key,
            "value": value,
        }
        with p_file.open("a", encoding="utf-8") as f:
            f.write(json.dumps(record) + "\n")
        return p_idx, next_offset

    def consume_group(self, topic_name: str, group_id: str, reset_earliest: bool = False) -> list[dict]:
        """
        Reads unread messages for a specific Consumer Group across all partitions
        and commits the new offsets to __consumer_offsets.json.
        """
        offsets_state = json.loads(self.offsets_file.read_text())
        group_offsets = offsets_state.get(group_id, {}).get(topic_name, {})

        topic_path = self.broker_dir / topic_name
        partitions = sorted(topic_path.glob("partition-*.jsonl"))
        consumed_records = []

        new_group_offsets = {}
        for p_file in partitions:
            p_name = p_file.stem  # e.g., 'partition-0'
            last_committed_offset = -1 if reset_earliest else group_offsets.get(p_name, -1)

            lines = p_file.read_text().splitlines()
            for idx, line in enumerate(lines):
                if idx > last_committed_offset:
                    consumed_records.append(json.loads(line))
                    last_committed_offset = idx
            new_group_offsets[p_name] = last_committed_offset

        # Commit updated offsets for this Consumer Group
        if group_id not in offsets_state:
            offsets_state[group_id] = {}
        offsets_state[group_id][topic_name] = new_group_offsets
        self.offsets_file.write_text(json.dumps(offsets_state, indent=2))

        # Sort by Event Timestamp for stream processing
        return sorted(consumed_records, key=lambda r: (r["timestamp_event"], r["partition"], r["offset"]))


# =============================================================================
# 2. MAIN HANDS-ON LAB PIPELINE
# =============================================================================
def run_standalone_eda_lab():
    broker_dir = Path("data/standalone_kafka_broker")
    out_dir = Path("data/output")
    out_dir.mkdir(parents=True, exist_ok=True)

    broker = StandaloneKafkaBroker(broker_dir)

    # Create 3 Enterprise Topics:
    # 1. Raw Telemetry Stream (3 partitions)
    # 2. Dead-Letter Queue (DLQ) for corrupted sensor payloads (1 partition)
    # 3. Event-Sourced Asset Lifecycle Log (2 partitions)
    broker.create_topic("uk.offshore.telemetry.raw", num_partitions=3)
    broker.create_topic("uk.offshore.telemetry.dlq", num_partitions=1)
    broker.create_topic("uk.offshore.asset.eventsourcing", num_partitions=2)

    # -------------------------------------------------------------------------
    # STEP 1: Publish High-Frequency Telemetry Stream + 1 Corrupted Sensor Event
    # -------------------------------------------------------------------------
    print("\n--- 1. PRODUCING REAL-TIME TELEMETRY STREAM (PARTITIONED BY ASSET_ID) ---")
    base_time = datetime(2026, 7, 15, 0, 0, 0)
    assets = [
        ("UK-WIND-01", "Dogger Bank Wind", 14.0, 3.2),
        ("NS-PLAT-02", "Forties Charlie Rig", 22.5, 5.8),
        ("UK-WIND-02", "Moray East Wind", 18.2, 4.6),
    ]

    for hour in range(12):  # 12 hourly events across 3 assets = 36 events
        ev_time = (base_time + timedelta(hours=hour)).isoformat() + "Z"
        for asset_id, name, base_wind, base_wave in assets:
            # Simulate storm intensification between hours 4 and 8
            storm_boost = 1.8 if 4 <= hour <= 8 else 0.0
            payload = {
                "asset_id": asset_id,
                "asset_name": name,
                "wind_speed_ms": round(base_wind + storm_boost * 2.5 + (hour * 0.2), 2),
                "wave_height_m": round(base_wave + storm_boost + (hour * 0.08), 2),
                "power_mw": round(min(12.0, (base_wind + storm_boost) * 0.55), 2),
            }
            broker.produce("uk.offshore.telemetry.raw", key=asset_id, value=payload, event_time=ev_time)

    # Inject 1 corrupted sensor glitch (Wave height = -999.0m, Wind = 450.0 m/s) to test our DLQ!
    corrupt_time = (base_time + timedelta(hours=5)).isoformat() + "Z"
    broker.produce(
        "uk.offshore.telemetry.raw",
        key="UK-WIND-01",
        value={"asset_id": "UK-WIND-01", "asset_name": "Dogger Bank Wind", "wind_speed_ms": 450.0, "wave_height_m": -999.0, "power_mw": -50.0},
        event_time=corrupt_time,
    )

    # Inspect Physical Partition Distribution
    print("Physical Partition Files Created on Disk (Notice deterministic key routing!):")
    for p_file in sorted((broker_dir / "uk.offshore.telemetry.raw").glob("partition-*.jsonl")):
        lines = [json.loads(line) for line in p_file.read_text().splitlines()]
        keys_in_part = sorted(list({r["key"] for r in lines}))
        print(f"  ├── {p_file.name}: {len(lines):>2} messages | Asset Keys assigned: {keys_in_part}")

    # -------------------------------------------------------------------------
    # STEP 2: Stream Processing (Consumer Group 1) -> Validation, DLQ & Tumbling Windows
    # -------------------------------------------------------------------------
    print("\n--- 2. STATEFUL STREAM PROCESSOR: DLQ FILTERING + 6-HOUR TUMBLING WINDOWS ---")
    raw_stream = broker.consume_group("uk.offshore.telemetry.raw", group_id="cg_stream_processor_v1")

    valid_events = []
    dlq_count = 0

    for record in raw_stream:
        val = record["value"]
        # Schema / Physical Bounds Guardrail (EIP Filter + Dead Letter Channel)
        if val["wave_height_m"] < 0.0 or val["wind_speed_ms"] > 100.0:
            broker.produce(
                "uk.offshore.telemetry.dlq",
                key=record["key"],
                value={"error": "PHYSICAL_BOUNDS_VIOLATION", "original_record": record},
                event_time=record["timestamp_event"],
            )
            dlq_count += 1
            print(f"  [DLQ ROUTED] Caught corrupted message at Partition={record['partition']} Offset={record['offset']} (wave={val['wave_height_m']}m)")
        else:
            dt = datetime.fromisoformat(record["timestamp_event"].replace("Z", ""))
            # Assign each event to a 6-Hour Tumbling Window based on EVENT TIME
            window_start_hour = (dt.hour // 6) * 6
            window_label = f"{dt.strftime('%Y-%m-%d')} {window_start_hour:02d}:00-{window_start_hour+5:02d}:59"
            valid_events.append({
                "event_time": record["timestamp_event"],
                "partition": record["partition"],
                "offset": record["offset"],
                "tumbling_window_6h": window_label,
                **val,
            })

    print(f"[STREAM SUMMARY] Processed {len(raw_stream)} events -> {len(valid_events)} valid | {dlq_count} quarantined in DLQ.")

    # -------------------------------------------------------------------------
    # STEP 3: Real-Time OLAP Analytics with DuckDB over Tumbling Windows
    # -------------------------------------------------------------------------
    print("\n--- 3. REAL-TIME OLAP ANALYTICS OVER 6-HOUR TUMBLING WINDOWS (DUCKDB) ---")
    stream_df = pd.DataFrame(valid_events)

    window_sql = """
    SELECT
        tumbling_window_6h,
        asset_id,
        asset_name,
        COUNT(*)                        AS events_in_window,
        ROUND(AVG(wind_speed_ms), 2)    AS avg_wind_ms,
        ROUND(MAX(wave_height_m), 2)    AS max_wave_m,
        ROUND(SUM(power_mw), 2)         AS total_energy_mwh,
        CASE
            WHEN MAX(wave_height_m) >= 5.0 THEN 'CRITICAL_WAVE_SHUTDOWN'
            ELSE 'OPTIMAL_GENERATION'
        END                             AS real_time_status
    FROM stream_df
    GROUP BY tumbling_window_6h, asset_id, asset_name
    ORDER BY asset_id, tumbling_window_6h;
    """
    window_summary_df = duckdb.sql(window_sql).df()
    duckdb.sql(window_sql).show()

    # -------------------------------------------------------------------------
    # STEP 4: Event Sourcing & State Reconstruction (Replay from Offset 0)
    # -------------------------------------------------------------------------
    print("\n--- 4. EVENT SOURCING: REBUILDING DIGITAL-TWIN STATE FROM IMMUTABLE LOG ---")
    # Instead of running UPDATE statements in Postgres, we append immutable state-change events!
    lifecycle_events = [
        ("NS-PLAT-02", "2026-07-15T00:00:00Z", "ASSET_REGISTERED", {"status": "ONLINE", "heli_deck": "OPEN", "crew_onboard": 85}),
        ("NS-PLAT-02", "2026-07-15T04:00:00Z", "WAVE_THRESHOLD_5M_BREACHED", {"status": "STORM_STANDBY", "heli_deck": "CLOSED"}),
        ("NS-PLAT-02", "2026-07-15T06:00:00Z", "NON_ESSENTIAL_CREW_EVACUATED", {"crew_onboard": 40}),
        ("NS-PLAT-02", "2026-07-15T10:00:00Z", "STORM_CLEARED_ALL_CLEAR", {"status": "ONLINE", "heli_deck": "OPEN"}),
    ]
    for asset_key, ts, ev_type, state_delta in lifecycle_events:
        broker.produce(
            "uk.offshore.asset.eventsourcing",
            key=asset_key,
            value={"event_type": ev_type, "state_delta": state_delta},
            event_time=ts,
        )

    # Replay the event log from Offset 0 to materialize the current state (plus audit history!)
    es_records = broker.consume_group("uk.offshore.asset.eventsourcing", group_id="cg_digital_twin_projector", reset_earliest=True)
    current_twin_state = {"asset_id": "NS-PLAT-02"}
    for idx, rec in enumerate(es_records):
        ev_type = rec["value"]["event_type"]
        delta = rec["value"]["state_delta"]
        current_twin_state.update(delta)
        current_twin_state["last_event_offset"] = rec["offset"]
        current_twin_state["last_updated_utc"] = rec["timestamp_event"]
        print(f"  Replay Step {idx+1} [Offset {rec['offset']}] ({ev_type:<28}) -> State Now: {current_twin_state}")

    # -------------------------------------------------------------------------
    # STEP 5: Prove Independent Consumer Group Offsets (Pub/Sub Fan-Out)
    # -------------------------------------------------------------------------
    print("\n--- 5. VERIFYING INDEPENDENT CONSUMER GROUP OFFSETS (__consumer_offsets.json) ---")
    # A second consumer group (e.g., Lakehouse Archiver) reads the exact same raw topic from Offset 0!
    archiver_records = broker.consume_group("uk.offshore.telemetry.raw", group_id="cg_lakehouse_parquet_sink")
    print(f"  • Consumer Group 1 ('cg_stream_processor_v1') read {len(raw_stream)} messages.")
    print(f"  • Consumer Group 2 ('cg_lakehouse_parquet_sink') independently read all {len(archiver_records)} messages from Offset 0!")
    print(f"  • Committed Offsets File: {broker.offsets_file}")
    print(broker.offsets_file.read_text())

    # Render Visual Architecture Diagram
    create_eda_mastery_visual(window_summary_df, out_dir / "module1_eda_streaming_mastery.png")


def create_eda_mastery_visual(window_df: pd.DataFrame, out_img: Path):
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(16, 6.8), dpi=150)
    fig.suptitle(
        "Standalone Mastery Module 1: Kafka Partition Log, Consumer Groups, Stream Windows & Event Sourcing",
        fontsize=13.5, fontweight="bold", y=0.98
    )

    ax1.set_title("1. Inside the Partitioned Log Broker: Pub/Sub Fan-Out, DLQ & Event Sourcing", fontsize=11, fontweight="bold")
    ax1.axis("off")

    boxes = [
        (0.03, 0.72, 0.94, 0.21, "#fff2cc", "#b45f06",
         "1. Partitioned Append-Only Topic: 'uk.offshore.telemetry.raw' (Key = asset_id)",
         "• Partition 0: [Offset 0..N]  |  • Partition 1: [Offset 0..N]  |  • Partition 2: [Offset 0..N]\n"
         "• Messages are NEVER deleted on read! Each Consumer Group tracks its own offset pointer."),
        (0.03, 0.43, 0.45, 0.22, "#cfe2f3", "#0b5394",
         "2A. Consumer Group 1: Stream Processor",
         "• Filters bad sensors -> Dead-Letter Queue (DLQ)\n"
         "• Groups valid events by 6h Tumbling Window\n"
         "• Feeds DuckDB Real-Time OLAP table"),
        (0.52, 0.43, 0.45, 0.22, "#d9ead3", "#274e13",
         "2B. Consumer Group 2: Lakehouse Sink",
         "• Reads exact same topic independently!\n"
         "• Zero contention with Consumer Group 1\n"
         "• Lands micro-batches into Parquet / Iceberg"),
        (0.03, 0.08, 0.94, 0.27, "#e6cff2", "#4a154b",
         "3. Event Sourcing Pattern ('uk.offshore.asset.eventsourcing')",
         "• Instead of overwriting rows in Postgres (UPDATE assets SET...), every state transition is appended:\n"
         "  [Offset 0: ASSET_REGISTERED] -> [Offset 1: WAVE_5M_BREACH] -> [Offset 2: CREW_EVAC] -> [Offset 3: ALL_CLEAR]\n"
         "• Replaying Offsets 0..3 reconstructs current Digital-Twin state AND provides a 100% immutable audit trail!"),
    ]
    for x, y, w, h, fc, ec, title, body in boxes:
        ax1.add_patch(FancyBboxPatch((x, y), w, h, boxstyle="round,pad=0.02", facecolor=fc, edgecolor=ec, linewidth=1.5))
        ax1.text(x + 0.02, y + h - 0.055, title, fontsize=9.2, fontweight="bold", color=ec)
        ax1.text(x + 0.02, y + 0.03, body, fontsize=8, family="monospace")

    # Right Panel: 6-Hour Tumbling Window Stream Aggregations
    ax2.set_title("2. Real-Time 6-Hour Tumbling Window Peak Wave Height (Stream Analytics)", fontsize=11, fontweight="bold")
    piv = window_df.pivot(index="asset_id", columns="tumbling_window_6h", values="max_wave_m")
    piv.plot(kind="bar", ax=ax2, width=0.55, edgecolor="black", color=["#3c78d8", "#cc0000"])
    ax2.axhline(5.0, color="black", linestyle="--", linewidth=1.8, label="5.0m Marine Shutdown Threshold")
    ax2.set_ylabel("Max Wave Height in 6h Window (m)")
    ax2.set_xlabel("Offshore Asset ID (Partition Key)")
    ax2.set_xticklabels(ax2.get_xticklabels(), rotation=0, fontweight="bold")
    ax2.grid(axis="y", linestyle="--", alpha=0.4)
    ax2.legend(title="6-Hour Tumbling Window (Event Time)", fontsize=8)

    plt.tight_layout(rect=[0, 0, 1, 0.94])
    plt.savefig(out_img)
    plt.close(fig)
    print(f"\n[SUCCESS] Saved Module 1 EDA & Streaming Mastery Diagram to: {out_img.resolve()}")


if __name__ == "__main__":
    run_standalone_eda_lab()