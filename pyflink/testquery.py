import os
import sys
from pyflink.common import WatermarkStrategy, Encoder, Types, Time
from pyflink.datastream import StreamExecutionEnvironment, RuntimeExecutionMode
from pyflink.datastream.connectors.file_system import FileSource, StreamFormat, FileSink, OutputFileConfig, RollingPolicy
from pyflink.datastream.functions import AggregateFunction
from pyflink.common.watermark_strategy import TimestampAssigner
from pyflink.datastream.window import SlidingEventTimeWindows
import argparse
from pyflink.datastream.functions import ProcessWindowFunction
from pyflink.common import Row
from StatMonitor import StatMonitor
from thread_id_logger import ThreadIdLogger
from pyflink.datastream import MapFunction
import time  # Import time module for introducing delays
import threading

# Creating this one has global to share it with all the window instances.
# Notice the parallelism is one, so there should be no problem when it comes
# to concurrent accessess
wins_monitor_global = None


def init_thread_id_logger(path):
    if ThreadIdLogger.get_singleton() is None:
        thread_id_logger = ThreadIdLogger(
            csv_path=path
        )
        ThreadIdLogger.set_singleton(thread_id_logger)


def close_thread_id_logger():
    monitor = ThreadIdLogger.get_singleton()
    if monitor:
        monitor.close()
        ThreadIdLogger.set_singleton(None)


def init_stat_monitor(path):
    if StatMonitor.get_singleton() is None:
        monitor = StatMonitor(
            csv_path=path,
            reset_value=0,
            stat_type="SUM",
            reporting_type="CUMULATIVE"
        )
        StatMonitor.set_singleton(monitor)


def close_stat_monitor():
    monitor = StatMonitor.get_singleton()
    if monitor:
        monitor.close()
        StatMonitor.set_singleton(None)

# -------------------- For throughput monitoring ------------------------


class StatMonitorMapFunction(MapFunction):
    def __init__(self, stat_monitor):
        self.stat_monitor = stat_monitor

    def map(self, value):
        # Report the value to StatMonitor
        self.stat_monitor.report(1)
        return value

    def close(self):
        # Call StatMonitor's close method when the map function is closed
        self.stat_monitor.close()

# -------------------- Define EventTimestampAssigner --------------------


class EventTimestampAssigner(TimestampAssigner):
    def extract_timestamp(self, event, record_timestamp):
        return event[0]  # Extract event_time as timestamp


# -------------------- To be able to output timestamp and key too --------------------
class MyProcessWindowFunction(ProcessWindowFunction):

    def __init__(self):
        self.thread_reported = False

    def process(self, key, context, aggregates):
        if not self.thread_reported:
            # Get the native Thread ID (TID)
            thread_id = threading.get_native_id()
            # Update window statistics
            thread_id_logger = ThreadIdLogger.get_singleton()
            if thread_id_logger:
                thread_id_logger.report(thread_id, "aggregate")
            self.thread_reported = True

        window_end = context.window().end
        return [(window_end, key, agg) for agg in aggregates]

# -------------------- Define Custom Aggregate Function --------------------


class MyAggregateFunction(AggregateFunction):
    """An aggregate function that stores all tuples and computes the average only when get_result is called."""

    def create_accumulator(self):
        """Creates a new accumulator which stores all values."""

        # Update window statistics
        monitor = StatMonitor.get_singleton()
        if monitor:
            monitor.report(1)

        return {"values": []}

    def add(self, value, accumulator):
        """Adds a value to the state (accumulator)."""
        accumulator["values"].append(value[2])
        return accumulator

    def get_result(self, accumulator):
        """Computes the average only when needed."""

        # Update window statistics
        monitor = StatMonitor.get_singleton()
        if monitor:
            monitor.report(-1)

        if len(accumulator["values"]) == 0:
            return 0  # Avoid division by zero
        return sum(accumulator["values"]) / len(accumulator["values"])

    def merge(self, acc1, acc2):
        """Merging is not allowed; raise an exception if called."""
        raise RuntimeError("Merge function should not be called!")


# -------------------- Define precise delay function --------------------
def precise_delay(target_time):
    """Spin-wait until the target time is reached."""
    while time.perf_counter() < target_time:
        pass

# -------------------- Define Flink Job --------------------


def run_flink_job(input_file, output_folder):
    """
    Run the Flink job with the specified input file and output folder.

    Args:
        input_file (str): Path to the input CSV file.
        output_folder (str): Path to the output folder where result files will be created.
    """
    # Create file paths for output, throughput, and windows stats
    output_dir = os.path.join(output_folder, "output")
    throughput_csv_path = os.path.join(output_folder, "throughput.csv")
    windows_csv_path = os.path.join(output_folder, "windows.csv")
    threads_ids_path = os.path.join(output_folder, "threads_ids.csv")

    # Initialize the stat monitor ONCE globally
    init_stat_monitor(windows_csv_path)
    init_thread_id_logger(threads_ids_path)

    # Create StreamExecutionEnvironment
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)  # Ensures deterministic results for debugging

    # Define Source
    source = env.from_source(
        FileSource.for_record_stream_format(
            StreamFormat.text_line_format(), input_file
        ).build(), WatermarkStrategy.no_watermarks(), "FileSource")

    # Initialize StatMonitor for throughput
    stat_monitor_throughput = StatMonitor(
        throughput_csv_path, reset_value=0, stat_type="SUM", reporting_type="RESET")

    # Inject stat reporting after source
    source = source.map(
        StatMonitorMapFunction(stat_monitor_throughput),
        output_type=Types.STRING()
    )

    # Parse CSV lines into tuples (event_time, id, value) with delay
    def parse_line_with_delay():
        previous_event_time = None
        start_time = time.perf_counter()

        def parse_line(line):
            nonlocal previous_event_time, start_time
            parts = line.split(",")
            event_time = int(parts[0])  # Extract event_time
            key = int(parts[1])
            value = float(parts[2])

            # Calculate the delay based on event timestamps
            if previous_event_time is not None:
                delay = (event_time - previous_event_time) / \
                    1000.0  # Convert ms to seconds
                target_time = start_time + delay
                precise_delay(target_time)
                start_time = time.perf_counter()  # Reset start time after delay

            previous_event_time = event_time
            return event_time, key, value

        return parse_line

    parse_line = parse_line_with_delay()

    parsed_stream = source.map(parse_line, output_type=Types.TUPLE(
        [Types.LONG(), Types.INT(), Types.FLOAT()]))

    # Define Watermark Strategy (monotonically increasing timestamps)
    watermark_strategy = WatermarkStrategy.for_monotonous_timestamps().with_timestamp_assigner(
        EventTimestampAssigner()
    )

    # Apply Watermark Strategy
    timestamped_stream = parsed_stream.assign_timestamps_and_watermarks(
        watermark_strategy)

    # Apply Sliding Window Aggregation (Window size = 1 min, slide = 20 sec)
    aggregated_stream = timestamped_stream \
        .key_by(lambda x: x[1]) \
        .window(SlidingEventTimeWindows.of(Time.seconds(10), Time.seconds(5))) \
        .aggregate(MyAggregateFunction(), MyProcessWindowFunction(), output_type=Types.TUPLE([Types.LONG(), Types.INT(), Types.FLOAT()])
                   )

    # Set output file config to use exact file name
    output_file_config = OutputFileConfig.builder() \
        .with_part_prefix("output") \
        .with_part_suffix(".txt") \
        .build()

    # Define Sink (File Sink)
    sink = FileSink.for_row_format(
        output_dir,
        Encoder.simple_string_encoder()
    ).with_output_file_config(output_file_config).build()

    aggregated_stream.map(
        lambda x: f"{x[0]},{x[1]},{x[2]}", output_type=Types.STRING()).sink_to(sink)

    # Execute Flink Job
    env.execute("Flink Python Sliding Window Aggregate Job")

    # Close the window stat monitor
    close_stat_monitor()
    close_thread_id_logger()


# -------------------- Run the Application --------------------
if __name__ == "__main__":

    parser = argparse.ArgumentParser(
        description="Run PyFlink aggregate job on CSV input.")
    parser.add_argument("--input", "-i", required=True,
                        help="Path to the input CSV file")
    parser.add_argument("--output_folder", "-o", required=True,
                        help="Path to the output folder")

    args = parser.parse_args()

    input_csv_path = args.input
    output_folder = args.output_folder

    # Ensure the output folder exists
    os.makedirs(output_folder, exist_ok=True)

    print("Starting Flink job...")

    run_flink_job(input_csv_path, output_folder)
