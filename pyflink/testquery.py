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

    def __init__(self,stat_monitor):
        self.thread_reported = False
        self.stat_monitor = stat_monitor

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
        
        # Report output rate statistic
        self.stat_monitor.report(len(aggregates))

        return [(window_end, key, agg) for agg in aggregates]

    def close(self):
        # Call StatMonitor's close method when the map function is closed
        self.stat_monitor.close()
        
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

# ----------------------------------------------------------


class LineParserWithDelay:
    def __init__(self):
        self.previous_event_time = None
        self.thread_reported = False
        print(f"LineParser init with thread_reported: {self.thread_reported}")
        self.start_time = time.perf_counter()

    def __getstate__(self):
        print("LineParser __getstate__")
        # Exclude thread_reported from being pickled
        state = self.__dict__.copy()
        if "thread_reported" in state:
            del state["thread_reported"]
        return state

    def __setstate__(self, state):
        print("LineParser __setstate__")
        # Restore the state and reinitialize thread_reported
        self.__dict__.update(state)
        self.thread_reported = False

    def parse_line(self, line):

        # Report the thread ID
        if not self.thread_reported:
            # Get the native Thread ID (TID)
            thread_id = threading.get_native_id()
            # Update window statistics
            thread_id_logger = ThreadIdLogger.get_singleton()
            if thread_id_logger:
                print("LineParser reporting line_parser to thread_id_logger")
                thread_id_logger.report(thread_id, "line_parser")
            self.thread_reported = True

        parts = line.split(",")
        event_time = int(parts[0])  # Extract event_time
        key = int(parts[1])
        value = float(parts[2])

        # Calculate the delay based on event timestamps
        if self.previous_event_time is not None:
            delay = (event_time - self.previous_event_time) / \
                1000.0  # Convert ms to seconds
            # print(f"Delay: {delay} seconds")
            target_time = self.start_time + delay
            precise_delay(target_time)
            self.start_time = time.perf_counter()  # Reset start time after delay

        self.previous_event_time = event_time

        return event_time, key, value


# ----------------------------------------------------------


class AggregatedStreamFormatter:
    def __init__(self):
        # self.sink = sink
        self.thread_reported = False
        print(
            f"AggregatedStreamFormatter init with thread_reported: {self.thread_reported}")

    def format_record(self, record):

        # Report the thread ID
        if not self.thread_reported:
            # Get the native Thread ID (TID)
            thread_id = threading.get_native_id()
            # Update window statistics
            thread_id_logger = ThreadIdLogger.get_singleton()
            if thread_id_logger:
                print(
                    "AggregatedStreamFormatter reporting line_parser to sink_formatter")
                thread_id_logger.report(thread_id, "sink_formatter")
            self.thread_reported = True

        """Format a record as a comma-separated string."""
        return f"{record[0]},{record[1]},{record[2]}"

    def format_and_sink(self, aggregated_stream, sink):
        """Format the aggregated stream and sink it to the specified sink."""
        aggregated_stream.map(
            self.format_record, output_type=Types.STRING()
        ).sink_to(sink).disable_chaining()  # Disable chaining for this operator
        
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
    outputrate_csv_path = os.path.join(output_folder, "outputrate.csv")

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

    # Instantiate the parser class
    line_parser = LineParserWithDelay()

    parsed_stream = source.map(line_parser.parse_line, output_type=Types.TUPLE(
        [Types.LONG(), Types.INT(), Types.FLOAT()]
    ))

    # Define Watermark Strategy (monotonically increasing timestamps)
    watermark_strategy = WatermarkStrategy.for_monotonous_timestamps().with_timestamp_assigner(
        EventTimestampAssigner()
    )

    # Apply Watermark Strategy
    timestamped_stream = parsed_stream.assign_timestamps_and_watermarks(
        watermark_strategy
    )

    # Initialize StatMonitor for throughput
    stat_monitor_outputrate = StatMonitor(
        outputrate_csv_path, reset_value=0, stat_type="SUM", reporting_type="RESET")

    # Apply Sliding Window Aggregation (Window size = 1 min, slide = 20 sec)
    aggregated_stream = timestamped_stream \
        .key_by(lambda x: x[1]) \
        .window(SlidingEventTimeWindows.of(Time.seconds(10), Time.seconds(5))) \
        .aggregate(MyAggregateFunction(), MyProcessWindowFunction(stat_monitor_outputrate), output_type=Types.TUPLE([Types.LONG(), Types.INT(), Types.FLOAT()])
                   ).disable_chaining()  # Disable chaining for this operator

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

    # Use the AggregatedStreamFormatter class
    formatter = AggregatedStreamFormatter()
    formatter.format_and_sink(aggregated_stream, sink)

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
