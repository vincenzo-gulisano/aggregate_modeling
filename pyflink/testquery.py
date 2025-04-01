import os
import sys
from pyflink.common import WatermarkStrategy, Encoder, Types
from pyflink.datastream import StreamExecutionEnvironment, RuntimeExecutionMode
from pyflink.datastream.connectors.file_system import FileSource, StreamFormat, FileSink, OutputFileConfig, RollingPolicy
from pyflink.datastream.functions import AggregateFunction



# -------------------- Define Custom Aggregate Function --------------------
class MyAggregateFunction(AggregateFunction):
    """An aggregate function that stores all tuples and computes the average only when get_result is called."""

    def create_accumulator(self):
        """Creates a new accumulator which stores all values."""
        return {"values": []}

    def add(self, value, accumulator):
        """Adds a value to the state (accumulator)."""
        accumulator["values"].append(value)
        return accumulator

    def get_result(self, accumulator):
        """Computes the average only when needed."""
        if len(accumulator["values"]) == 0:
            return 0  # Avoid division by zero
        return sum(accumulator["values"]) / len(accumulator["values"])

    def merge(self, acc1, acc2):
        """Merging is not allowed; raise an exception if called."""
        raise RuntimeError("Merge function should not be called!")


# -------------------- Define Flink Job --------------------
def run_flink_job(input_file, output_file):
    # Create StreamExecutionEnvironment
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)  # Ensures deterministic results for debugging

    # Define Source
    input_format = TextInputFormat(input_file)
    input_format.set_charset("UTF-8")
    source = env.read_file(input_format, input_file, line_delimiter="\n", type_info=Types.STRING())

    # Parse CSV lines into tuples (event_time, id, value)
    def parse_line(line):
        parts = line.split(",")
        return int(parts[0]), int(parts[1]), float(parts[2])  # (event_time, id, value)

    parsed_stream = source.map(parse_line, output_type=Types.TUPLE([Types.LONG(), Types.INT(), Types.FLOAT()]))

    # Define Watermark Strategy (monotonically increasing timestamps)
    watermark_strategy = WatermarkStrategy.for_monotonous_timestamps().with_timestamp_assigner(
        lambda event, timestamp: event[0]  # Assigns event_time as the timestamp
    )

    # Apply Watermark Strategy
    timestamped_stream = parsed_stream.assign_timestamps_and_watermarks(watermark_strategy)

    # Apply Sliding Window Aggregation (Window size = 1 min, slide = 20 sec)
    aggregated_stream = timestamped_stream \
        .key_by(lambda x: x[1]) \
        .window(SlidingEventTimeWindows.of(Time.minutes(1), Time.seconds(20))) \
        .aggregate(MyAggregateFunction(), output_type=Types.FLOAT())

    # Define Sink (File Sink)
    sink = FileSink.for_row_format(
        output_file,
        SimpleStringEncoder()
    ).with_output_file_config(
        OutputFileConfig.builder().with_part_prefix("output").with_part_suffix(".txt").build()
    ).build()

    aggregated_stream.map(lambda x: str(x), output_type=Types.STRING()).sink_to(sink)

    # Execute Flink Job
    env.execute("Flink Python Sliding Window Aggregate Job")


# -------------------- Run the Application --------------------
if __name__ == "__main__":
    # File paths (modify as needed)
    input_csv_path = "input.csv"  # Provide the path to the input CSV file
    output_csv_path = "output.txt"  # Output file path

    # Run Flink job
    run_flink_job(input_csv_path, output_csv_path)
