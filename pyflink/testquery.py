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

# -------------------- Define EventTimestampAssigner --------------------


class EventTimestampAssigner(TimestampAssigner):
    def extract_timestamp(self, event, record_timestamp):
        return event[0]  # Extract event_time as timestamp


# -------------------- To be able to output timestamp and key too --------------------
class MyProcessWindowFunction(ProcessWindowFunction):
    def process(self, key, context, aggregates):
        window_end = context.window().end
        return [(window_end, key, agg) for agg in aggregates]

# -------------------- Define Custom Aggregate Function --------------------


class MyAggregateFunction(AggregateFunction):
    """An aggregate function that stores all tuples and computes the average only when get_result is called."""

    def create_accumulator(self):
        """Creates a new accumulator which stores all values."""
        return {"values": []}

    def add(self, value, accumulator):
        # print(f"Adding value={value[2]} from tuple={value} to accumulator.")
        """Adds a value to the state (accumulator)."""
        accumulator["values"].append(value[2])
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
    source = env.from_source(
        FileSource.for_record_stream_format(
            StreamFormat.text_line_format(), input_file
        ).build(), WatermarkStrategy.no_watermarks(), "FileSource")

    # Parse CSV lines into tuples (event_time, id, value)

    def parse_line(line):
        parts = line.split(",")
        # (event_time, id, value)
        return int(parts[0]), int(parts[1]), float(parts[2])

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
        .window(SlidingEventTimeWindows.of(Time.minutes(1), Time.seconds(20))) \
        .aggregate(MyAggregateFunction(), MyProcessWindowFunction(), output_type=Types.TUPLE([Types.LONG(), Types.INT(), Types.FLOAT()])
                   )

    # Set output file config to use exact file name
    output_file_config = OutputFileConfig.builder() \
        .with_part_prefix("output") \
        .with_part_suffix(".txt") \
        .build()

    # Define Sink (File Sink)
    sink = FileSink.for_row_format(
        output_file,
        Encoder.simple_string_encoder()
    ).with_output_file_config(output_file_config).build()

    aggregated_stream.map(
        lambda x: f"{x[0]},{x[1]},{x[2]}", output_type=Types.STRING()).sink_to(sink)

    # Execute Flink Job
    env.execute("Flink Python Sliding Window Aggregate Job")


# -------------------- Run the Application --------------------
if __name__ == "__main__":

    parser = argparse.ArgumentParser(
        description="Run PyFlink aggregate job on CSV input.")
    parser.add_argument("--input", "-i", required=True,
                        help="Path to the input CSV file")
    parser.add_argument("--output", "-o", required=True,
                        help="Path to the output file")

    args = parser.parse_args()

    input_csv_path = args.input
    output_csv_path = args.output

    run_flink_job(input_csv_path, output_csv_path)
