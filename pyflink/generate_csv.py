import argparse
import csv
import random
import time

class SyntheticCSVGenerator:
    """
    Generates synthetic CSV data with timestamps, keys, and values.

    Attributes:
        output_path (str): Path to the output CSV file.
        duration_sec (int): Duration of data generation in seconds.
        rate_per_sec (int): Number of tuples to generate per second.
        num_keys (int): Number of unique keys.
        stats_path (str): Path to the injection rate statistics CSV file.
    """
    def __init__(self, output_path, duration_sec, rate_per_sec, num_keys, stats_path):
        self.output_path = output_path
        self.duration_sec = duration_sec
        self.rate_per_sec = rate_per_sec
        self.num_keys = num_keys
        self.stats_path = stats_path

    def generate(self):
        total_tuples = self.duration_sec * self.rate_per_sec
        print(f"Generating {total_tuples} tuples...")

        # Start timestamp = now in milliseconds
        start_ts_ms = int(time.time() * 1000)
        try:
            with open(self.output_path, mode='w', newline='') as f, \
                 open(self.stats_path, mode='w', newline='') as stats_f:
                writer = csv.writer(f)
                stats_writer = csv.writer(stats_f) if self.stats_path else None

                for sec in range(self.duration_sec):
                    base_ts = start_ts_ms + sec * 1000

                    # Generate `rate_per_sec` timestamps spread across this second
                    offsets = sorted(int(random.expovariate(1.5) * 1000) % 1000 for _ in range(self.rate_per_sec))
                    tuples_created = 0

                    for offset in offsets:
                        event_time = base_ts + offset
                        key = random.randint(1, self.num_keys)
                        value = round(random.uniform(10.0, 30.0), 2)  # Random float value
                        writer.writerow([event_time, key, value])
                        tuples_created += 1

                    stats_writer.writerow([sec, tuples_created])

            print(f"Done. Output written to {self.output_path}")
            if self.stats_path:
                print(f"Injection rate stats written to {self.stats_path}")
        except IOError as e:
            print(f"Error writing to file: {e}")
            return


def main():
    parser = argparse.ArgumentParser(description="Generate synthetic CSV data.")
    parser.add_argument("--output", "-o", required=True, help="Output CSV file path")
    parser.add_argument("--duration", "-d", type=int, required=True, help="Duration in seconds")
    parser.add_argument("--rate", "-r", type=int, required=True, help="Tuples per second")
    parser.add_argument("--keys", "-k", type=int, required=True, help="Number of unique keys")
    parser.add_argument("--seed", type=int, help="Random seed for reproducibility")
    parser.add_argument("--injectionRateStat", help="Path to injection rate statistics CSV file")

    args = parser.parse_args()

    if args.seed is not None:
        random.seed(args.seed)

    if args.duration <= 0:
        raise ValueError("Duration must be greater than 0.")
    if args.rate <= 0:
        raise ValueError("Rate must be greater than 0.")
    if args.keys <= 0:
        raise ValueError("Number of keys must be greater than 0.")

    generator = SyntheticCSVGenerator(
        output_path=args.output,
        duration_sec=args.duration,
        rate_per_sec=args.rate,
        num_keys=args.keys,
        stats_path=args.injectionRateStat
    )
    generator.generate()


if __name__ == "__main__":
    main()
