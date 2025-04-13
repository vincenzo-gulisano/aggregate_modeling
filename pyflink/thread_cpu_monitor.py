import psutil
import time
import os
from StatMonitor import StatMonitor

def monitor_threads(pid, output_dir, interval=1):
    """
    Monitors CPU usage of threads in a process and writes statistics to CSV files.

    Args:
        pid (int): The process ID to monitor.
        output_dir (str): Directory to store the CSV files.
        interval (int): Interval in seconds to collect statistics.
    """
    try:
        # Attach to the process
        process = psutil.Process(pid)
        print(f"Monitoring threads of process {pid}...")

        # Dictionary to hold StatMonitor instances for each thread
        thread_monitors = {}
        # previous_cpu_times = {}

        # Print threads information
        threads = process.threads()
        print(f"Threads in process {pid}:")
        for thread in threads:
            print(f"Thread info: {thread}")
    
        while True:
            # Get all threads in the process
            threads = process.threads()

            
            total_percent = process.cpu_percent(interval)
            total_time = sum(process.cpu_times())

            for thread in threads:
                thread_id = thread.id
                thread_name = f"thread_{thread_id}"  # Default thread name

                # Create a StatMonitor for the thread if it doesn't exist
                if thread_name not in thread_monitors:
                    csv_path = os.path.join(output_dir, f"{thread_name}.csv")
                    thread_monitors[thread_name] = StatMonitor(
                        csv_path=csv_path,
                        reset_value=0,
                        stat_type="ABSOLUTE",
                        reporting_type="CUMULATIVE"
                    )

                # # Get current CPU times for the thread
                # thread_cpu_times = process.cpu_times()
                # user_time = thread_cpu_times.user
                # system_time = thread_cpu_times.system

                # # Calculate CPU usage for the thread
                # if thread_id in previous_cpu_times:
                #     prev_user_time, prev_system_time = previous_cpu_times[thread_id]
                #     cpu_usage = ((user_time - prev_user_time) + (system_time - prev_system_time)) / interval
                # else:
                #     cpu_usage = 0  # No previous data, assume 0 usage

                # # Update previous CPU times
                # previous_cpu_times[thread_id] = (user_time, system_time)

                # Report CPU usage to the StatMonitor
                thread_monitors[thread_name].report(total_percent * ((thread.system_time + thread.user_time)/total_time))

            # Sleep for the specified interval
            time.sleep(interval)

    except psutil.NoSuchProcess:
        print(f"Process with PID {pid} no longer exists.")
    except KeyboardInterrupt:
        print("Monitoring stopped by user.")
    finally:
        # Close all StatMonitor instances
        for monitor in thread_monitors.values():
            monitor.close()

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Monitor CPU usage of threads in a process.")
    parser.add_argument("--pid", "-p", type=int, required=True, help="PID of the process to monitor")
    parser.add_argument("--output_dir", "-o", required=True, help="Directory to store CSV files")
    parser.add_argument("--interval", "-i", type=int, default=1, help="Interval in seconds to collect statistics")

    args = parser.parse_args()

    # Ensure the output directory exists
    os.makedirs(args.output_dir, exist_ok=True)

    # Start monitoring
    monitor_threads(args.pid, args.output_dir, args.interval)