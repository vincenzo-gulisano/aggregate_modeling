import argparse
import pandas as pd
import matplotlib.pyplot as plt

def plot_csv_to_pdf(csv_path, output_pdf, x_label, y_label, x_scale=1, y_scale=1):
    # Read the CSV file
    data = pd.read_csv(csv_path, header=None, names=["time", "value"])
    
    # Apply scaling factors
    data["time"] *= x_scale
    data["value"] *= y_scale

    # Create the plot
    plt.figure(figsize=(8, 6))
    plt.plot(data["time"], data["value"], marker='o', markersize=2, linewidth=0.5)
    plt.xlabel(x_label, fontsize=8)
    plt.ylabel(y_label, fontsize=8)
    plt.xticks(fontsize=8)
    plt.yticks(fontsize=8)
    plt.tight_layout()

    # Save the plot to a PDF
    plt.savefig(output_pdf, format="pdf")
    plt.close()

def main():
    parser = argparse.ArgumentParser(description="Plot time vs value from a CSV and save as a PDF.")
    parser.add_argument("--csv", "-c", required=True, help="Path to the input CSV file (two columns: time, value)")
    parser.add_argument("--output", "-o", required=True, help="Path to the output PDF file")
    parser.add_argument("--xlabel", "-xl", required=True, help="Label for the x-axis")
    parser.add_argument("--ylabel", "-yl", required=True, help="Label for the y-axis")
    parser.add_argument("--xscale", "-xs", type=float, default=1, help="Scaling factor for the x-axis (default: 1)")
    parser.add_argument("--yscale", "-ys", type=float, default=1, help="Scaling factor for the y-axis (default: 1)")

    args = parser.parse_args()

    plot_csv_to_pdf(
        csv_path=args.csv,
        output_pdf=args.output,
        x_label=args.xlabel,
        y_label=args.ylabel,
        x_scale=args.xscale,
        y_scale=args.yscale
    )

if __name__ == "__main__":
    main()