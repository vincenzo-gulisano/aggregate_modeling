package com.vincenzogulisano.usecases.synthetic;

import java.io.FileWriter;
import java.io.IOException;
import java.util.Random;

public class DataGenerator {
    private final Random random = new Random();

    // Configuration parameters
    private final int maxTimestamp;
    private final int minPeakDistance;
    private final int maxPeakDistance;
    private final int minPeak;
    private final int maxPeak;
    private final int meanMin;
    private final int meanMax;
    private final int varianceMin;
    private final int varianceMax;
    private final int distributionMin;
    private final int distributionMax;

    public DataGenerator(int maxTimestamp, int minPeakDistance, int maxPeakDistance,
            int minPeak, int maxPeak, int meanMin, int meanMax,
            int varianceMin, int varianceMax, int distributionMin, int distributionMax) {
        this.maxTimestamp = maxTimestamp;
        this.minPeakDistance = minPeakDistance;
        this.maxPeakDistance = maxPeakDistance;
        this.minPeak = minPeak;
        this.maxPeak = maxPeak;
        this.meanMin = meanMin;
        this.meanMax = meanMax;
        this.varianceMin = varianceMin;
        this.varianceMax = varianceMax;
        this.distributionMin = distributionMin;
        this.distributionMax = distributionMax;
    }

    public void generate(String filePath) {
        try (FileWriter writer = new FileWriter(filePath)) {
            int currentTime = 0;
            int nextDistributionChange = 0;
            int currentMean = meanMin;
            int currentVariance = varianceMin;

            while (currentTime <= maxTimestamp) {
                // Update distribution if needed
                if (currentTime >= nextDistributionChange) {
                    currentMean = meanMin + random.nextInt(meanMax - meanMin + 1);
                    currentVariance = varianceMin + random.nextInt(varianceMax - varianceMin + 1);
                    nextDistributionChange = currentTime + distributionMin
                            + random.nextInt(distributionMax - distributionMin + 1);
                }

                // Determine next peak timestamp and value
                int nextPeakTime = currentTime + minPeakDistance
                        + random.nextInt(maxPeakDistance - minPeakDistance + 1);
                int peakValue = minPeak + random.nextInt(maxPeak - minPeak + 1);
                int timeToNextPeak = nextPeakTime - currentTime;

                // Generate entries until the next peak
                for (int t = currentTime; t < nextPeakTime && t <= maxTimestamp; t++) {

                        // Update distribution if needed
                    if (t >= nextDistributionChange) {
                        currentMean = meanMin + random.nextInt(meanMax - meanMin + 1);
                        currentVariance = varianceMin + random.nextInt(varianceMax - varianceMin + 1);
                        nextDistributionChange = t + distributionMin
                                + random.nextInt(distributionMax - distributionMin + 1);
                    }
                    
                    int entriesCount = (t - currentTime < timeToNextPeak / 2)
                            ? linearIncrease(1000, peakValue, t - currentTime, timeToNextPeak / 2)
                            : linearDecrease(peakValue, 1000, t - currentTime - timeToNextPeak / 2, timeToNextPeak / 2);

                    // Generate keys based on Gaussian distribution
                    for (int i = 0; i < entriesCount; i++) {
                        int key = (int) (currentMean + random.nextGaussian() * currentVariance);
                        int value = random.nextInt(); // Random integer value
                        writer.write(t + "," + key + "," + value + "\n");
                    }
                }

                currentTime = nextPeakTime;
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private int linearIncrease(int start, int end, int currentTime, int totalTime) {
        return start + (end - start) * currentTime / totalTime;
    }

    private int linearDecrease(int start, int end, int currentTime, int totalTime) {
        return start - (start - end) * currentTime / totalTime;
    }

    public static void main(String[] args) {
        // Example usage
        DataGenerator generator = new DataGenerator(3600*2, 180, 300, 5000, 15000,
                50, 50000, 10, 100, 120, 180);
        generator.generate("/home/vincenzo/romeo/data/input/synthetic.csv");
    }
}
