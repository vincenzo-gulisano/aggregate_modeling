package com.vincenzogulisano.util;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;

public class RewardsLogger {

    private BufferedWriter writer;

    public RewardsLogger(String fileName) {

        try {
            this.writer = new BufferedWriter(new FileWriter(fileName));

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void writeReward(long reward) {
        String line = String.format("%d,%d\n", System.currentTimeMillis() / 1000, reward);

        try {
            writer.write(line);
            writer.flush();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void close() {
        try {
            if (writer != null) {
                writer.close();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
