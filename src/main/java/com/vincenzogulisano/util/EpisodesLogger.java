package com.vincenzogulisano.util;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;

public class EpisodesLogger {

    private BufferedWriter writer;
    private int counter;

    public EpisodesLogger(String fileName) {
        this.counter = 0;

        try {
            this.writer = new BufferedWriter(new FileWriter(fileName));
            writer.write("ts,episode,event\n");

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void writeStartEvent() {
        writeEvent("start");
    }

    public void writeMeasurementEvent() {
        writeEvent("state");
    }

    public void writeActionEvent(String action) {
        writeEvent(String.format("action %s", action));
    }

    public void writeEndEvent() {
        writeEvent("end");
        counter++;
    }


    public void writeCloseEvent() {
        writeEvent("close");
    }

    private void writeEvent(String event) {
        String line = String.format("%d,%d,%s\n", System.currentTimeMillis() / 1000, counter, event);

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
