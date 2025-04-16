package com.vincenzogulisano.usecases.synthetic;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.function.Consumer;

import org.apache.commons.lang3.Validate;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vincenzogulisano.javapythoncommunicator.StatReporter;
import com.vincenzogulisano.usecases.linearroad.InjectorType;

import common.metrics.Metric;
import common.util.Util;
import component.source.SourceFunction;
import query.LiebreContext;

public class SourceReadFromFile implements SourceFunction<TupleInput> {

    private final long IDLE_SLEEP;
    private final String path;
    private BufferedReader reader;
    private volatile boolean done;
    private boolean enabled;
    private Metric injectionRateMetric;

    private InjectorType type;
    private long firstInvocationTs;
    private long firstTupleTs;

    private long startingTS;
    private ConcurrentLinkedQueue<Long> startingTSUpdates;
    private long WS;
    private boolean firstTupleAtRealRate;
    private boolean firstTuplesSkipped;

    private volatile boolean resetRequest;
    private volatile boolean resetAck;
    private volatile boolean waitingForSPEGreenlightToStartSendingStateFillingTuples;
    private volatile boolean ackFromSPEGreenlightToStartSendingStateFillingTuples;
    private volatile boolean allStateFillingTuplesSent;
    private volatile boolean waitingForSPEGreenlightToStartSendingRealRateTuples;
    private volatile boolean ackFromSPEGreenlightToStartSendingRealRateTuples;
    private volatile boolean firstEpisodeCanStart;

    // The name of this Logger will be "org.apache.logging.Child"
    public Logger logger = LogManager.getLogger();

    public SourceReadFromFile(String path, InjectorType type, long startingTS, long WS) {
        Validate.notBlank(path, "path");
        this.path = path;
        this.type = type;
        this.startingTS = startingTS;
        this.startingTSUpdates = new ConcurrentLinkedQueue<>();
        this.WS = WS;
        IDLE_SLEEP = 1000;
        done = false;
        firstInvocationTs = -1;
        firstTupleTs = -1;
        firstTupleAtRealRate = true;
        firstTuplesSkipped = false;
        resetRequest = false;
        resetAck = false;
        waitingForSPEGreenlightToStartSendingStateFillingTuples = false;
        ackFromSPEGreenlightToStartSendingStateFillingTuples = false;
        allStateFillingTuplesSent = false;
        waitingForSPEGreenlightToStartSendingRealRateTuples = false;
        ackFromSPEGreenlightToStartSendingRealRateTuples = false;
        firstEpisodeCanStart = false;
    }

    public void setStartingTS(long startingTS) {
        logger.debug("Storing new value for startingTS ({}) in startingTSUpdates", startingTS);
        startingTSUpdates.add(startingTS);
    }

    public SourceReadFromFile(String path, InjectorType type) {
        this(path, type, 0, 0);
    }

    private void initializeReader() {
        try {
            this.reader = new BufferedReader(new FileReader(path));
        } catch (FileNotFoundException e) {
            throw new IllegalArgumentException(String.format("File not found: %s", path));
        }
        firstInvocationTs = -1;
        firstTupleTs = -1;
        firstTupleAtRealRate = true;
        firstTuplesSkipped = false;
    }

    // Temp for debugging, remove later
    private volatile long position = 0;

    @Override
    public TupleInput get() {

        position = 0;

        if (done || !firstEpisodeCanStart) {
            logger.debug(
                    "Either done processing or not authorized from SPE to start the first episode. Returning null");
            Util.sleep(IDLE_SLEEP);
            return null;
        }

        if (resetRequest) {
            logger.debug("Got a reset request, Resetting the source!");

            logger.debug("Retrieving the next startingTS");
            assert (!startingTSUpdates.isEmpty());
            startingTS = startingTSUpdates.poll();
            logger.debug("Next startingTS is {}", startingTS);

            injectionRateMetric.reset();

            resetRequest = false; // Clear the request
            resetAck = true; // Tell SPE I have stopped
            // resetReader = true; // Make sure next call I reset the reader
            waitingForSPEGreenlightToStartSendingStateFillingTuples = true; // Wait for ack from SPE to start sending
                                                                            // state filling tuples
            ackFromSPEGreenlightToStartSendingStateFillingTuples = false; // Register the ack has not been received yet
            allStateFillingTuplesSent = false; // Register we still have to complete this, since the SPE needs to know
            waitingForSPEGreenlightToStartSendingRealRateTuples = true; // Wait for ack from SPE to start sending
                                                                        // real rate tuples
            ackFromSPEGreenlightToStartSendingRealRateTuples = false; // Register the ack has not been received yet
            initializeReader();
            return null;
        }

        // If the reader has not been created yet or if a reset was requested and, thus,
        // the reader should be recreated, create a new reader
        if (reader == null) {
            initializeReader();
        }

        String t = readNextLine();
        if (t == null) {
            return null;
        }
        TupleInput result = TupleInput.fromReading(t);

        if (firstInvocationTs == -1) {
            firstInvocationTs = System.currentTimeMillis();
        }
        if (firstTupleTs == -1) {
            firstTupleTs = result.getTimestamp();
        }

        if (!firstTuplesSkipped && type == InjectorType.RL) {
            logger.debug(
                    "This is a RL injector, skipping all tuples with timestamp lower than " + (startingTS - WS));
            while (result.getTimestamp() - firstTupleTs < startingTS - WS) {

                position = 1;

                t = readNextLine();
                if (t == null) {
                    logger.warn("Returning a null tuple!");
                    return null;
                }
                result = TupleInput.fromReading(t);
            }
            logger.debug("Skipping of initial tuples completed");
            firstTuplesSkipped = true;
        }

        if (waitingForSPEGreenlightToStartSendingStateFillingTuples) {
            logger.debug("Source - checking if we got greenlight from SPE to send state filling tuples");
            while (!ackFromSPEGreenlightToStartSendingStateFillingTuples) {

                position = 2;

                Util.sleep(50);
            }
            logger.debug("Source - got greenlight from SPE to send state filling tuples");
            waitingForSPEGreenlightToStartSendingStateFillingTuples = false;
        }

        switch (type) {
            case FIXEDRATE:
                throw new RuntimeException("This injector does not support FIXEDRATE");
            case REALRATE:
                while ((System.currentTimeMillis() - firstInvocationTs) < (result.getTimestamp() - firstTupleTs)
                        * 1000) {
                    try {
                        Thread.sleep(1);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
                break;
            case RL:
                // Start the real sleep only if the first WS is over
                if (result.getTimestamp() - firstTupleTs >= startingTS) {
                    // The very first time, actually sleep for a while and then reset
                    // firstInvocationTs and firstTupleTs
                    if (firstTupleAtRealRate) {

                        logger.debug("Sending the first tuple at a real rate");

                        allStateFillingTuplesSent = true;

                        if (waitingForSPEGreenlightToStartSendingRealRateTuples) {
                            logger.debug(
                                    "Source - ready to send real tuples, but waiting for the ack from the SPE");
                            while (!ackFromSPEGreenlightToStartSendingRealRateTuples) {
                                position = 3;
                                Util.sleep(50);
                            }
                            logger.debug(
                                    "Source - ack received");
                            waitingForSPEGreenlightToStartSendingRealRateTuples = false;
                        }
                        logger.debug("Sleeping " + QuerySynthetic.sleepBeforeRealRate
                                + " ms before starting for real");
                        firstTupleAtRealRate = false;
                        try {
                            Thread.sleep(QuerySynthetic.sleepBeforeRealRate);
                        } catch (InterruptedException e) {
                            logger.warn("Thread sleep Interrupted Exception");
                        }
                        firstInvocationTs = System.currentTimeMillis();
                    }
                    while ((System.currentTimeMillis()
                            - firstInvocationTs) < (result.getTimestamp() - (firstTupleTs + startingTS))
                                    * 1000) {
                        position = 4;
                        try {
                            Thread.sleep(1);
                        } catch (InterruptedException e) {
                            logger.warn("InterruptedException!");
                        }
                    }
                }
                break;
            default:
                break;
        }
        injectionRateMetric.record(1);

        result.setStimulus(System.currentTimeMillis());
        return result;
    }

    private String readNextLine() {
        String nextLine = null;
        try {
            nextLine = reader.readLine();
        } catch (IOException e) {
            logger.warn("Text Source failed to read " + e);
        }
        done = (nextLine == null);
        return nextLine;
    }

    @Override
    public boolean isInputFinished() {
        return done;
    }

    @Override
    public void enable() {
        this.enabled = true;
        injectionRateMetric.enable();
    }

    @Override
    public boolean isEnabled() {
        return enabled;
    }

    @Override
    public void disable() {
        this.enabled = false;
        try {
            this.reader.close();
        } catch (IOException e) {
            logger.warn(String.format("Problem closing file %s: %s", path, e));
        }
        injectionRateMetric.disable();
    }

    @Override
    public boolean canRun() {
        return !done;
    }

    public HashMap<String, Consumer<Object[]>> setStatReporter(StatReporter reporter) {
        logger.debug("Source - Registering consumers");
        HashMap<String, Consumer<Object[]>> consumers = new HashMap<>();
        consumers.put("injectionrate", x -> reporter.report((long) x[0], "injectionrate", ((Long) x[1]).doubleValue()));

        return consumers;
    }

    public void createStatistics() {
        logger.debug("Source - Creating statistics");
        injectionRateMetric = LiebreContext.userMetrics().newCountPerSecondMetric("injectionrate", "rate");
    }

    public void registerResetRequest() {
        logger.debug("Registering reset request");
        resetAck = false;
        resetRequest = true;
    }

    public boolean getResetAck() {
        logger.debug("The SPE is checking the resetAck, injector is at position {}", position);
        return resetAck;
    }

    public void giveGreenlightToStartSendingStateFillingTuples() {
        logger.debug("The SPE is giving the green light to start injecting state tuples");
        ackFromSPEGreenlightToStartSendingStateFillingTuples = true;
    }

    public void giveGreenlightToStartSendingRealRateTuples() {
        ackFromSPEGreenlightToStartSendingRealRateTuples = true;
    }

    public boolean areAllStateFillingTuplesSent() {
        return allStateFillingTuplesSent;
    }

    public void notifyFirstEpisodeCanStart() {
        firstEpisodeCanStart = true;
    }

}
