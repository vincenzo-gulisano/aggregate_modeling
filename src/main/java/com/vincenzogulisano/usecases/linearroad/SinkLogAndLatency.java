package com.vincenzogulisano.usecases.linearroad;

import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.HashMap;
import java.util.function.Consumer;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vincenzogulisano.javapythoncommunicator.StatReporter;

import common.metrics.Metric;
import common.metrics.TimeMetric;
import common.tuple.RichTuple;
import common.util.Util;
import component.sink.BaseSink;
import component.sink.SinkFunction;
import query.LiebreContext;

public class SinkLogAndLatency<T extends RichTuple> extends BaseSink<T> {

    private Metric outrateMetric;
    private TimeMetric latencyMetric;
    private PrintWriter writer;
    private boolean writeOut;
    private String outPath;

    public Logger logger = LogManager.getLogger();

    public SinkLogAndLatency(String id, SinkFunction<T> function, boolean writeOut, String outPath) {
        super(id, function);
        this.writeOut = writeOut;
        this.outPath = outPath;

    }

    public void reset() {
        logger.debug("Registering reset request");
        logger.debug("Got the reset lock");
        while (getInput().size() > 0) {
            logger.debug("There are tuples in the input stream, waiting");
            Util.sleep(50);
        }
        if (getInput().size() == 0) {
            logger.debug("No tuples in the input stream, resetting immediately");
            internalReset();
        } else {
            logger.debug("There exist tuples in the input stream, deferring the reset to main thread");
        }
    }

    private void internalReset() {
        logger.debug("Clearing {} tuples in input stream", getInput().size());
        getInput().clear();
        outrateMetric.reset();
        latencyMetric.reset();
    }

    // This method gets called by the BaseSink, and there are no concurrent calls to
    // process, so there should be no need for locks
    @Override
    public void ping() {
        outrateMetric.ping();
        latencyMetric.ping();
    }

    @Override
    public void enable() {
        super.enable();
        outrateMetric.enable();
        latencyMetric.enable();
        if (writeOut) {
            try {
                this.writer = new PrintWriter(new FileWriter(outPath), true);
            } catch (IOException e) {
                throw new IllegalArgumentException(String.format("Cannot write to file :%s", outPath));
            }
        }
    }

    @Override
    public void disable() {
        super.disable();
        outrateMetric.disable();
        latencyMetric.disable();
        if (writeOut) {
            writer.flush();
            writer.close();
        }
    }

    @Override
    public void processTuple(T t) {

        super.processTuple(t);
        outrateMetric.record(1);
        latencyMetric.record(System.currentTimeMillis() - t.getStimulus());
        if (writeOut) {
            writer.println(t);
        }
    }

    public HashMap<String, Consumer<Object[]>> setStatReporter(StatReporter reporter) {
        logger.debug("Sink - Registering consumers");
        HashMap<String, Consumer<Object[]>> consumers = new HashMap<>();
        consumers.put("outrate", x -> reporter.report((long) x[0], "outrate", ((Long) x[1]).doubleValue()));
        consumers.put("latency", x -> reporter.report((long) x[0], "latency", ((Long) x[1]).doubleValue()));

        return consumers;
    }

    public void createStatistics() {
        logger.debug("Sink - Creating statistics");
        outrateMetric = LiebreContext.userMetrics().newCountPerSecondMetric("outrate", "rate");
        latencyMetric = LiebreContext.userMetrics().newAverageTimeMetric("latency", "average");
    }

}
