package com.vincenzogulisano.woost;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.function.Consumer;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.xerial.snappy.Snappy;

import com.vincenzogulisano.javapythoncommunicator.StatReporter;

import common.metrics.Metric;
import common.metrics.TimeMetric;
import common.tuple.RichTuple;
import common.util.Util;
import component.operator.in1.aggregate.BaseKeyExtractor;
import component.operator.in1.aggregate.TimeAggregate;
import query.LiebreContext;

@SuppressWarnings("unchecked")
public class WoostAggregateWithCompression<IN extends RichTuple, OUT extends RichTuple> extends TimeAggregate<IN, OUT> {

    private WoostTimeWindow<IN, OUT> aggregateWindow;
    private Map<String, WoostTimeWindow<IN, OUT>> uncompressedWins;
    private Map<String, byte[]> compressedWins;
    private long earliestWinLeftBoundary;

    private Metric throughputMetric;
    private Metric outputtMetric;

    private long compressionTimeThreshold;
    private Metric compressionsMetric;
    private Metric decompressionMetric;
    private TimeMetric compressionRatio;

    private Metric maxEventTimeMetric;

    // This new field is added to keep track of the latest event time
    // It is set to -1 upon reset
    // TODO It is probably redundant, latestTimestamp might do
    private volatile long latestEventTime;

    private TreeMap<Long, Set<String>> tsKeys;
    private HashMap<String, Long> keyLatestTs;

    // Used for compression
    private ByteArrayOutputStream baos;
    private ObjectOutputStream oos;

    private ConcurrentLinkedQueue<Long> dUpdates;

    public Logger logger = LogManager.getLogger();

    private volatile boolean resetAck;
    // Temp
    private volatile boolean firstCallAfterReset;

    public WoostAggregateWithCompression(
            String id,
            int instance,
            int parallelismDegree,
            long windowSize,
            long windowSlide,
            WoostTimeWindow<IN, OUT> aggregateWindow,
            long compressionTimeThreshold) {
        super(id, instance, parallelismDegree, windowSize, windowSlide, aggregateWindow, new BaseKeyExtractor<IN>());

        this.aggregateWindow = aggregateWindow;
        this.compressionTimeThreshold = compressionTimeThreshold;
        this.dUpdates = new ConcurrentLinkedQueue<>();

        this.resetAck = false;
        this.firstCallAfterReset = false;

    }

    public void reset() {
        logger.debug("Registering reset request");
        resetAck = false;
        firstCallAfterReset = false;
        logger.debug("{} tuples in input stream", getInput().size());
        while (getInput().size() > 0 || inProcess) {
            logger.debug("Tuples being processed ({})", getInput().size());
            Util.sleep(200);
        }
        logger.debug("No tuples in the input stream, resetting");
        if (inProcess) {
            logger.debug("In process though... so we wait");
            while (inProcess) {
                Util.sleep(50);
            }
            logger.debug("Process complete");
        } else {
            logger.debug("and not processing tuples");
        }
        internalReset();
    }

    private void internalReset() {
        logger.debug("Clearing {} tuples in input stream", getInput().size());
        getInput().clear();
        logger.debug("Resetting windows");
        uncompressedWins = new HashMap<>();
        compressedWins = new HashMap<>();
        tsKeys = new TreeMap<>();
        keyLatestTs = new HashMap<>();
        earliestWinLeftBoundary = -1;
        latestEventTime = -1;
        compressionsMetric.reset();
        decompressionMetric.reset();
        maxEventTimeMetric.reset();
        compressionRatio.reset();
        throughputMetric.reset();
        outputtMetric.reset();
        logger.debug("Acking back to SPE");
        resetAck = true;
        firstCallAfterReset = true;
    }

    public boolean getResetAck() {
        return resetAck;
    }

    @Override
    public void enable() {

        logger.debug("Enabling statistiscs");

        super.enable();
        compressionsMetric.enable();
        decompressionMetric.enable();
        maxEventTimeMetric.enable();
        compressionRatio.enable();
        throughputMetric.enable();
        outputtMetric.enable();

    }

    @Override
    public void disable() {
        super.disable();
        compressionsMetric.disable();
        decompressionMetric.disable();
        maxEventTimeMetric.disable();
        compressionRatio.disable();
        throughputMetric.disable();
        outputtMetric.disable();
    }

    // Iterators and entries used by the processTupleIn1 function
    Iterator<Entry<String, byte[]>> i1;
    Entry<String, byte[]> e1;
    WoostTimeWindow<IN, OUT> wToDecompress;
    Iterator<Entry<String, WoostTimeWindow<IN, OUT>>> i2;
    Entry<String, WoostTimeWindow<IN, OUT>> e2;
    Iterator<Map.Entry<Long, Set<String>>> compressionIt;
    Map.Entry<Long, Set<String>> compressionE;
    WoostTimeWindow<IN, OUT> wToCompress;

    private volatile boolean inProcess = false;

    public List<OUT> processTupleIn1(IN t) {

        // Ping all stats
        throughputMetric.record(1);
        latestEventTime = t.getTimestamp();
        maxEventTimeMetric.record(latestEventTime);

        if (firstCallAfterReset) {
            logger.debug("First invocation of processTupleIn1 after the reset");
            logger.debug("{} tuples in input stream to be processed", getInput().size());
        }

        inProcess = true;

        // Check for D updates
        while (!dUpdates.isEmpty()) {
            logger.debug("dUpdate is not empty");
            Long d = dUpdates.poll();
            if (d != null) {
                compressionTimeThreshold = d;
                logger.debug("\n************\n* Compression threshold updated to {} at {}\n************",
                        compressionTimeThreshold, System.currentTimeMillis() / 1000);
            }
        }

        // Create result
        List<OUT> result = new LinkedList<>();

        // Extract tuple info
        latestTimestamp = t.getTimestamp();
        long tL = getEarliestWinStartTS(latestTimestamp);
        String k = keyExtractor.getKey(t);

        if (firstCallAfterReset) {
            logger.debug("The condition to enter the output production loop is {}",
                    (earliestWinLeftBoundary != -1 && earliestWinLeftBoundary < tL));
        }

        while (earliestWinLeftBoundary != -1 && earliestWinLeftBoundary < tL) {

            if (firstCallAfterReset) {
                logger.debug("earliestWinLeftBoundary is {}", earliestWinLeftBoundary);
                logger.debug("tL is {}", tL);
            }

            long compressions = 0;
            long decompressions = 0;
            // Produce results for compressed (if any)
            i1 = compressedWins.entrySet().iterator();
            while (i1.hasNext()) {
                e1 = i1.next();

                // Decompress
                try {
                    wToDecompress = (WoostTimeWindow<IN, OUT>) new ObjectInputStream(
                            new ByteArrayInputStream(Snappy.uncompress(e1.getValue()))).readObject();
                    decompressions++;
                } catch (ClassNotFoundException | IOException e1) {
                    e1.printStackTrace();
                }

                // TEMP
                if (wToDecompress == null) {
                    logger.warn("Trying to decompress window for {} but it's null! The byte[] length is {}",
                            e1.getKey(), e1.getValue().length);
                    try {
                        byte[] snappyUncompress = Snappy.uncompress(e1.getValue());
                        logger.warn("snappyUncompress length: {}", snappyUncompress.length);
                        try {
                            wToDecompress = (WoostTimeWindow<IN, OUT>) new ObjectInputStream(
                                    new ByteArrayInputStream(Snappy.uncompress(e1.getValue()))).readObject();
                        } catch (ClassNotFoundException e) {
                            logger.warn("Window still null!");
                        }
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }

                // Get output
                wToDecompress.setLatestStimulus(t.getStimulus());
                OUT outT = wToDecompress.getAggregatedResult();
                if (outT != null) {
                    outputtMetric.record(1);
                    result.add(outT);
                }

                // Slide
                wToDecompress.slideTo(earliestWinLeftBoundary + WA);

                // Remove or compress again
                if (wToDecompress.isEmpty()) {
                    i1.remove();
                } else {

                    try {
                        baos = new ByteArrayOutputStream();
                        oos = new ObjectOutputStream(baos);
                        oos.writeObject(wToDecompress);
                        oos.close();
                        e1.setValue(Snappy.compress(baos.toByteArray()));
                    } catch (IOException exception) {
                        exception.printStackTrace();
                    }

                    compressions++;
                }

            }
            compressionsMetric.record(compressions);
            decompressionMetric.record(decompressions);
            compressionRatio.record((long) (((double) uncompressedWins.size() * 100)
                    / ((double) compressedWins.size() + (double) uncompressedWins.size())));

            // Produce results for uncompressed (if any)
            i2 = uncompressedWins.entrySet().iterator();
            while (i2.hasNext()) {
                e2 = i2.next();

                // Get output
                e2.getValue().setLatestStimulus(t.getStimulus());
                OUT outT = e2.getValue().getAggregatedResult();
                if (outT != null) {
                    outputtMetric.record(1);
                    result.add(outT);
                }

                // Slide
                e2.getValue().slideTo(earliestWinLeftBoundary + WA);

                // Remove or compress again
                if (e2.getValue().isEmpty()) {

                    tsKeys.get(keyLatestTs.get(e2.getKey())).remove(e2.getKey());
                    keyLatestTs.remove(e2.getKey());

                    i2.remove();
                }

            }

            // At this point, all windows at earliestWinLeftBoundary are done, so we can
            // update tsKeys
            tsKeys.remove(earliestWinLeftBoundary);

            earliestWinLeftBoundary += tL;
        }

        // Add contribution of this tuple and update metrics
        WoostTimeWindow<IN, OUT> w = getWindow(tL, k);
        w.add(t);
        
        // if the key was already observed before, it has been
        // stored associated to its previous timestamp, so that can be
        // removed
        if (keyLatestTs.containsKey(k)) {
            tsKeys.get(keyLatestTs.get(k)).remove(k);
        }
        // Now store the latest key and ts pair in both variables
        keyLatestTs.put(k, latestTimestamp);
        if (!tsKeys.containsKey(latestTimestamp)) {
            tsKeys.put(latestTimestamp, new HashSet<>());
        }
        tsKeys.get(latestTimestamp).add(k);

        // Compress early windows (if any)
        compressionIt = tsKeys.entrySet().iterator();
        long compressions = 0;
        while (compressionIt.hasNext()) {
            compressionE = compressionIt.next();
            if (latestTimestamp - compressionE.getKey() >= compressionTimeThreshold) {

                for (String wK : compressionE.getValue()) {

                    wToCompress = uncompressedWins.get(wK);
                    uncompressedWins.remove(wK); // Added while writing pseudocode, check if correct!
                    // Move to compressed windows and update memory metric

                    try {
                        baos = new ByteArrayOutputStream();
                        oos = new ObjectOutputStream(baos);
                        oos.writeObject(wToCompress);
                        oos.close();
                        compressedWins.put(wK, Snappy.compress(baos.toByteArray()));

                        compressions++;
                    } catch (IOException exception) {
                        exception.printStackTrace();
                    }

                    // Remove from keyLatestTs, because now they are compressed
                    keyLatestTs.remove(wK);
                }

                compressionIt.remove(); // Remove the current entry safely
            } else {
                break;
            }
        }
        compressionsMetric.record(compressions);
        compressionRatio.record((long) (((double) uncompressedWins.size() * 100)
                / ((double) compressedWins.size() + (double) uncompressedWins.size())));

        earliestWinLeftBoundary = tL;
        if (firstCallAfterReset) {
            logger.debug("Now updating metrics");
        }
        
        inProcess = false;
        if (firstCallAfterReset) {
            logger.debug("Exiting");
            firstCallAfterReset = false;
        }

        return result;
    }

    /**
     * Returns the latest event time processed by the Aggregate. Or -1 if no tuple
     * has been processed (possibly after a reset)
     * 
     * @return The latest event time
     */
    public long getLatestEventTime() {
        return latestEventTime;
    }

    private WoostTimeWindow<IN, OUT> getWindow(long tL, String k) {
        WoostTimeWindow<IN, OUT> result = null;
        if (compressedWins.containsKey(k)) {
            // Decompress

            try {
                result = (WoostTimeWindow<IN, OUT>) new ObjectInputStream(
                        new ByteArrayInputStream(Snappy.uncompress(compressedWins.get(k)))).readObject();
            } catch (ClassNotFoundException | IOException e1) {
                e1.printStackTrace();
            }

            decompressionMetric.record(1);
            uncompressedWins.put(k, result);
            compressedWins.remove(k);
        } else if (uncompressedWins.containsKey(k)) {
            result = uncompressedWins.get(k);
        } else {
            result = aggregateWindow.woostFactory();
            result.setKey(k);
            result.setInstanceNumber(instance);
            result.setParallelismDegree(parallelismDegree);
            result.slideTo(tL);
            uncompressedWins.put(k, result);

        }
        return result;
    }

    public long changeD(long v) {
        logger.debug("Storing change request to d: {}",v);
        dUpdates.add(v);
        return latestEventTime;
    }

    public HashMap<String, Consumer<Object[]>> setStatReporter(StatReporter reporter) {
        logger.debug("Agg - Registering consumers");
        HashMap<String, Consumer<Object[]>> consumers = new HashMap<>();
        consumers.put("comp", x -> reporter.report((long) x[0], "comp", ((Long) x[1]).doubleValue()));
        consumers.put("ratio", x -> reporter.report((long) x[0], "ratio", ((Long) x[1]).doubleValue()));
        consumers.put("dec", x -> reporter.report((long) x[0], "dec", ((Long) x[1]).doubleValue()));
        consumers.put("eventtime", x -> reporter.report((long) x[0], "eventtime", ((Long) x[1]).doubleValue()));
        consumers.put("throughput", x -> reporter.report((long) x[0], "throughput", ((Long) x[1]).doubleValue()));
        consumers.put("agg-output", x -> reporter.report((long) x[0], "agg-output", ((Long) x[1]).doubleValue()));

        return consumers;

    }

    public void createStatistics() {
        logger.debug("Agg - Creating statistics");
        compressionsMetric = LiebreContext.userMetrics().newCountPerSecondMetric("comp", "count");
        compressionRatio = LiebreContext.userMetrics().newAverageTimeMetric("ratio", "percent");
        decompressionMetric = LiebreContext.userMetrics().newCountPerSecondMetric("dec", "count");
        maxEventTimeMetric = LiebreContext.userMetrics().newTotalMaxMetric("eventtime", "max");
        throughputMetric = LiebreContext.userMetrics().newCountPerSecondMetric("throughput", "count");
        outputtMetric = LiebreContext.userMetrics().newCountPerSecondMetric("agg-output", "count");
    }

}
