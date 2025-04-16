package com.vincenzogulisano.javapythoncommunicator;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vincenzogulisano.util.EpisodesLogger;
import com.vincenzogulisano.util.RewardsLogger;

import common.util.Util;
import java.util.HashSet;
import java.util.TreeMap;
import java.util.ArrayList;
import java.util.Map.Entry;
import java.util.Arrays;

public class EnvironmentStateCalculator implements StatReporter {

    enum LatStatus {
        BELOW,
        ABOVE,
        UNKNOWN;
    }

    class CompressionValue {
        public final double value;
        public final boolean valid;

        public CompressionValue(double value, boolean valid) {
            this.value = value;
            this.valid = valid;
        }

        @Override
        public String toString() {
            return value + " (" + valid + ")";
        }
    }

    protected class Pair<T, U> {
        private final T timestamp;
        private final U value;

        public Pair(T timestamp, U value) {
            this.timestamp = timestamp;
            this.value = value;
        }

        public T getTimestamp() {
            return timestamp;
        }

        public U getValue() {
            return value;
        }

        public String toString() {
            return String.format("(%d,%.2f)", timestamp, value);
        }
    }

    protected final long monitoringPeriod;
    protected final Producer<String, String> producer;

    protected Map<String, LinkedList<Pair<Long, Double>>> measurements;

    protected volatile boolean resetRequest;
    protected volatile boolean resetAcknowledged;
    protected volatile boolean resetCompleted;

    public Logger logger = LogManager.getLogger();
    public EpisodesLogger episodesLogger;

    protected AtomicInteger sendStateTokens;
    protected long clockTimeBarrier;
    protected long eventTimeBarrier;
    protected final String separator;

    protected ReentrantLock lock;

    private boolean dataSpansAtLeastTheMonitoringPeriod;

    protected final int maxVarDValues = 1000;
    protected List<Long> varDValues;

    private boolean runInternalThread;
    private final int internalThreadPeriod = 100;

    private final RewardsLogger rewardsLogger;

    private TreeMap<Long, HashMap<String, Double>> stateMeasurements;
    private long lastReportedStateMaxTS;
    List<String> relevantMetrics;
    private final long hardLatencyThreshold;

    // These two variables keep track of whether the latency was above the threshold
    // and about the compression of the previously reported states
    private List<LatStatus> latStatusInStates;
    private List<CompressionValue> latestCompressionsInReportedStates;

    private TreeMap<Long, Double> prevLatenciesAboveTerminationThreshold;
    private TreeMap<Long, Double> prevCPUsAboveTerminationThreshold;
    private long numberOfLatenciesExceedingEarlyTerminationThreshold;
    private final double earlyTerminationThreshold;
    private long numberOfCPUsExceedingEarlyTerminationThreshold;
    private final double earlyTerminationThresholdCPU;

    public EnvironmentStateCalculator(long monitoringPeriod, Producer<String, String> producer, String separator,
            long valuesPerObservation, long latencyThreshold, double CPUThreshold, double earlyTerminationThreshold,
            String rewardsLoggerOutputFile) {
        this.monitoringPeriod = monitoringPeriod;
        this.producer = producer;
        this.measurements = new HashMap<>();
        this.resetRequest = false;
        this.resetAcknowledged = false;
        this.resetCompleted = true;
        this.separator = separator;
        this.sendStateTokens = new AtomicInteger();
        this.lock = new ReentrantLock();
        this.dataSpansAtLeastTheMonitoringPeriod = false;

        varDValues = new LinkedList<>();

        runInternalThread = true;

        this.rewardsLogger = new RewardsLogger(rewardsLoggerOutputFile);

        this.hardLatencyThreshold = latencyThreshold;
        // this.softLatencyThreshold = latencyThreshold / 2;
        this.earlyTerminationThreshold = earlyTerminationThreshold;
        this.earlyTerminationThresholdCPU = CPUThreshold;
        this.prevLatenciesAboveTerminationThreshold = new TreeMap<>();
        this.prevCPUsAboveTerminationThreshold = new TreeMap<>();
        
        logger.debug("Hard latency set to {}", hardLatencyThreshold);
        relevantMetrics = new ArrayList<>(
                Arrays.asList("injectionrate", "throughput", "outrate", "latency", "ratio", "comp", "dec",
                        "CPU-in", "CPU-agg", "CPU-out", "eventtime"));
        stateMeasurements = new TreeMap<>();

        latStatusInStates = new LinkedList<>();
        latestCompressionsInReportedStates = new LinkedList<>();

        resetVariables();
        try {

            Thread internalThread = new Thread(() -> sendStateAndRewardIfAvailable());
            // Set the thread as a daemon so it doesn't prevent the program from exiting
            internalThread.setDaemon(true);
            // Start the thread
            internalThread.start();

        } catch (Exception e) {
            System.out.println(e);
        }

    }

    public void close() {
        this.lock.lock();
        this.producer.close();
        runInternalThread = false;
        this.rewardsLogger.close();
        this.lock.unlock();
    }

    public void setResetRequest() {
        this.resetRequest = true;
        logger.debug("SPE asking for a reset. resetRequest:{}", resetRequest);
    }

    public boolean getResetAcknowledged() {
        logger.debug("SPE invoking getResetAcknowledged. resetAcknowledged:{}", resetAcknowledged);
        return resetAcknowledged;
    }

    public void setResetCompleted() {
        resetVariables();
        this.resetCompleted = true;
        logger.debug("reset completed set by SPE. resetCompleted={}", resetCompleted);
    }

    @Override
    public void addSendStateToken(long clockTimeBarrier, long eventTimeBarrier, long dValue) {
        if (this.sendStateTokens.get() != 0) {
            logger.fatal("Cannot add a state token if one is already defined!");
            throw new RuntimeException("Cannot add a state token if one is already defined!");
        }
        this.sendStateTokens.incrementAndGet();
        this.clockTimeBarrier = clockTimeBarrier;
        this.eventTimeBarrier = eventTimeBarrier;
        logger.debug("added send state token, current value is {}, clockTimeBarrier:{}, eventTimeBarrier:{}, D:{}",
                sendStateTokens.get(), clockTimeBarrier, eventTimeBarrier, dValue);

        logger.debug("Storing D value {}", dValue);
        varDValues.add(dValue);
        while (varDValues.size() > maxVarDValues) {
            logger.debug("Removing D value {}", varDValues.get(0));
            varDValues.remove(0);
        }
    }

    // protected boolean valueIsToBeRegistered(String id, double value) {
    // if (id.equals("outrate") && value == 0) {
    // return false;
    // }
    // if ((id.equals("latency") || id.equals("ratio")) && value == -1) {
    // return false;
    // }
    // return true;
    // }
    protected boolean valueIsToBeRegistered(String id, double value) {
        return true;
    }

    protected void resetVariables() {
        measurements.clear();
        logger.debug("Clearing varDValues");
        varDValues.clear();
        dataSpansAtLeastTheMonitoringPeriod = false;
        stateMeasurements.clear();
        lastReportedStateMaxTS = -1;

        logger.debug(
                "Clearing latencyAboveThresholdInReportedStates, compressionsAboveZeroInReportedStates, and latenciesAboveTerminationThreshold");
        latStatusInStates.clear();
        latestCompressionsInReportedStates.clear();
        prevLatenciesAboveTerminationThreshold.clear();
        prevCPUsAboveTerminationThreshold.clear();
        numberOfLatenciesExceedingEarlyTerminationThreshold = 0;
        numberOfCPUsExceedingEarlyTerminationThreshold = 0;
        logger.debug("numberOfLatenciesExceedingEarlyTerminationThreshold reset to 0.");
        logger.debug("numberOfCPUsExceedingEarlyTerminationThreshold reset to 0.");
    }

    @Override
    public void report(long ts, String id, double value) {

        this.lock.lock();

        if (resetRequest) {
            resetRequest = false;
            resetCompleted = false;
            resetAcknowledged = true;
            logger.debug(
                    "EnvironmentStateCalculator - resetting. resetRequest:{}, resetAcknowledged:{}, resetCompleted:{}",
                    resetRequest, resetAcknowledged, resetCompleted);
            measurements.clear();
            this.lock.unlock();
            return;
        }

        if (resetAcknowledged && !resetCompleted) {
            logger.debug(
                    "EnvironmentStateCalculator - reset acknowledge, but not completed. Not storing stats. resetAcknowledged:{}, resetCompleted:{}",
                    resetAcknowledged, resetCompleted);
            this.lock.unlock();
            return;
        }

        if (resetAcknowledged && resetCompleted) {
            resetAcknowledged = false;
            resetCompleted = false;
            logger.debug("Setting resetAcknowledged and resetCompleted to false");
        }

        // dataSpansAtLeastTheMonitoringPeriod = false;

        // Checking if we have enought measurements
        // If more than enough and keepOnlyMonitoringPeriodData, removing them
        if (!measurements.isEmpty()) {
            for (String id_ : measurements.keySet()) {
                while (!measurements.get(id_).isEmpty()
                        && measurements.get(id_).peek().getTimestamp() <= ts - monitoringPeriod) {
                    if (!dataSpansAtLeastTheMonitoringPeriod) {
                        logger.debug(
                                "dataSpansAtLeastTheMonitoringPeriod to true thanks to stat {}, ts {}, and current ts {}",
                                id_, measurements.get(id_).peek().getTimestamp(), ts);
                    }
                    dataSpansAtLeastTheMonitoringPeriod = true;
                    break;
                }
            }
        }

        if (valueIsToBeRegistered(id, value)) {
            if (!measurements.containsKey(id)) {
                measurements.put(id, new LinkedList<>());
            }
            measurements.get(id).add(new Pair<Long, Double>(ts, value));
            // logger.debug("Registering {},{},{}", ts, id, String.format("%.2f", value));
        }

        this.lock.unlock();
    }

    private void sendStateAndRewardIfAvailable() {

        while (runInternalThread) {

            this.lock.lock();

            if (!dataSpansAtLeastTheMonitoringPeriod) {
                // logger.debug("Internal thread, data does not span monitoring period");
            } else if (!(sendStateTokens.get() > 0)) {
                // logger.debug("Internal thread, no send state tokens");
            } else if (!areRewardAndNewStateMeasurementAvailable()) {
                // logger.debug("Internal thread, rewards and state not available");
            } else {

                logger.debug(
                        "Internal thread, data spans monitoring period, one token is available and reward/state are ready...");

                sendStateTokens.set(0);

                long reward = getReward();
                String msg = getStateMeasurementAsString() + separator + reward + separator + getExtraInfo();
                rewardsLogger.writeReward(reward);
                logger.debug("Sending state/reward/extrainfo {}", msg);
                producer.send(new ProducerRecord<>("stats", msg));
                logger.debug("Sent");
                if (episodesLogger != null) {
                    logger.debug("logging event at time {}", System.currentTimeMillis() / 1000);
                    episodesLogger.writeMeasurementEvent();
                    logger.debug("logged");
                }

                // if (resetAllMeasurementsAfterReport) {
                // measurements.clear();
                // dataSpansAtLeastTheMonitoringPeriod = false;
                // }

                logger.debug("done with state forwarding");

            }

            this.lock.unlock();

            Util.sleep(internalThreadPeriod);

        }

    }

    @Override
    public void registerLogger(EpisodesLogger logger) {
        this.episodesLogger = logger;
    }

    public String getStateMeasurementAsString() {

        if ((stateMeasurements.lastKey() - stateMeasurements.firstKey()) > monitoringPeriod * 2) {
            logger.debug("Trimming state measurements since they grew over 2 times the monitoring period");
            while ((stateMeasurements.lastKey() - stateMeasurements.firstKey()) > monitoringPeriod * 2) {
                stateMeasurements.pollFirstEntry();
            }
        }

        logger.debug("Preparing the state measurement as string.");
        if (logger.isDebugEnabled()) {
            // Inside if to avoid substring operation cost if not needed
            logger.debug("These are the latest reports\n{}",
                    stateFormatter(stateMeasurements));
        }
        logger.debug("stateMeasurements.lastKey():{}, monitoringPeriod:{}", stateMeasurements.lastKey(),
                monitoringPeriod);

        long thresholdTS = stateMeasurements.firstKey();
        logger.debug("The state will contain readings for state from ts {}.", thresholdTS);
        if (stateMeasurements.lastKey() - thresholdTS > monitoringPeriod) {
            for (long ts : stateMeasurements.keySet()) {
                thresholdTS = ts;
                if (stateMeasurements.lastKey() - thresholdTS <= monitoringPeriod) {
                    break;
                }
            }
            logger.debug(
                    "Actually, the state will contain readings for state from {} to avoid having more than {} entries",
                    thresholdTS, monitoringPeriod);
        }

        // Collect the entries exceeding the latency threshold in the latest set of
        // measurements
        TreeMap<Long, Double> latenciesAboveTerminationThreshold = new TreeMap<>();
        for (Entry<Long, HashMap<String, Double>> entry : stateMeasurements.entrySet()) {
            if (entry.getValue().containsKey("latency")
                    && entry.getValue().get("latency") > earlyTerminationThreshold) {
                latenciesAboveTerminationThreshold.put(entry.getKey(), entry.getValue().get("latency"));
            }
        }
        logger.debug("Latencies exceeding early termination threshold: {}", latenciesAboveTerminationThreshold);
        // Clean the ones that where already reported
        HashSet<Long> toBeRemoved = new HashSet<>();
        for (Entry<Long, Double> entry : latenciesAboveTerminationThreshold.entrySet()) {
            if (prevLatenciesAboveTerminationThreshold.containsKey(entry.getKey())) {
                logger.debug("Removing this latency because it has been already accounted for: {}", entry);
                toBeRemoved.add(entry.getKey());
            }
        }
        for (Long k : toBeRemoved) {
            latenciesAboveTerminationThreshold.remove(k);
        }
        numberOfLatenciesExceedingEarlyTerminationThreshold += latenciesAboveTerminationThreshold.size();
        logger.debug("Number of latencies exceeding early termination threshold: {}",
                numberOfLatenciesExceedingEarlyTerminationThreshold);
        prevLatenciesAboveTerminationThreshold = latenciesAboveTerminationThreshold;

        // Collect the entries exceeding the CPU threshold in the latest set of
        // measurements
        TreeMap<Long, Double> cpusAboveTerminationThreshold = new TreeMap<>();
        for (Entry<Long, HashMap<String, Double>> entry : stateMeasurements.entrySet()) {
            if (entry.getValue().containsKey("CPU-agg")
                    && entry.getValue().get("CPU-agg") > earlyTerminationThresholdCPU) {
                cpusAboveTerminationThreshold.put(entry.getKey(), entry.getValue().get("CPU-agg"));
            }
        }
        logger.debug("CPUs exceeding early termination threshold: {}", cpusAboveTerminationThreshold);
        // Clean the ones that where already reported
        HashSet<Long> toBeRemovedCPU = new HashSet<>();
        for (Entry<Long, Double> entry : cpusAboveTerminationThreshold.entrySet()) {
            if (prevCPUsAboveTerminationThreshold.containsKey(entry.getKey())) {
                logger.debug("Removing this cpu because it has been already accounted for: {}", entry);
                toBeRemovedCPU.add(entry.getKey());
            }
        }
        for (Long k : toBeRemovedCPU) {
            cpusAboveTerminationThreshold.remove(k);
        }
        numberOfCPUsExceedingEarlyTerminationThreshold += cpusAboveTerminationThreshold.size();
        logger.debug("Number of cpus exceeding early termination threshold: {}",
                numberOfCPUsExceedingEarlyTerminationThreshold);
        prevCPUsAboveTerminationThreshold = cpusAboveTerminationThreshold;

        StringBuilder logMsg = new StringBuilder();
        for (String metric : relevantMetrics) {
            for (Entry<Long, HashMap<String, Double>> entry : stateMeasurements.entrySet()) {
                if (entry.getKey() > thresholdTS) {
                    if (entry.getValue().containsKey(metric)) {
                        logMsg.append(String.format("%.2f", entry.getValue().get(metric)) + ",");
                    } else {
                        logMsg.append("-1.0,");
                    }
                }
            }
        }

        if (logger.isDebugEnabled()) {
            // Inside if to avoid substring operation cost if not needed
            logger.debug("serialized state:\n{}", logMsg.substring(0, logMsg.length() - 1));
        }

        // Keep track of state latency and compressiong
        latStatusInStates.add(isLatencyGreaterThanOrEqualToThreshold());
        latestCompressionsInReportedStates.add(retrieveLatestCompressionValueInState());
        logger.debug("Stored latency above treshold {}, latest compression {}",
                latStatusInStates
                        .get(latStatusInStates.size() - 1),
                latestCompressionsInReportedStates
                        .get(latestCompressionsInReportedStates.size() - 1));

        return logMsg.substring(0, logMsg.length() - 1);
    }

    /**
     * Called when measurements over the specified monitoring period are avaible,
     * for the specialized class to try to compute a state measurement a reward
     * 
     * @return True if a state measurement and a Reward are available
     */
    public boolean areRewardAndNewStateMeasurementAvailable() {

        for (String metric : relevantMetrics) {
            if (measurements.containsKey(metric)) {
                for (Pair<Long, Double> measurement : measurements.get(metric)) {
                    if (!stateMeasurements.containsKey(measurement.getTimestamp())) {
                        stateMeasurements.put(measurement.getTimestamp(), new HashMap<>());
                    }
                    stateMeasurements.get(measurement.getTimestamp()).put(metric, measurement.getValue());
                }
            }
        }

        double lastEventTimeDouble = -1;
        for (Entry<Long, HashMap<String, Double>> entry : stateMeasurements.entrySet()) {
            if (entry.getValue().containsKey("eventtime")) {
                lastEventTimeDouble = Math.max(entry.getValue().get("eventtime"), lastEventTimeDouble);
            }
        }
        long lastEventTime = (long) lastEventTimeDouble;
        boolean ready = stateMeasurements.lastKey() >= clockTimeBarrier && lastEventTime >= eventTimeBarrier
                && stateMeasurements.size() > monitoringPeriod;
        // The following was also part of the ready check, but in principle it should
        // not be there otherwise we cannot enfore the AOB policy!
        // && stateMeasurements.lastKey() > lastReportedStateMaxTS
        if (ready) {
            logger.debug(
                    "State ready based on barriers (>=)? {} - clock time:{} clock time barrier:{} event time:{} event time barrier:{} lastReportedStateMaxTS:{}, lastReportedState.size():{}",
                    ready, stateMeasurements.lastKey(), clockTimeBarrier, lastEventTime, eventTimeBarrier,
                    lastReportedStateMaxTS, stateMeasurements.size());
        }

        return ready;

    }

    public long getReward() {

        logger.debug("\nComputing Reward (for timestamps greater than {})", lastReportedStateMaxTS);

        // Clearing earliest Dvalues, latency above threshold, compression above zero
        while (varDValues.size() > 2) {
            varDValues.remove(0);
        }
        while (latStatusInStates.size() > 2) {
            latStatusInStates.remove(0);
        }
        while (latestCompressionsInReportedStates.size() > 2) {
            latestCompressionsInReportedStates.remove(0);
        }
        logger.debug(
                "\nDValues: {}\nLatencies greater than/equal to threshold: {}\nlatest compression ratios: {}",
                varDValues,
                latStatusInStates,
                latestCompressionsInReportedStates);

        long reward = 0;

        if (varDValues.size() > 1 && latStatusInStates.size() > 1
                && latestCompressionsInReportedStates.size() > 1) {

            LatStatus pstLatStatus = latStatusInStates.get(0);
            LatStatus lstLatStatus = latStatusInStates.get(1);
            CompressionValue pstRatio = latestCompressionsInReportedStates.get(0);
            CompressionValue lstRatio = latestCompressionsInReportedStates.get(1);

            if (pstLatStatus == LatStatus.UNKNOWN) {
                logger.warn("Reward cannot be computed. Past latency unkown.");
                return 0;
            }
            if (lstLatStatus == LatStatus.UNKNOWN) {
                logger.warn("Reward cannot be computed. Last latency unkown.");
                return 0;
            }
            if (!pstRatio.valid) {
                logger.warn("Reward cannot be computed. Past ratio unkown.");
                return 0;
            }
            if (!lstRatio.valid) {
                logger.warn("Reward cannot be computed. Last ratio unkown.");
                return 0;
            }

            if (lstRatio.value < pstRatio.value && lstLatStatus == LatStatus.BELOW) {
                reward = Math.round(Math.pow(pstRatio.value - lstRatio.value, 0.5));
            }

            // logger.debug("Returning always 0!");
            // reward = 0;

        }

        lastReportedStateMaxTS = stateMeasurements.lastKey();
        logger.debug("Reward computed, lastReportedStateMaxTS updated to {}", lastReportedStateMaxTS);
        logger.debug("\n*************\n* Reward: {}\n*************\n", reward);

        while (!stateMeasurements.isEmpty()
                && stateMeasurements.firstKey() < lastReportedStateMaxTS - monitoringPeriod) {
            stateMeasurements.pollFirstEntry();
        }

        return reward;
    }

    private LatStatus isLatencyGreaterThanOrEqualToThreshold() {
        boolean aboveHardThreshold = false;
        boolean found = false;
        for (Entry<Long, HashMap<String, Double>> m : stateMeasurements.entrySet()) {
            if (m.getKey() > lastReportedStateMaxTS) {
                if (m.getValue().containsKey("latency") && Double.compare(m.getValue().get("latency"), -1.0) != 0) {
                    found = true;
                    logger.debug("Returning a LatStatus because of the state entry {}-{}", m.getKey(),
                            m.getValue().get("latency"));
                    if (m.getValue().get("latency") >= hardLatencyThreshold) {
                        aboveHardThreshold = true;
                    }
                }
            }
        }
        if (!found) {
            logger.warn(
                    "There seems to be no latency value in the latest state measurements (considering values greater than {})",
                    lastReportedStateMaxTS);
        }
        return found ? (aboveHardThreshold ? LatStatus.ABOVE : LatStatus.BELOW) : LatStatus.UNKNOWN;
    }

    private CompressionValue retrieveLatestCompressionValueInState() {
        double value = -1.0;
        boolean valid = false;
        for (Entry<Long, HashMap<String, Double>> m : stateMeasurements.entrySet()) {
            if (m.getKey() > lastReportedStateMaxTS) {
                if (m.getValue().containsKey("ratio") && Double.compare(m.getValue().get("ratio"), -1.0) != 0) {
                    logger.debug("Returning a CompressionValue because of the state entry {}-{}", m.getKey(),
                            m.getValue().get("ratio"));
                    value = m.getValue().get("ratio");
                    valid = true;
                }
            }
        }
        if (!valid) {
            logger.warn(
                    "There seems to be no ratio value in the latest state measurements (considering values greater than {})",
                    lastReportedStateMaxTS);
        }
        return new CompressionValue(value, valid);
    }

    private String stateFormatter(TreeMap<Long, HashMap<String, Double>> newState) {

        String formattedState = "";

        int columns = (int) (newState.lastKey() - newState.firstKey()) + 2;
        int rows = relevantMetrics.size() + 1;

        String[][] data = new String[rows][columns];
        data[0][0] = "";
        for (long ts : newState.keySet()) {
            int column = (int) (ts - newState.firstKey()) + 1;
            data[0][column] = "" + ts;
            for (int i = 0; i < relevantMetrics.size(); i++) {
                String metric = relevantMetrics.get(i);
                data[i + 1][0] = metric;
                if (newState.get(ts).containsKey(metric)) {
                    data[i + 1][column] = String.format("%.2f", newState.get(ts).get(metric));
                } else {
                    data[i + 1][column] = "-1.0";
                }
            }

        }

        // Calculate the maximum width for each column
        int[] maxWidths = new int[data[0].length];
        for (String[] row : data) {
            for (int i = 0; i < row.length; i++) {
                maxWidths[i] = Math.max(maxWidths[i], row[i].length());
            }
        }

        // Print the data with aligned columns
        for (String[] row : data) {
            for (int i = 0; i < row.length; i++) {
                formattedState += String.format("%-" + (maxWidths[i] + 2) + "s", row[i]); // +2 for padding
            }
            formattedState += "\n";
        }

        return formattedState;

    }

    public String getExtraInfo() {
        return "" + numberOfLatenciesExceedingEarlyTerminationThreshold + "/"
                + numberOfCPUsExceedingEarlyTerminationThreshold;
    }

}
