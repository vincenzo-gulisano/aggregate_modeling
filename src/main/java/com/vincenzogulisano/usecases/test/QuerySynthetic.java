package com.vincenzogulisano.usecases.synthetic;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Random;
import java.util.function.Consumer;

import org.apache.commons.cli.ParseException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vincenzogulisano.javapythoncommunicator.Actionable;
import com.vincenzogulisano.javapythoncommunicator.EnvironmentMonitor;
import com.vincenzogulisano.javapythoncommunicator.PolicyBarrier;
import com.vincenzogulisano.javapythoncommunicator.PolicyBarrierCalculator;
import com.vincenzogulisano.javapythoncommunicator.StatReporter;
import com.vincenzogulisano.usecases.linearroad.SinkLogAndLatency;
import com.vincenzogulisano.usecases.linearroad.InjectorType;
import com.vincenzogulisano.util.EpisodesLogger;
import com.vincenzogulisano.util.ExperimentOptions;
import com.vincenzogulisano.util.ThreadCPUMonitor;
import com.vincenzogulisano.woost.WoostAggregateWithCompression;

import common.metrics.Metrics;
import common.util.Util;
import component.operator.Operator;
import component.sink.Sink;
import component.sink.SinkFunction;
import component.source.Source;
import query.LiebreContext;
import query.Query;

public class QuerySynthetic implements Actionable, EnvironmentMonitor {

    private Query q = new Query();
    private long experimentLength;
    private WoostAggregateWithCompression<TupleInput, TupleInput> woostAgg;
    private SourceReadFromFile sourceFunction;
    private SinkLogAndLatency<TupleInput> sink;
    private ThreadCPUMonitor threadCPUMonitor;
    private long valueDAtEpisodeStart;
    private String statsFolder;
    private StatReporter reporter;
    private EpisodesLogger episodesLogger;
    private boolean firstEpisodeStarted;
    private long startingTimeMinimum;
    private long startingTimeMaximum;
    private long wa;
    private long ws;
    private Random r;
    private long randomSeed;
    private boolean randomizeSeed;
    private PolicyBarrier policyBarrier;

    public final static long sleepBeforeRealRate = 1000;

    // The name of this Logger will be "org.apache.logging.Child"
    public Logger logger = LogManager.getLogger();

    public static void main(String[] args) throws ParseException, IOException {
        QuerySynthetic q = new QuerySynthetic();
        q.createQuery(args);
        q.setStatReporter(new StatReporter() {

            @Override
            public void report(long ts, String id, double value) {

            }

            @Override
            public void setResetRequest() {

            }

            @Override
            public boolean getResetAcknowledged() {
                return true;
            }

            @Override
            public void setResetCompleted() {

            }

            @Override
            public void addSendStateToken(long clockTimeBarrier, long eventTimeBarrier, long dValue) {

            }

            @Override
            public void registerLogger(EpisodesLogger logger) {

            }

            @Override
            public void close() {

            }

        });
        q.activateQuery();
        Util.sleep(q.experimentLength);
        q.close();
        q.q.deActivate();

    }

    public void createQuery(String[] args)
            throws ParseException, IOException {

        ExperimentOptions expOps = new ExperimentOptions(args);

        statsFolder = expOps.commandLine().getOptionValue("s");
        String inputFile = expOps.commandLine().getOptionValue("i");
        valueDAtEpisodeStart = Long.parseLong(expOps.commandLine().getOptionValue("d", String.valueOf(Long.MAX_VALUE)));
        experimentLength = Long.parseLong(expOps.commandLine().getOptionValue("l"));
        wa = Long.parseLong(expOps.commandLine().getOptionValue("wa"));
        ws = Long.parseLong(expOps.commandLine().getOptionValue("ws"));
        String outPath = expOps.commandLine().getOptionValue("o", "");
        boolean writeOut = outPath.equals("") ? false : true;
        InjectorType type = InjectorType
                .valueOf(expOps.commandLine().getOptionValue("t", String.valueOf(InjectorType.FIXEDRATE)));
        startingTimeMinimum = Long.valueOf(expOps.commandLine().getOptionValue("stmin",
                String.valueOf(0)));
        startingTimeMaximum = Long.valueOf(expOps.commandLine().getOptionValue("stmax",
                String.valueOf(0)));
        randomSeed = Long.valueOf(expOps.commandLine().getOptionValue("randomSeed", String.valueOf(0L)));
        randomizeSeed = Boolean.valueOf(expOps.commandLine().getOptionValue("rer", "False"));
        policyBarrier = PolicyBarrier.valueOf(expOps.commandLine().getOptionValue("pb", "WEAAW"));

        r = new Random(0);

        episodesLogger = new EpisodesLogger(statsFolder + File.separator + "episodes.csv");
        firstEpisodeStarted = false;

        sourceFunction = new SourceReadFromFile(inputFile, type, startingTimeMinimum, ws);

        sink = new SinkLogAndLatency<>("out", new SinkFunction<TupleInput>() {

            @Override
            public void accept(TupleInput arg0) {
            }

        }, writeOut, outPath);

        Source<TupleInput> s = q.addBaseSource("in", sourceFunction);

        woostAgg = new WoostAggregateWithCompression<>("agg",
                0, 1, ws, wa, new WindowSynthetic(), valueDAtEpisodeStart);

        Operator<TupleInput, TupleInput> agg = q.addOperator(woostAgg);

        Sink<TupleInput> o1 = q.addSink(sink);

        q.connect(s, agg).connect(agg, o1);

        threadCPUMonitor = new ThreadCPUMonitor(List.of("in", "agg", "out"));

    }

    public void activateQuery() {

        q.activate();
        threadCPUMonitor.startMonitoring();

        // Forcing a "reset" here to make sure we wait for the injector from the very
        // first episode
        reset();

    }

    @Override
    public void setStatReporter(StatReporter reporter) {

        this.reporter = reporter;
        this.reporter.registerLogger(episodesLogger);

        logger.debug("SPE - setStatReporter invoked");
        HashMap<String, Consumer<Object[]>> consumers = new HashMap<>();

        logger.debug("SPE - preparing consumers");
        consumers.putAll(sourceFunction.setStatReporter(reporter));
        consumers.putAll(woostAgg.setStatReporter(reporter));
        consumers.putAll(sink.setStatReporter(reporter));
        consumers.putAll(threadCPUMonitor.setStatReporter(reporter));

        logger.debug("SPE - Setting metrics type in Liebre");
        LiebreContext.setUserMetrics(Metrics.fileAndConsumer(statsFolder, consumers));

        logger.debug("SPE - Creating statistics");
        sourceFunction.createStatistics();
        woostAgg.createStatistics();
        sink.createStatistics();
        threadCPUMonitor.createStatistics();

    }

    public long getWS_WA_Ceil() {
        return (long) Math.ceil((double) ws / (double) wa);
    }

    public long getContributingWindows(long ts) {
        return ts % wa >= ws % wa && ws % wa != 0L ? (getWS_WA_Ceil() - 1) : getWS_WA_Ceil();
    }

    public long getEarliestWinStartTS(long ts) {
        return (long) Math.max((double) ((ts / wa - this.getContributingWindows(ts) + 1L) * wa), 0.0);
    }

    @Override
    public void changeD(long v) {
        logger.debug("SPE - changeD invoked");
        episodesLogger.writeActionEvent(Long.toString(v));
        long newCompression = (long) ((double) ws * ((double) v / 10.0));
        long latestEventTime = woostAgg.changeD(newCompression);
        long latestClockTime = System.currentTimeMillis() / 1000;
        logger.debug("Since D has changed, adding a token to the state monitor");
        PolicyBarrierCalculator barrier = PolicyBarrierCalculator.getBarriers(policyBarrier, latestClockTime,
                latestEventTime, wa, ws);
        logger.debug(
                "D changed to {} at event time {} and clock time {}. Barriers: event time >= {} and clock time >= {}",
                v, latestEventTime, latestClockTime, barrier.getEventTimeBarrier(), barrier.getWallclockTimeBarrier());
        reporter.addSendStateToken(barrier.getWallclockTimeBarrier(), barrier.getEventTimeBarrier(), v);
    }

    @Override
    public void reset() {

        logger.debug("SPE - Got a RESET request");

        if (randomizeSeed) {
            r = new Random(System.currentTimeMillis());
        } else {
            r = new Random(randomSeed);
        }

        long startingTS = startingTimeMinimum + r.nextInt((int) (startingTimeMaximum - startingTimeMinimum) + 1);
        logger.debug("SPE - Updating source starting time to " + startingTS);
        sourceFunction.setStartingTS(startingTS);

        logger.debug("SPE - Synchronizing with Source to initiate the procedure");
        sourceFunction.registerResetRequest();

        if (!firstEpisodeStarted) {
            logger.debug(
                    "This is the first episode, and the source has not been authorized to start sending tuples. Authorizing it before continuing with the reset");
            sourceFunction.notifyFirstEpisodeCanStart();
        }

        if (firstEpisodeStarted) {
            episodesLogger.writeEndEvent();
        }

        while (!sourceFunction.getResetAck()) {
            Util.sleep(50);
        }
        logger.debug("SPE - The source is no longer injecting tuples, resetting Agg, Sink, and Source");
        woostAgg.reset();
        while (!woostAgg.getResetAck()) {
            Util.sleep(50);
        }
        logger.debug("Got Ack from the Agg");
        sink.reset();
        logger.debug("Sink reset");

        long newCompression = (long) ((double) ws * ((double) valueDAtEpisodeStart / 10.0));
        logger.debug("Reset compression threshold of the Aggregate to {}", newCompression);
        long changeDEventTime = woostAgg.changeD(newCompression);
        logger.debug("latestEventTime returned by changeD: {} (not used, logging to make sure we reach this point)",
                changeDEventTime);

        logger.debug("invoking giveGreenlightToStartSendingStateFillingTuples");
        sourceFunction.giveGreenlightToStartSendingStateFillingTuples();
        logger.debug("giveGreenlightToStartSendingStateFillingTuples invoked");

        while (!sourceFunction.areAllStateFillingTuplesSent()) {
            Util.sleep(50);
        }
        logger.debug("SPE - the source has sent all the state filling tuples too");

        logger.debug("Everything is ready to start. Sleeping 3 seconds to let the CPU rest and not pollute stats");
        Util.sleep(3000);
        logger.debug("Let's go!");

        sourceFunction.giveGreenlightToStartSendingRealRateTuples();

        firstEpisodeStarted = true;
        episodesLogger.writeStartEvent();

        logger.debug("Resetting the EnvironmentStateCalculator");
        reporter.setResetRequest();
        while (!reporter.getResetAcknowledged()) {
            Util.sleep(50);
        }
        reporter.setResetCompleted();
        logger.debug("EnvironmentStateCalculator is now reset");

        logger.debug("Since the reset is complete, adding a token to the state monitor");
        long latestEventTime = woostAgg.getLatestEventTime();
        long latestClockTime = System.currentTimeMillis() / 1000;
        // In this case I pass the barriers automatically since it is the beginning of
        // an episode
        PolicyBarrierCalculator barrier = PolicyBarrierCalculator.getBarriers(policyBarrier, latestClockTime,
                latestEventTime, wa, ws);
        logger.debug(
                "Reset completed at event time {} and clock time {}. Barriers: event time >= {} and clock time >= {}",
                latestEventTime, latestClockTime, barrier.getEventTimeBarrier(), barrier.getWallclockTimeBarrier());
        reporter.addSendStateToken(barrier.getWallclockTimeBarrier(), barrier.getEventTimeBarrier(),
                valueDAtEpisodeStart);

    }

    @Override
    public void close() {
        logger.debug("Received close command");

        // Log the end of the final episode
        episodesLogger.writeEndEvent();
        episodesLogger.writeCloseEvent();
        episodesLogger.close();

        threadCPUMonitor.stopMonitoring();
    }

}
