package com.vincenzogulisano.util;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

public class ExperimentOptions {

    private CommandLine commandLine;

    public ExperimentOptions(String[] args) throws ParseException {
        Options options = new Options();
        options.addOption("i", "inputFile", true, "Input file path");
        options.addOption("s", "statsFolder", true, "Output folder for stats");
        options.addOption("d", "compressionThreshold", true, "Defines the compression threshold");
        options.addOption("l", "experimentLength", true, "Length of the experiment in milliseconds");
        options.addOption("wa", "windowAdvance", true, "Aggregate's window advance");
        options.addOption("ws", "windowSize", true, "Aggregate's window size");
        options.addOption("o", "outputFile", true, "File to output tuples");
        options.addOption("t", "injectorType", true, "Type of injector");
        options.addOption("stmin", "startingTimeMinimum", true, "minimum starting time for RL");
        options.addOption("stmax", "startingTimeMaximum", true, "maximum starting time for RL");
        options.addOption("log4j", "log4jConfigFile", true, "log4j config file");
        options.addOption("rer", "randomizeEpisodeRate", true,
                "If true, each episode resets the random seed to the current time");
        options.addOption("randomSeed", "randomizeEpisodeRate", true, "long randomg seed");
        options.addOption("usecase", "usecase", true, "Which usecase to run");
        options.addOption("pb", "policyBarrier", true, "Which policy to use for the barrier");
        options.addOption("bs", "bootstrapServer", true, "boostrapServer");
        options.addOption("lt", "latencyTreshold", true, "Which policy to use for the barrier");
        options.addOption("ct", "CPUTreshold", true, "CPUTreshold");
        options.addOption("lt", "latencyTreshold", true, "latencyTreshold");
        options.addOption("etlt", "earlyTerminationLatencyThreshold", true, "earlyTerminationLatencyThreshold");

        CommandLineParser parser = new DefaultParser();
        this.commandLine = parser.parse(options, args);
    }

    public CommandLine commandLine() {
        return commandLine;
    }

}
