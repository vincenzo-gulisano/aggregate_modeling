package com.vincenzogulisano.javapythoncommunicator;

import java.io.File;
import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

import org.apache.commons.cli.ParseException;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vincenzogulisano.usecases.linearroad.QueryCountConsecutiveStops;
import com.vincenzogulisano.usecases.synthetic.QuerySynthetic;
import com.vincenzogulisano.util.ExperimentOptions;

public class JPComm {

    private Actionable actionable;
    private Properties properties;
    private Producer<String, String> producer;
    private Consumer<String, String> consumer;
    private EnvironmentStateCalculator esc;

    public Logger logger = LogManager.getLogger();

    private JPComm(Actionable actionable, long latencyTreshold, double CPUThreshold,
            double earlyTerminationLatencyThreshold, String statsFolder, String bootstrapServer) {
        this.actionable = actionable;

        properties = new Properties();
        properties.put("bootstrap.servers", bootstrapServer);
        properties.put("group.id", "0");
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("key.deserializer", StringDeserializer.class.getName());
        properties.put("value.deserializer", StringDeserializer.class.getName());

        producer = new KafkaProducer<>(properties);
        consumer = new KafkaConsumer<>(properties);
        consumer.subscribe(Collections.singletonList("dchanges"));
        esc = new EnvironmentStateCalculator(7, producer, "/", 7, latencyTreshold, CPUThreshold,
                earlyTerminationLatencyThreshold, statsFolder + File.separator + "rewards.actions.csv");

    }

    public static JPComm createInstance(Actionable actionable, EnvironmentMonitor monitor, long latencyTreshold,
            double CPUThreshold, double earlyTerminationLatencyThreshold, String statsFolder, String bootstrapServer) {
        JPComm jpc = new JPComm(actionable, latencyTreshold, CPUThreshold, earlyTerminationLatencyThreshold,
                statsFolder, bootstrapServer);
        monitor.setStatReporter(jpc.esc);
        return jpc;
    }

    public void startInternalThread() {
        try {

            Thread reportingThread = new Thread(() -> {

                while (true) {
                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
                    records.forEach(record -> {
                        // Parse and process the received message
                        logger.debug("Received record " + record);
                        String[] parts = record.value().split(",");
                        if (parts[0].equals("changeD")) {
                            String action = parts[1];
                            Long change = Long.parseLong(action);
                            logger.debug("Got action " + change);
                            actionable.changeD(change);
                        } else if (parts[0].equals("reset")) {
                            logger.debug("Got a reset request");
                            actionable.reset();
                        } else if (parts[0].equals("close")) {
                            logger.debug("Closing SPE");
                            actionable.close();
                            logger.debug("Closing reporter (and producer)");
                            esc.close();
                            logger.debug("Closing consumer");
                            consumer.close();
                            System.exit(0);
                        } else {
                            throw new RuntimeException("Unknown command " + record.value());
                        }
                    });
                }
            });

            // Set the thread as a daemon so it doesn't prevent the program from exiting
            reportingThread.setDaemon(true);

            // Start the thread
            reportingThread.start();

        } catch (

        Exception e) {
            System.out.println(e);
        }

    }

    public static void main(String[] args) throws InterruptedException, ParseException, IOException {

        ExperimentOptions expOps = new ExperimentOptions(args);

        String usecase = expOps.commandLine().getOptionValue("usecase", "LinearRoad");
        String statsFolder = expOps.commandLine().getOptionValue("statsFolder");
        String bootstrapServer = expOps.commandLine().getOptionValue("bootstrapServer");

        long latencyThreshold = Long.valueOf(expOps.commandLine().getOptionValue("latencyTreshold", "1500"));
        double CPUThreshold = Double.valueOf(expOps.commandLine().getOptionValue("CPUTreshold", "100"));
        double earlyTerminationLatencyThreshold = Double
                .valueOf(expOps.commandLine().getOptionValue("earlyTerminationLatencyThreshold", "2000"));

        switch (usecase) {
            case "LinearRoad":

                QueryCountConsecutiveStops q = new QueryCountConsecutiveStops();
                q.createQuery(args);
                JPComm jpc = JPComm.createInstance(q, q, latencyThreshold, CPUThreshold,
                        earlyTerminationLatencyThreshold, statsFolder, bootstrapServer);
                jpc.startInternalThread();
                q.activateQuery();

                break;

            case "Synthetic":

                QuerySynthetic q2 = new QuerySynthetic();
                q2.createQuery(args);
                JPComm jpc2 = JPComm.createInstance(q2, q2, latencyThreshold, CPUThreshold,
                        earlyTerminationLatencyThreshold, statsFolder, bootstrapServer);
                jpc2.startInternalThread();
                q2.activateQuery();

                break;
            
            case "Synthetic5s":

                QuerySynthetic q3 = new QuerySynthetic();
                q3.createQuery(args);
                JPComm jpc3 = JPComm.createInstance(q3, q3, latencyThreshold, CPUThreshold,
                        earlyTerminationLatencyThreshold, statsFolder, bootstrapServer);
                jpc3.startInternalThread();
                q3.activateQuery();

                break;

            default:
                throw new RuntimeException("Unkown usecase " + usecase);
        }

    }

}
