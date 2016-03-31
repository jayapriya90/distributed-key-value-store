package edu.umn.cs.distributedkeyvaluestore.coordinator;

/**
 * Created by jayapriya on 3/25/16.
 */

import edu.umn.cs.distributedkeyvaluestore.CoordinatorEndPoints;
import edu.umn.cs.distributedkeyvaluestore.FileServerEndPoints;
import edu.umn.cs.distributedkeyvaluestore.FileServerInfo;
import edu.umn.cs.distributedkeyvaluestore.common.Constants;
import edu.umn.cs.distributedkeyvaluestore.common.Utilities;
import edu.umn.cs.distributedkeyvaluestore.fileserver.FileServer;
import edu.umn.cs.distributedkeyvaluestore.fileserver.FileServerEndPointsImpl;
import org.apache.commons.cli.*;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TServerTransport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.UnknownHostException;

/**
 * usage: coordinator
 * -nr <arg>  Size of read quorum (default chosen by nr + nw > n rule)
 * -nw <arg>  Size of write quorum (default chosen by nw > n/2 rule)
 * -si <arg>  Sync interval in seconds (default: 3 seconds)
 * -h        Help
 */

public class Coordinator {
    private static final Logger LOG = LoggerFactory.getLogger(Coordinator.class);

    private CoordinatorEndPointsImpl coordinatorEndPoints;
    private CoordinatorEndPoints.Processor coordinatorServiceProcessor;
    private TServer server = null;
    private TServerTransport serverTransport = null;

    public Coordinator(int nr, int nw, long syncInterval) {
        this.coordinatorEndPoints = new CoordinatorEndPointsImpl(nr, nw, syncInterval);
        this.coordinatorServiceProcessor = new CoordinatorEndPoints.Processor(coordinatorEndPoints);
    }

    public void startCoordinatorService() {
        try {
            serverTransport = new TServerSocket(Constants.COORDINATOR_PORT);
            // Use this for a multi-threaded server
            server = new TThreadPoolServer(new TThreadPoolServer.Args(serverTransport).processor(
                    coordinatorServiceProcessor));

            LOG.info("Started the coordinator service at port: " + Constants.COORDINATOR_PORT);
            server.serve();
        } catch (Exception e) {
            e.printStackTrace();
            if (serverTransport != null) {
                serverTransport.close();
            }
            if (server != null) {
                server.stop();
            }
        }
    }

    public void stopCoordinatorService() {
        if (serverTransport != null) {
            serverTransport.close();
        }
        if (server != null) {
            server.stop();
        }
    }

    public static void main(String[] args) throws Exception {
        HelpFormatter formatter = new HelpFormatter();

        // arguments that can be passed to this application
        Options options = new Options();
        options.addOption("nr", true, "Size of read quorum (default chosen by nr + nw > n rule)");
        options.addOption("nw", true, "Size of write quorum (default chosen by nw > n/2 rule)");
        options.addOption("si", true, "Sync interval (default: 5 seconds)");
        options.addOption("h", false, "Help");

        // command line parser for the above options
        CommandLineParser cliParser = new GnuParser();
        try {
            CommandLine cli = cliParser.parse(options, args);

            // print help
            if (cli.hasOption("h")) {
                formatter.printHelp("coordinator", options);
                return;
            }

            int nr = -1;
            int nw = -1;
            if (cli.hasOption("nr")) {
                nr = Integer.parseInt(cli.getOptionValue("nr"));
            }

            if (cli.hasOption("nw")) {
                nw = Integer.parseInt(cli.getOptionValue("nw"));
            }

            long syncInterval = Constants.SYNC_TASK_INTERVAL;
            if (cli.hasOption("si")) {
                // convert user specified seconds to milliseconds
                syncInterval = Integer.parseInt(cli.getOptionValue("si")) * 1000;
            }

            String coordinatorHost = InetAddress.getLocalHost().getHostName();
            LOG.info("Starting coordinator with hostname: " + coordinatorHost + " syncInterval: " + syncInterval + " ms");
            final Coordinator coordinator = new Coordinator(nr, nw, syncInterval);

            // start the services offered by coordinator in separate thread
            Runnable serviceThread = new Runnable() {
                public void run() {
                    coordinator.startCoordinatorService();
                }
            };
            new Thread(serviceThread).start();

            Thread.sleep(1000);

            // coordinator is also file server, start file server service on different port.
            // both use same hostname
            FileServer fileServer = new FileServer(coordinatorHost);
            final int port = Utilities.getRandomPort();
            String fileServerHost = coordinatorHost;
            fileServer.startService(fileServerHost, port, coordinatorHost);
        } catch (ParseException e) {

            // if wrong format is specified
            System.err.println("Invalid option.");
            formatter.printHelp("coordinator", options);
        }
    }
}
