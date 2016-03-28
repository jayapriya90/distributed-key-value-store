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

/**
 * usage: coordinator
 * -nr <arg>  Size of read quorum (default chosen by nr + nw > n rule)
 * -nw <arg>  Size of write quorum (default chosen by nw > n/2 rule)
 * -h        Help
 */

public class Coordinator {
    private static final Logger LOG = LoggerFactory.getLogger(Coordinator.class);

    private CoordinatorEndPointsImpl coordinatorEndPoints;
    private CoordinatorEndPoints.Processor coordinatorServiceProcessor;

    // coordinator also acts as fileserver
    private FileServerEndPointsImpl fileServerEndPoints;
    private FileServerEndPoints.Processor fileServerServiceProcessor;

    public Coordinator(int nr, int nw, String hostname) {
        this.coordinatorEndPoints = new CoordinatorEndPointsImpl(nr, nw);
        this.coordinatorServiceProcessor = new CoordinatorEndPoints.Processor(coordinatorEndPoints);
        FileServerInfo coordinator = new FileServerInfo(hostname, Constants.COORDINATOR_PORT);
        this.fileServerEndPoints = new FileServerEndPointsImpl(asyncPort, coordinator);
        this.fileServerServiceProcessor = new FileServerEndPoints.Processor(fileServerEndPoints);
    }

    private void startCoordinatorService() {
        try {
            TServerTransport serverTransport = new TServerSocket(Constants.COORDINATOR_PORT);
            // Use this for a multi-threaded server
            TServer server = new TThreadPoolServer(new TThreadPoolServer.Args(serverTransport).processor(
                    coordinatorServiceProcessor));

            LOG.info("Started the coordinator service at port: " + Constants.COORDINATOR_PORT);
            server.serve();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void startFileServerService(int port) {
        try {
            TServerTransport serverTransport = new TServerSocket(port);
            // Use this for a multi-threaded server
            TServer server = new TThreadPoolServer(new TThreadPoolServer.Args(serverTransport).processor(
                    fileServerServiceProcessor));

            LOG.info("Started the file server service at port: " + port);
            server.serve();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) throws Exception {
        HelpFormatter formatter = new HelpFormatter();

        // arguments that can be passed to this application
        Options options = new Options();
        options.addOption("nr", true, "Size of read quorum (default chosen by nr + nw > n rule)");
        options.addOption("nw", true, "Size of write quorum (default chosen by nw > n/2 rule)");
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

            String coordinatorHost = InetAddress.getLocalHost().getHostName();
            LOG.info("Starting coordinator with hostname: " + coordinatorHost);
            final Coordinator coordinator = new Coordinator(nr, nw, coordinatorHost);

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
            FileServer fileServer = new FileServer(asyncPort, coordinatorHost);
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
