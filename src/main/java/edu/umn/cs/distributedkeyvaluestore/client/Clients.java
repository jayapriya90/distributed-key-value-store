package edu.umn.cs.distributedkeyvaluestore.client;

import edu.umn.cs.distributedkeyvaluestore.*;
import edu.umn.cs.distributedkeyvaluestore.common.Constants;
import edu.umn.cs.distributedkeyvaluestore.fileserver.FileServer;
import org.apache.commons.cli.*;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.concurrent.*;

/**
 * Created by jayapriya on 3/27/16.
 */

/**
 * usage: clients
 * -i <arg>  CSV list of request sequence in filename=contents format.
 * Example: file1,file2,file3=value3 is RRW (read, read, write) sequence.
 * -f <arg>  File containing each line with read/write request in filename=contents format.
 * Example: input.txt contents for RRW sequence
 * file1
 * file2
 * file3=value3
 * -n <arg>  Number of concurrent clients (Each client will run in a thread)
 * -h <arg>  Hostname for coordinator (default: localhost)
 * -p        Print file servers metadata
 * --help    Help
 */
public class Clients {
    private static final Logger LOG = LoggerFactory.getLogger(Clients.class);

    public static void main(String[] args) throws FileNotFoundException {
        // input is either
        // 1) csv of read, write operations. For read operations simply specify the filename to read.
        //    For write operation specify in filename=contents format.
        // 2) specify input file with each line in the above format
        HelpFormatter formatter = new HelpFormatter();

        // arguments that can be passed to this application
        Options options = new Options();
        options.addOption("i", true, "CSV list of request sequence in filename=contents format.\n" +
                "Example: file1,file2,file3=value3 is RRW (read, read, write) sequence.");
        options.addOption("f", true, "File containing each line with read/write request in filename=contents format.\n" +
                "Example: input.txt contents for RRW sequence\n" +
                "file1\n" +
                "file2\n" +
                "file3=value3");
        options.addOption("n", true, "Number of concurrent clients (Each client will run in a thread)");
        options.addOption("h", true, "Hostname for coordinator (default: localhost)");
        options.addOption("p", false, "Print file servers metadata");
        options.addOption("help", false, "Help");

        // command line parser for the above options
        CommandLineParser cliParser = new GnuParser();
        try {
            CommandLine cli = cliParser.parse(options, args);

            // print help
            if (cli.hasOption("help")) {
                formatter.printHelp("clients", options);
                return;
            }

            String coordinatorHost = "localhost";
            if (cli.hasOption("h")) {
                coordinatorHost = cli.getOptionValue("h");
            }

            // only -p is specified
            if (cli.hasOption("p") && (!cli.hasOption("f") && !cli.hasOption("i"))) {
                printFileServerMetadata(coordinatorHost);
                return;
            }

            if (!cli.hasOption("f") && !cli.hasOption("i")) {
                System.err.println("-f or -i option has to be specified.");
                formatter.printHelp("client", options);
                return;
            }

            if (cli.hasOption("f") && cli.hasOption("i")) {
                System.err.println("Both -f and -i option cannot be specified. Specify either one.");
                formatter.printHelp("client", options);
                return;
            }

            int nClients = 1;
            List<String> input = null;
            if (cli.hasOption("f")) {
                String inputFilename = cli.getOptionValue("f");
                input = readFile(inputFilename);
            }

            if (cli.hasOption("i")) {
                String[] tokens = cli.getOptionValue("i").split(",");
                input = new ArrayList<String>();
                for (String token : tokens) {
                    input.add(token);
                }
            }

            if (cli.hasOption("n")) {
                nClients = Integer.parseInt(cli.getOptionValue("n"));
            }

            LOG.info("#clients: " + nClients + ". Starting thread pool of size: " + nClients + " for each client");
            ExecutorService executorService = Executors.newFixedThreadPool(nClients);

            if (input != null) {
                LOG.info("Total requests: " + input.size());
                List<String> reqSequence = getRequestSequence(input);
                LOG.info("Request sequence: " + reqSequence);
                LOG.info("Requests: " + input);
            }

            List<Future<?>> futures = new ArrayList<Future<?>>();
            for (String request : input) {
                FileServerInfo randomServer = getRandomServerFromCoordinator(coordinatorHost);
                RequestThread callable = new RequestThread(request, randomServer);
                Future<?> futureResponse = executorService.submit(callable);
                futures.add(futureResponse);
            }

            for (int i = 0; i < futures.size(); i++) {
                try {
                    String request = input.get(i);
                    Object response = futures.get(i).get();
                    if (response instanceof ReadResponse) {
                        ReadResponse readResponse = (ReadResponse) response;
                        LOG.info("Got read response: " + readResponse + " for request: " + request);
                    }

                    if (response instanceof WriteResponse) {
                        WriteResponse writeResponse = (WriteResponse) response;
                        LOG.info("Got write response: " + writeResponse + " for request: " + request);
                    }
                } catch (InterruptedException e) {
                    e.printStackTrace();
                } catch (ExecutionException e) {
                    e.printStackTrace();
                }
            }

            executorService.shutdownNow();

            if (cli.hasOption("p")) {
                printFileServerMetadata(coordinatorHost);
                return;
            }
        } catch (ParseException e) {

            // if wrong format is specified
            System.err.println("Invalid option.");
            formatter.printHelp("clients", options);
        } catch (TException e) {
            e.printStackTrace();
        }
    }

    private static void printFileServerMetadata(String coordinatorHost) throws TException {
        TTransport nodeSocket = new TSocket(coordinatorHost, Constants.COORDINATOR_PORT);
        try {
            nodeSocket.open();
            TProtocol protocol = new TBinaryProtocol(nodeSocket);
            CoordinatorEndPoints.Client client = new CoordinatorEndPoints.Client(protocol);
            Map<FileServerInfo, FileServerMetaData> metadata = client.getMetadata();

            System.out.println("\n");
            System.out.println("-----------------------------------------------------------------------------");
            System.out.println("                         FILE SERVER METADATA                                ");
            System.out.println("-----------------------------------------------------------------------------");
            for (Map.Entry<FileServerInfo, FileServerMetaData> entry : metadata.entrySet()) {
                System.out.println("Metadata from server: " + entry.getKey());
                for (FileInfo fileInfo : entry.getValue().getFileinfos()) {
                    System.out.println(fileInfo);
                }
                System.out.println("\n");
            }
            System.out.println("-----------------------------------------------------------------------------");
        } finally {
            if (nodeSocket != null) {
                nodeSocket.close();
            }
        }
    }

    private static FileServerInfo getRandomServerFromCoordinator(String coordinatorHost) throws TException {
        LOG.info("Connecting to coordinator at " + coordinatorHost + " to get random node for client.");
        TTransport nodeSocket = new TSocket(coordinatorHost, Constants.COORDINATOR_PORT);
        try {
            nodeSocket.open();
            TProtocol protocol = new TBinaryProtocol(nodeSocket);
            CoordinatorEndPoints.Client client = new CoordinatorEndPoints.Client(protocol);
            FileServerResponse fileServerResponse = client.getFileServer();
            if (fileServerResponse.getStatus().equals(Status.NO_NODE_FOUND)) {
                LOG.error("No nodes found in the system for clients to connect!");
                return null;
            }

            LOG.info("Returning node: " + fileServerResponse.getFileServerInfo() + " for client");
            return fileServerResponse.getFileServerInfo();
        } finally {
            if (nodeSocket != null) {
                nodeSocket.close();
            }
        }
    }

    private static List<String> getRequestSequence(List<String> input) {
        List<String> seq = new ArrayList<String>();
        for (String row : input) {
            if (row.contains("=")) {
                seq.add("W");
            } else {
                seq.add("R");
            }
        }
        return seq;
    }

    private static List<String> readFile(String inputFilename) throws FileNotFoundException {
        List<String> input = new ArrayList<String>();
        Scanner scanner = new Scanner(new File(inputFilename));
        while (scanner.hasNext()) {
            input.add(scanner.nextLine());
        }
        return input;
    }

    private static class RequestThread implements Callable<Object> {

        private String requestString;
        private FileServerInfo server;

        public RequestThread(String request, FileServerInfo fileServerInfo) {
            this.requestString = request;
            this.server = fileServerInfo;
        }

        @Override
        public Object call() throws Exception {
            TTransport nodeSocket = new TSocket(server.getHostname(), server.getPort());
            try {
                nodeSocket.open();
                TProtocol protocol = new TBinaryProtocol(nodeSocket);
                FileServerEndPoints.Client client = new FileServerEndPoints.Client(protocol);
                if (requestString.contains("=")) {
                    // write request
                    String[] tokens = requestString.split("=");
                    String filename = tokens[0];
                    String contents = tokens[1];
                    LOG.info("Sending write request (filename: " + filename + " contents: " + contents + ") to " + server);
                    WriteResponse writeResponse = client.write(filename, contents);
                    return writeResponse;
                } else {
                    // read request
                    LOG.info("Sending read request (filename: " + requestString + ") to " + server);
                    ReadResponse readResponse = client.read(requestString);
                    return readResponse;
                }
            } finally {
                if (nodeSocket != null) {
                    nodeSocket.close();
                }
            }
        }
    }
}
