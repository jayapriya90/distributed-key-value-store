package edu.umn.cs.distributedkeyvaluestore.fileserver;

import edu.umn.cs.distributedkeyvaluestore.CoordinatorEndPoints;
import edu.umn.cs.distributedkeyvaluestore.FileServerEndPoints;
import edu.umn.cs.distributedkeyvaluestore.FileServerInfo;
import edu.umn.cs.distributedkeyvaluestore.common.Constants;
import edu.umn.cs.distributedkeyvaluestore.common.Utilities;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TServerTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;

/**
 * Created by jayapriya on 3/25/16.
 */
public class FileServer {
    private static final Logger LOG = LoggerFactory.getLogger(FileServer.class);

    private FileServerEndPointsImpl fileServerEndPoints;
    private FileServerEndPoints.Processor fileServerServiceProcessor;

    public FileServer(String hostname, int asyncPort, String coordinatorHostname) {
        FileServerInfo coordinator = new FileServerInfo(coordinatorHostname, Constants.COORDINATOR_PORT);
        this.fileServerEndPoints = new FileServerEndPointsImpl(hostname, asyncPort, coordinator);
        this.fileServerServiceProcessor = new FileServerEndPoints.Processor(fileServerEndPoints);
    }

    public void startService(String hostname, int port, String coordinatorHost) {
        try {
            joinWithCoordinator(hostname, port, coordinatorHost);

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

    private void joinWithCoordinator(String hostname, int port, String coordinatorHost) throws TException {
        LOG.info("Connecting to Coordinator at hostname: " + coordinatorHost + " and port: " + Constants.COORDINATOR_PORT);
        // join with coordinator
        TTransport socket = new TSocket(coordinatorHost, Constants.COORDINATOR_PORT);
        socket.open();

        // create protocol for the superNodeSocket
        TProtocol protocol = new TBinaryProtocol(socket);

        // create the client for master's service
        CoordinatorEndPoints.Client client = new CoordinatorEndPoints.Client(protocol);
        client.join(hostname, port);
        LOG.info("Connected to coordinator with identity, hostname: " + hostname + " port: " + port);
        socket.close();
    }

    public static void main(String[] args) throws Exception {
        TTransport socket = null;
        try {
            final int port = Utilities.getRandomPort();
            final int asyncPort = Utilities.getRandomPort();
            final String hostname = InetAddress.getLocalHost().getHostName();
            final String coordinatorHostname = args.length > 0 ? args[0] : hostname;

            final FileServer fileServer = new FileServer(hostname, asyncPort, coordinatorHostname);
            // start file server service
            Runnable service = new Runnable() {
                public void run() {
                    fileServer.startService(hostname, port, coordinatorHostname);
                }
            };

            // start the service thread
            new Thread(service).start();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
