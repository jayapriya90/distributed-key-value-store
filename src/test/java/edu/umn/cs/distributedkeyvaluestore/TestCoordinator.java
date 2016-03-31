package edu.umn.cs.distributedkeyvaluestore;

import edu.umn.cs.distributedkeyvaluestore.common.Utilities;
import edu.umn.cs.distributedkeyvaluestore.coordinator.Coordinator;
import edu.umn.cs.distributedkeyvaluestore.fileserver.FileServer;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Created by jayapriya on 3/25/16.
 */
public class TestCoordinator {

    @Test
    public void testCoordinatorWrongQuorum() throws Exception {
        // with 2 nodes quorum will not reached
        final Coordinator coordinator = new Coordinator(1, 1, 100000);
        try {
            Thread thread = new Thread(new Runnable() {
                @Override
                public void run() {
                    coordinator.startCoordinatorService();
                }
            });
            thread.start();
            Thread.sleep(1000);

            // quorum will not reached with 1 file server addition
            final int port = Utilities.getRandomPort();
            Thread thread2 = new Thread(new Runnable() {
                @Override
                public void run() {
                    FileServer fileServer = new FileServer("localhost");
                    fileServer.startService("localhost", port, "localhost");
                }
            });
            thread2.start();
            Thread.sleep(1000);

            TTransport nodeSocket = new TSocket("localhost", port);
            nodeSocket.open();
            TProtocol protocol = new TBinaryProtocol(nodeSocket);
            FileServerEndPoints.Client client = new FileServerEndPoints.Client(protocol);

            WriteResponse writeResponse = client.write("foo.txt", "bar");
            assertEquals(Status.SUCCESS, writeResponse.getStatus());
            assertEquals(0, writeResponse.getVersion());
            assertEquals(3, writeResponse.getBytesWritten());

            // add another node, quorum will not be reached as nr and nw
            // has to be 2 but we explicitly specified 1 above
            final int port2 = Utilities.getRandomPort();
            Thread thread3 = new Thread(new Runnable() {
                @Override
                public void run() {
                    FileServer fileServer = new FileServer("localhost");
                    fileServer.startService("localhost", port2, "localhost");
                }
            });
            thread3.start();
            Thread.sleep(1000);

            writeResponse = client.write("foo1.txt", "bar1");
            assertEquals(Status.SERVER_CANNOT_BE_CONTACTED, writeResponse.getStatus());
            nodeSocket.close();

            coordinator.stopCoordinatorService();
        } finally {
            // to make sure 9090 port is closed before next test run
            coordinator.stopCoordinatorService();
        }
    }

    @Test
    public void testCoordinatorCorrectQuorum() throws Exception {
        // with 2 nodes quorum will reached with nr=2 and nw=2
        final Coordinator coordinator = new Coordinator(2, 2, 100000);
        try {
            Thread thread = new Thread(new Runnable() {
                @Override
                public void run() {
                    coordinator.startCoordinatorService();
                }
            });
            thread.start();
            Thread.sleep(1000);

            // server 1
            // quorum will not reached with 1 file server addition
            final int port = Utilities.getRandomPort();
            Thread thread2 = new Thread(new Runnable() {
                @Override
                public void run() {
                    FileServer fileServer = new FileServer("localhost");
                    fileServer.startService("localhost", port, "localhost");
                }
            });
            thread2.start();
            Thread.sleep(1000);

            TTransport nodeSocket = new TSocket("localhost", port);
            nodeSocket.open();
            TProtocol protocol = new TBinaryProtocol(nodeSocket);
            FileServerEndPoints.Client client = new FileServerEndPoints.Client(protocol);

            // server 2
            // quorum will be reached with 2nd node addition
            final int port2 = Utilities.getRandomPort();
            Thread thread3 = new Thread(new Runnable() {
                @Override
                public void run() {
                    FileServer fileServer = new FileServer("localhost");
                    fileServer.startService("localhost", port2, "localhost");
                }
            });
            thread3.start();
            Thread.sleep(1000);

            WriteResponse writeResponse = client.write("foo.txt", "bar");
            assertEquals(Status.SUCCESS, writeResponse.getStatus());
            assertEquals(0, writeResponse.getVersion());
            assertEquals(3, writeResponse.getBytesWritten());

            writeResponse = client.write("foo1.txt", "bar1");
            assertEquals(Status.SUCCESS, writeResponse.getStatus());
            assertEquals(0, writeResponse.getVersion());
            assertEquals(4, writeResponse.getBytesWritten());
            nodeSocket.close();

            coordinator.stopCoordinatorService();
        } finally {
            // to make sure 9090 port is closed before next test run
            coordinator.stopCoordinatorService();
        }
    }
}
