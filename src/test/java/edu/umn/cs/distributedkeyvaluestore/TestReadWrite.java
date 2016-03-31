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
public class TestReadWrite {

    @Test
    public void testFileNotFound() throws Exception {
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

            ReadResponse readResponse = client.read("foo.txt");
            assertEquals(Status.FILE_NOT_FOUND, readResponse.getStatus());

            coordinator.stopCoordinatorService();
        } finally {
            // to make sure 9090 port is closed before next test run
            coordinator.stopCoordinatorService();
        }
    }

    @Test
    public void testFileRead() throws Exception {
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

            // server 2
            TTransport nodeSocket = new TSocket("localhost", port);
            nodeSocket.open();
            TProtocol protocol = new TBinaryProtocol(nodeSocket);
            FileServerEndPoints.Client client = new FileServerEndPoints.Client(protocol);

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

            ReadResponse readResponse = client.read("foo.txt");
            assertEquals(Status.SUCCESS, readResponse.getStatus());
            assertEquals("bar", readResponse.getContents());
            assertEquals(0, readResponse.getVersion());
        } finally {
            // to make sure 9090 port is closed before next test run
            coordinator.stopCoordinatorService();
        }
    }

    @Test
    public void testFileReadLatestVersion() throws Exception {
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

            writeResponse = client.write("foo.txt", "bar1");
            assertEquals(Status.SUCCESS, writeResponse.getStatus());
            assertEquals(1, writeResponse.getVersion());
            assertEquals(4, writeResponse.getBytesWritten());

            writeResponse = client.write("foo.txt", "bar11");
            assertEquals(Status.SUCCESS, writeResponse.getStatus());
            assertEquals(2, writeResponse.getVersion());
            assertEquals(5, writeResponse.getBytesWritten());

            // clients should see latest version
            ReadResponse readResponse = client.read("foo.txt");
            assertEquals(Status.SUCCESS, readResponse.getStatus());
            assertEquals("bar11", readResponse.getContents());
            assertEquals(2, readResponse.getVersion());
        } finally {
            // to make sure 9090 port is closed before next test run
            coordinator.stopCoordinatorService();
        }
    }

}
