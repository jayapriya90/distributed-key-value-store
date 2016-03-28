package edu.umn.cs.distributedkeyvaluestore.coordinator;

import edu.umn.cs.distributedkeyvaluestore.FileServerEndPoints;
import edu.umn.cs.distributedkeyvaluestore.FileServerInfo;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * Created by jayapriya on 3/27/16.
 */
public class NonQuorumWriteThread implements Runnable {
    private static final Logger LOG = LoggerFactory.getLogger(NonQuorumWriteThread.class);
    private List<FileServerInfo> nonQuorum;
    private String filename;
    private String contents;
    private long version;

    public NonQuorumWriteThread(List<FileServerInfo> nonQuorum, String filename, String contents, long version) {
        this.nonQuorum = nonQuorum;
        this.filename = filename;
        this.contents = contents;
        this.version = version;
    }

    @Override
    public void run() {
        for (FileServerInfo serverInfo : nonQuorum) {
            TTransport nodeSocket = new TSocket(serverInfo.getHostname(), serverInfo.getPort());
            try {
                nodeSocket.open();
                TProtocol protocol = new TBinaryProtocol(nodeSocket);
                FileServerEndPoints.Client client = new FileServerEndPoints.Client(protocol);
                client.writeContents(filename, contents, version);
                LOG.info("Background thread - Successfully updated " + serverInfo + " about file: " + filename + " contents: " +
                        contents + " version: " + version);
            } catch (TException e) {
                e.printStackTrace();
            } finally {
                if (nodeSocket != null) {
                    nodeSocket.close();
                }
            }
        }
    }
}
