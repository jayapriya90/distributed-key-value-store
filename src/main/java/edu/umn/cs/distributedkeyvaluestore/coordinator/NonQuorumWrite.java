package edu.umn.cs.distributedkeyvaluestore.coordinator;

/**
 * Created by jayapriya on 3/25/16.
 */

import edu.umn.cs.distributedkeyvaluestore.FileServerEndPoints;
import edu.umn.cs.distributedkeyvaluestore.FileServerInfo;
import edu.umn.cs.distributedkeyvaluestore.WriteResponse;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * Write that that updates non-quorum file servers with latest version
 */

public class NonQuorumWrite implements Runnable {
    private static final Logger LOG = LoggerFactory.getLogger(NonQuorumWrite.class);
    private List<FileServerInfo> nonQuorum;
    private String filename;
    private String contents;
    private long version;

    public NonQuorumWrite(List<FileServerInfo> nonQuorum, String filename, String contents, long version) {
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
                WriteResponse wr = client.updateContentsToVersion(filename, contents, version);
                LOG.info("Background thread - Successfully updated " + serverInfo + " about file: " + filename +
                        " contents: " + contents + " version: " + wr.getVersion());
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
