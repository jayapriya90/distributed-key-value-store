package edu.umn.cs.distributedkeyvaluestore.coordinator;

import edu.umn.cs.distributedkeyvaluestore.*;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Created by jayapriya on 3/27/16.
 */
public class WriteThread implements Callable<Response> {
    private static final Logger LOG = LoggerFactory.getLogger(WriteThread.class);
    private int nw;
    private List<FileServerInfo> servers;
    private String filename;
    private String contents;
    // background thread to notify non-quorum members
    private ExecutorService backgroundThread;

    public WriteThread(int nw, List<FileServerInfo> servers, Request request) {
        this.nw = nw;
        this.servers = servers;
        this.filename = request.getFilename();
        this.contents = request.getContents();
        this.backgroundThread = Executors.newSingleThreadExecutor();
    }

    @Override
    public Response call() throws Exception {
        LOG.info("Write request received for file: " + filename + " contents: " + contents);
        List<FileServerInfo> writeQuorum = getQuorumServers(nw);
        long maxVersion = Long.MIN_VALUE;
        FileServerInfo maxVersionServer = null;
        // Connect to each file server and get their version number
        // Find max version number containing file server
        // Forward the write request to that file server with incremented version
        // send back the received write response
        for (FileServerInfo server : writeQuorum) {
            long version = getVersionFromFileServer(server, filename);
            if (version > maxVersion) {
                maxVersion = version;
                maxVersionServer = server;
            }
        }

        LOG.info(maxVersionServer + " contains latest write version: " + maxVersion + " for file: " + filename);

        // increment version
        maxVersion++;
        LOG.info("Incremented version number for file: " + filename + " to version: " + maxVersion);

        WriteResponse writeResponse = getWriteResponse(maxVersionServer, filename, contents, maxVersion);
        LOG.info("Updated quorum file server (with max version): " + maxVersionServer + " to use new version: " + maxVersion);

        // update other file servers in quorum to latest version and wait for it to complete
        LOG.info("Updating other file servers in quorum to latest version: " + maxVersion);
        writeQuorum.remove(maxVersionServer);
        for (FileServerInfo quorumServer : writeQuorum) {
            WriteResponse wr = getWriteResponse(quorumServer, filename, contents, maxVersion);
            LOG.info("Updated quorum server: " + quorumServer + " to version: " + wr.getVersion());
        }

        // update other file servers not in quorum to latest version in background and don't wait for completion
        LOG.info("Notifying file servers not in quorum in background thread to latest version: " + maxVersion);
        List<FileServerInfo> nonQuorumServers = getNonQuorumServers(writeQuorum);
        // submit is asynchronous whereas execute is synchronous
        backgroundThread.submit(new NonQuorumWriteThread(nonQuorumServers, filename, contents, maxVersion));

        // return write response
        LOG.info("Successfully updated file: " + filename + " to version: " + writeResponse.getVersion());
        LOG.info("Returning write response: " + writeResponse + " to client");
        Response response = new Response(Type.WRITE);
        response.setWriteResponse(writeResponse);
        return response;
    }

    private List<FileServerInfo> getQuorumServers(int n) {
        List<FileServerInfo> nodes = new ArrayList<FileServerInfo>(servers);
        Collections.shuffle(nodes);
        return nodes.subList(0, n);
    }

    private long getVersionFromFileServer(FileServerInfo server, String filename) throws TException {
        TTransport nodeSocket = new TSocket(server.getHostname(), server.getPort());
        nodeSocket.open();
        TProtocol protocol = new TBinaryProtocol(nodeSocket);
        FileServerEndPoints.Client client = new FileServerEndPoints.Client(protocol);
        long version = client.getVersion(filename);
        LOG.info(server + " returned version: " + version + " for file: " + filename);
        nodeSocket.close();
        return version;
    }

    private WriteResponse getWriteResponse(FileServerInfo server, String filename, String contents, long newVersion) throws TException {
        TTransport nodeSocket = new TSocket(server.getHostname(), server.getPort());
        nodeSocket.open();
        TProtocol protocol = new TBinaryProtocol(nodeSocket);
        FileServerEndPoints.Client client = new FileServerEndPoints.Client(protocol);
        WriteResponse writeResponse = client.writeContents(filename, contents, newVersion);
        return writeResponse;
    }

    private List<FileServerInfo> getNonQuorumServers(List<FileServerInfo> quorum) {
        List<FileServerInfo> nodes = new ArrayList<FileServerInfo>(servers);
        for (FileServerInfo serverInfo : quorum) {
            nodes.remove(serverInfo);
        }
        return nodes;
    }
}
