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

/**
 * Created by jayapriya on 3/27/16.
 */
public class ReadThread implements Callable<Response> {
    private static final Logger LOG = LoggerFactory.getLogger(ReadThread.class);
    private int nr;
    private List<FileServerInfo> servers;
    private String filename;

    public ReadThread(int nr, List<FileServerInfo> servers, Request request) {
        this.nr = nr;
        this.servers = servers;
        this.filename = request.getFilename();
    }

    @Override
    public Response call() throws Exception {
        LOG.info("Read request received for file: " + filename);
        List<FileServerInfo> readQuorum = getQuorumServers(nr);
        long maxVersion = Long.MIN_VALUE;
        FileServerInfo maxVersionServer = null;
        // Connect to each file server and get their version number
        // Find max version number containing file server
        // Forward read request to that node
        // send back the received read response
        for (FileServerInfo server : readQuorum) {
            long version = getVersionFromFileServer(server, filename);
            if (version > maxVersion) {
                maxVersion = version;
                maxVersionServer = server;
            }
        }

        LOG.info(maxVersionServer + " contains latest read version: " + maxVersion + " for file: " + filename);
        ReadResponse rr = getReadResponse(maxVersionServer, filename);
        Response response = new Response(Type.READ);
        response.setReadResponse(rr);
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

    private ReadResponse getReadResponse(FileServerInfo server, String filename) throws TException {
        TTransport nodeSocket = new TSocket(server.getHostname(), server.getPort());
        nodeSocket.open();
        TProtocol protocol = new TBinaryProtocol(nodeSocket);
        FileServerEndPoints.Client client = new FileServerEndPoints.Client(protocol);
        ReadResponse response = client.readContents(filename);
        LOG.info(server + " returned read response: " + response + " for file: " + filename);
        nodeSocket.close();
        return response;
    }
}
