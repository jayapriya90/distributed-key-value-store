package edu.umn.cs.distributedkeyvaluestore.coordinator;

import edu.umn.cs.distributedkeyvaluestore.*;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * Created by jayapriya on 3/25/16.
 */

public class CoordinatorEndPointsImpl implements CoordinatorEndPoints.Iface {

    private static final Logger LOG = LoggerFactory.getLogger(CoordinatorEndPointsImpl.class);
    private int n;
    private int nr;
    private int nw;
    private Set<FileServerInfo> servers;
    private boolean userSpecificedQuorumCount;

    public CoordinatorEndPointsImpl(int nr, int nw) {
        this.n = 0;
        this.nr = nr;
        this.nw = nw;
        this.servers = new HashSet<FileServerInfo>();
        if (nr >= 0 && nw >=0) {
            this.userSpecificedQuorumCount = true;
        } else {
            this.userSpecificedQuorumCount = false;
        }
    }

    @Override
    public FileServerResponse getFileServer() throws TException {
        List<FileServerInfo> nodes = new ArrayList<FileServerInfo>(servers);
        // thrift cannot send null as response. So sending empty hostname.
        if (nodes.isEmpty()) {
            return new FileServerResponse(Status.NO_NODE_FOUND);
        }
        Random rand = new Random();
        int index = rand.nextInt() % nodes.size();
        FileServerResponse result = new FileServerResponse(Status.SUCCESS);
        result.setFileServerInfo(nodes.get(index));
        return result;
    }

    @Override
    public List<FileServerMetaData> getFileServersMetadata() throws TException {
        List<FileServerMetaData> result = new ArrayList<FileServerMetaData>();
        for (FileServerInfo server : servers) {
            TTransport nodeSocket = new TSocket(server.getHostname(), server.getPort());
            nodeSocket.open();
            TProtocol protocol = new TBinaryProtocol(nodeSocket);
            FileServerEndPoints.Client client = new FileServerEndPoints.Client(protocol);
            FileServerMetaData fileServerMetaData = client.getFileServerMetadata();
            result.add(fileServerMetaData);
            nodeSocket.close();
        }
        return result;
    }

    @Override
    public void join(String hostname, int Port) throws TException {
        FileServerInfo hostInfo = new FileServerInfo(hostname, Port);
        LOG.info("Received join request from " + hostInfo);
        servers.add(hostInfo);
        updateQuorum();
    }

    private void updateQuorum() {
        n = servers.size();
        if (!userSpecificedQuorumCount) {
            nw = (n / 2) + 1;
            nr = n - nw + 1;
            LOG.info("Updating read and write quorum count..");
        } else {
            LOG.info("User has specified read and write quorum count. Not updating quorum.");
        }
        LOG.info("n: " + n + " nw: " + nw + " nr: " + nr);
    }

    @Override
    public FileServerMetaData getFileServerMetadata() throws TException {
        return null;
    }

    @Override
    public Response submitRequest(Request request) throws TException {
        Response response = new Response(request.getType());
        if (request.getType().equals(Type.READ)) {
            ReadResponse readResponse = readInternal(request.getFilename());
            response.setReadResponse(readResponse);
        } else {
            WriteResponse writeResponse = writeInternal(request.getFilename(), request.getContents());
            response.setWriteResponse(writeResponse);
        }
        return response;
    }

    private ReadResponse readInternal(String filename) throws TException {
        List<FileServerInfo> readQuorum = getRandomNodes(nr);
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

        LOG.debug(maxVersionServer + " contains latest version: " + maxVersion + " for file: " + filename);
        return getReadResponse(maxVersionServer, filename);
    }

    private ReadResponse getReadResponse(FileServerInfo server, String filename) throws TException {
        TTransport nodeSocket = new TSocket(server.getHostname(), server.getPort());
        nodeSocket.open();
        TProtocol protocol = new TBinaryProtocol(nodeSocket);
        FileServerEndPoints.Client client = new FileServerEndPoints.Client(protocol);
        ReadResponse response = client.readContents(filename);
        LOG.debug(server + " returned read response: " + response + " for file: " + filename);
        nodeSocket.close();
        return response;
    }

    private long getVersionFromFileServer(FileServerInfo server, String filename) throws TException {
        TTransport nodeSocket = new TSocket(server.getHostname(), server.getPort());
        nodeSocket.open();
        TProtocol protocol = new TBinaryProtocol(nodeSocket);
        FileServerEndPoints.Client client = new FileServerEndPoints.Client(protocol);
        long version = client.getVersion(filename);
        LOG.debug(server + " returned version: " + version + " for file: " + filename);
        nodeSocket.close();
        return version;
    }

    private WriteResponse writeInternal(String filename, String contents) throws TException {
        List<FileServerInfo> writeQuorum = getRandomNodes(nw);
        return null;
    }

    private List<FileServerInfo> getRandomNodes(int n) {
        List<FileServerInfo> nodes = new ArrayList<FileServerInfo>(servers);
        Collections.shuffle(nodes);
        return nodes.subList(0, n);
    }
}
