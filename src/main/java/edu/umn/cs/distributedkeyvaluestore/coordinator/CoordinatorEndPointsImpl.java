package edu.umn.cs.distributedkeyvaluestore.coordinator;

import edu.umn.cs.distributedkeyvaluestore.*;
import edu.umn.cs.distributedkeyvaluestore.common.Constants;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

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
    private boolean quorumConditionMet = true;

    // background thread to notify non-quorum members
    private ExecutorService backgroundThread;

    // timer that will schedule periodic sync task
    private Timer syncTimer;
    // sync timer task will be scheduled when coordinator receives first request.
    // ASSUMPTION: When sync thread is started coordinator needs to know all nodes in the system, that is the reason
    // we are starting this task after first read/write request is received. Also, after starting this task we assume
    // that no new nodes will be added.
    private static boolean firstRequest = true;
    private long syncInterval;

    public CoordinatorEndPointsImpl(int nr, int nw, long syncInterval) {
        this.n = 0;
        this.nr = nr;
        this.nw = nw;
        this.servers = new HashSet<FileServerInfo>();
        if (nr >= 0 && nw >= 0) {
            this.userSpecificedQuorumCount = true;
        } else {
            this.userSpecificedQuorumCount = false;
        }
        this.backgroundThread = Executors.newSingleThreadExecutor();
        this.syncTimer = new Timer("SYNC", true);
        this.syncInterval = syncInterval;
    }

    @Override
    public FileServerResponse getFileServer() throws TException {
        List<FileServerInfo> nodes = new ArrayList<FileServerInfo>(servers);
        // thrift cannot send null as response. So sending empty hostname.
        if (nodes.isEmpty()) {
            return new FileServerResponse(Status.NO_NODE_FOUND);
        }
        Random rand = new Random();
        int randInt = rand.nextInt();
        // should always be positive
        if (randInt < 0) {
            randInt = -1 * randInt;
        }
        int index = randInt % nodes.size();
        FileServerResponse result = new FileServerResponse(Status.SUCCESS);
        result.setFileServerInfo(nodes.get(index));
        return result;
    }

    @Override
    public Map<FileServerInfo, FileServerMetaData> getMetadata() throws TException {
        Map<FileServerInfo, FileServerMetaData> result = new HashMap<FileServerInfo, FileServerMetaData>();
        // iterate the file servers list and get metadata from each server
        for (FileServerInfo server : servers) {
            TTransport nodeSocket = new TSocket(server.getHostname(), server.getPort());
            nodeSocket.open();
            TProtocol protocol = new TBinaryProtocol(nodeSocket);
            FileServerEndPoints.Client client = new FileServerEndPoints.Client(protocol);
            FileServerMetaData fileServerMetaData = client.getFileServerMetadata();
            result.put(server, fileServerMetaData);
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
            if (!(nw > n/2)) {
                quorumConditionMet = false;
            }

            if (!(nw + nr > n)) {
                quorumConditionMet = false;
            }
            LOG.info("User has specified read and write quorum count. Not updating quorum." +
                    " quorumConditionMet: " + quorumConditionMet);
        }
        LOG.info("n: " + n + " nw: " + nw + " nr: " + nr);
    }

    @Override
    public Response submitRequest(Request request) throws TException {
        if (firstRequest) {
            LOG.info("Scheduling sync task..");
            // start the sync task after 3 seconds and run it every 5 seconds or user specified interval
            syncTimer.scheduleAtFixedRate(new SyncTimerTask(servers), Constants.SYNC_TASK_DELAY,
                    syncInterval);
            firstRequest = false;
        }
        Response response = new Response(request.getType());

        // server read and write request
        if (request.getType().equals(Type.READ)) {

            // if quorum condition is not met, the return error
            if (!quorumConditionMet) {
                response.setReadResponse(new ReadResponse(Status.SERVER_CANNOT_BE_CONTACTED));
            } else {
                ReadResponse readResponse = readInternal(request.getFilename());
                response.setReadResponse(readResponse);
            }

        } else {

            // if quorum condition is not met, the return error
            if (!quorumConditionMet) {
                response.setWriteResponse(new WriteResponse(Status.SERVER_CANNOT_BE_CONTACTED));
            } else {
                WriteResponse writeResponse = writeInternal(request.getFilename(), request.getContents());
                response.setWriteResponse(writeResponse);
            }

        }
        return response;
    }

    private ReadResponse readInternal(String filename) throws TException {
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
        return getReadResponse(maxVersionServer, filename);
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

    private WriteResponse writeInternal(String filename, String contents) throws TException {
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

        // first write to the file server with max version number. The response will contain the incremented version
        // number which will be used to update other servers
        WriteResponse writeResponse = getWriteResponse(maxVersionServer, filename, contents);
        long latestVersion = writeResponse.getVersion();
        LOG.info("Successfully wrote to quorum file server (with max version): " + maxVersionServer + "." +
                " Got new version: " + latestVersion + " for file: " + filename);

        // update other file servers in quorum to latest version and wait for it to complete
        LOG.info("Updating other file servers in quorum to latest version: " + latestVersion);
        writeQuorum.remove(maxVersionServer);
        for (FileServerInfo quorumServer : writeQuorum) {
            WriteResponse wr = getUpdateResponse(quorumServer, filename, contents, latestVersion);
            LOG.info("Updated quorum server: " + quorumServer + " to version: " + wr.getVersion());
        }

        // update other file servers not in quorum to latest version in background and don't wait for completion
        LOG.info("Notifying file servers not in quorum in background thread to latest version: " + latestVersion);
        List<FileServerInfo> nonQuorumServers = getNonQuorumServers(writeQuorum);
        backgroundThread.submit(new NonQuorumWrite(nonQuorumServers, filename, contents, maxVersion));

        // return write response
        LOG.info("Successfully updated file: " + filename + " to version: " + writeResponse.getVersion());
        LOG.info("Returning write response: " + writeResponse + " to client");
        return writeResponse;
    }

    private WriteResponse getUpdateResponse(FileServerInfo server, String filename, String contents,
                                            long latestVersion) throws TException {
        TTransport nodeSocket = new TSocket(server.getHostname(), server.getPort());
        nodeSocket.open();
        TProtocol protocol = new TBinaryProtocol(nodeSocket);
        FileServerEndPoints.Client client = new FileServerEndPoints.Client(protocol);
        WriteResponse writeResponse = client.updateContentsToVersion(filename, contents, latestVersion);
        nodeSocket.close();
        return writeResponse;
    }

    private WriteResponse getWriteResponse(FileServerInfo server, String filename, String contents) throws TException {
        TTransport nodeSocket = new TSocket(server.getHostname(), server.getPort());
        nodeSocket.open();
        TProtocol protocol = new TBinaryProtocol(nodeSocket);
        FileServerEndPoints.Client client = new FileServerEndPoints.Client(protocol);
        WriteResponse writeResponse = client.writeContents(filename, contents);
        nodeSocket.close();
        return writeResponse;
    }

    private List<FileServerInfo> getQuorumServers(int n) {
        List<FileServerInfo> nodes = new ArrayList<FileServerInfo>(servers);
        Collections.shuffle(nodes);
        return nodes.subList(0, n);
    }

    private List<FileServerInfo> getNonQuorumServers(List<FileServerInfo> quorum) {
        List<FileServerInfo> nodes = new ArrayList<FileServerInfo>(servers);
        for (FileServerInfo serverInfo : quorum) {
            nodes.remove(serverInfo);
        }
        return nodes;
    }
}
