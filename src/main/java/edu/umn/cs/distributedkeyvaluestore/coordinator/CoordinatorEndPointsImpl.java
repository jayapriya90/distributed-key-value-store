package edu.umn.cs.distributedkeyvaluestore.coordinator;

import edu.umn.cs.distributedkeyvaluestore.*;
import edu.umn.cs.distributedkeyvaluestore.common.Constants;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.*;

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

    // single consumer thread pool
    private ExecutorService consumerThreadPool;

    private BlockingQueue<Request> requestQueue;

    public CoordinatorEndPointsImpl(int nr, int nw) {
        this.n = 0;
        this.nr = nr;
        this.nw = nw;
        this.servers = new HashSet<FileServerInfo>();
        if (nr >= 0 && nw >= 0) {
            this.userSpecificedQuorumCount = true;
        } else {
            this.userSpecificedQuorumCount = false;
        }
        // Unbounded queue. Blocking queue will automatically wait when queue is empty.
        this.requestQueue = new LinkedBlockingQueue<Request>();
        this.consumerThreadPool = null;
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
            LOG.info("User has specified read and write quorum count. Not updating quorum.");
        }
        LOG.info("n: " + n + " nw: " + nw + " nr: " + nr);
    }

    @Override
    public void submitRequest(Request request) throws TException {
        if (consumerThreadPool == null) {
            consumerThreadPool = Executors.newSingleThreadExecutor();
            List<FileServerInfo> serverList = new ArrayList<FileServerInfo>(servers);
            // Pass the shared queue to consumer and run the consumer
            consumerThreadPool.execute(new ConsumerThread(requestQueue, nr, nw, serverList));
        }
        requestQueue.add(request);

//        Response response = new Response(request.getType());
//        if (request.getType().equals(Type.READ)) {
//            ReadResponse readResponse = readInternal(request.getFilename());
//            response.setReadResponse(readResponse);
//        } else {
//            WriteResponse writeResponse = writeInternal(request.getFilename(), request.getContents());
//            response.setWriteResponse(writeResponse);
//        }
//        return response;
    }
}
