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

import java.util.List;
import java.util.concurrent.*;

/**
 * Created by jayapriya on 3/27/16.
 */
public class ConsumerThread implements Runnable {
    private static final Logger LOG = LoggerFactory.getLogger(ConsumerThread.class);

    private BlockingQueue<Request> requestQueue;
    // concurrent reader thread pool (default 10 threads)
    private ExecutorService readerThreadPool;

    // single writer thread pool
    private ExecutorService writerThreadPool;

    private int nr;
    private int nw;
    private List<FileServerInfo> servers;

    public ConsumerThread(BlockingQueue<Request> requestQueue, int nr, int nw, List<FileServerInfo> servers) {
        this.requestQueue = requestQueue;
        this.readerThreadPool = Executors.newFixedThreadPool(Constants.MAX_CONCURRENT_READERS);
        this.writerThreadPool = Executors.newSingleThreadExecutor();
        this.nr = nr;
        this.nw = nw;
        this.servers = servers;
    }

    @Override
    public void run() {
        Request request = null;
        try {
            while ((request = requestQueue.take()) != null) {
                if (request.getType().equals(Type.READ)) {

                } else {
                    Future<Response> future = writerThreadPool.submit(new WriteThread(nw, servers, request));
                    Response response = future.get(); // synchronous wait
                    sendResponse(request, response);
                }
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }
    }

    private void sendResponse(Request request, Response response) throws TException {
        TTransport nodeSocket = new TSocket(request.getFileServerInfo().getHostname(), request.getFileServerInfo().getPort());
        nodeSocket.open();
        TProtocol protocol = new TBinaryProtocol(nodeSocket);
        FileServerEndPoints.Client client = new FileServerEndPoints.Client(protocol);
        client.submitResponse(request, response);
        LOG.info("Sending response: " + response + " for request: " + request);
        nodeSocket.close();
    }
}
