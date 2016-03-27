package edu.umn.cs.distributedkeyvaluestore.fileserver;

import edu.umn.cs.distributedkeyvaluestore.*;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by jayapriya on 3/25/16.
 */
public class FileServerEndPointsImpl implements FileServerEndPoints.Iface {
    private static final Logger LOG = LoggerFactory.getLogger(FileServerEndPointsImpl.class);
    private FileServerInfo coordinator;
    private Map<String, String> contentsMap;
    private Map<String, Long> versionMap;

    public FileServerEndPointsImpl(FileServerInfo coordinator) {
        this.coordinator = coordinator;
        this.contentsMap = new HashMap<String, String>();
        this.versionMap = new HashMap<String, Long>();
    }

    @Override
    public ReadResponse read(String filename) throws TException {
        return submitReadRequestToCoordinator(filename);
    }

    private ReadResponse submitReadRequestToCoordinator(String filename) throws TException {
        TTransport nodeSocket = new TSocket(coordinator.getHostname(), coordinator.getDfsPort());
        nodeSocket.open();
        TProtocol protocol = new TBinaryProtocol(nodeSocket);
        CoordinatorEndPoints.Client client = new CoordinatorEndPoints.Client(protocol);
        Request readRequest = new Request(Type.READ, filename);
        LOG.info("Submitting read request for file: " + filename + " to coordinator: " + coordinator);
        Response response = client.submitRequest(readRequest);
        nodeSocket.close();
        ReadResponse readResponse = response.getReadResponse();
        LOG.info("Returning read response: " + readResponse + " to client");
        return readResponse;
    }

    @Override
    public WriteResponse write(String filename, String contents) throws TException {
        return submitWriteRequestToCoordinator(filename, contents);
    }

    private WriteResponse submitWriteRequestToCoordinator(String filename, String contents) throws TException {
        TTransport nodeSocket = new TSocket(coordinator.getHostname(), coordinator.getDfsPort());
        nodeSocket.open();
        TProtocol protocol = new TBinaryProtocol(nodeSocket);
        CoordinatorEndPoints.Client client = new CoordinatorEndPoints.Client(protocol);
        Request writeRequest = new Request(Type.WRITE, filename);
        writeRequest.setContents(contents);
        LOG.info("Submitting write request for file: " + filename + " to coordinator: " + coordinator);
        Response response = client.submitRequest(writeRequest);
        nodeSocket.close();
        WriteResponse writeResponse = response.getWriteResponse();
        LOG.info("Returning write response: " + writeResponse + " to client");
        return writeResponse;
    }

    @Override
    public FileServerMetaData getFileServerMetadata() throws TException {
        List<FileInfo> fileInfos = new ArrayList<FileInfo>();
        for(String filename: contentsMap.keySet()) {
            long version = versionMap.get(filename);
            String contents = contentsMap.get(filename);
            fileInfos.add(new FileInfo(filename, version, contents));
        }
        FileServerMetaData fileServerMetaData = new FileServerMetaData(fileInfos);
        LOG.debug("Returning file server metadata: " + fileServerMetaData);
        return fileServerMetaData;
    }

    @Override
    public ReadResponse readContents(String filename) throws TException {
        if (!contentsMap.containsKey(filename)) {
            LOG.info(filename + " not found!");
            return new ReadResponse(Status.FILE_NOT_FOUND);
        }

        ReadResponse readResponse = new ReadResponse(Status.SUCCESS);
        readResponse.setContents(contentsMap.get(filename));
        LOG.info("Sending read response: " + readResponse + " for file: " + filename);
        return readResponse;
    }

    @Override
    public WriteResponse writeContents(String filename, String contents, long version) throws TException {
        contentsMap.put(filename, contents);
        versionMap.put(filename, version);
        LOG.info("Wrote contents: " + contents + " to file: " + filename + " with version: " + version);
        WriteResponse writeResponse = new WriteResponse(Status.SUCCESS);
        writeResponse.setBytesWritten(contents.length());
        writeResponse.setVersion(version);
        return writeResponse;
    }

    @Override
    public long getVersion(String filename) throws TException {
        if (!versionMap.containsKey(filename)) {
            LOG.info(filename + " not found! Return version: -1");
            return -1;
        }

        long version = versionMap.get(filename);
        LOG.info("Returning version: " + version + " for file: " + filename);
        return version;
    }
}
