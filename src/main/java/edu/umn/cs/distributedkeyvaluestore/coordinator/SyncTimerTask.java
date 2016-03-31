package edu.umn.cs.distributedkeyvaluestore.coordinator;

/**
 * Created by jayapriya on 3/25/16.
 */

import edu.umn.cs.distributedkeyvaluestore.*;
import edu.umn.cs.distributedkeyvaluestore.fileserver.FileServer;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * Sync thread which gets scheduled after fixed time. This is also a background thread that periodically updates
 * the contents of file servers to its latest version
 */

public class SyncTimerTask extends TimerTask {
    private static final Logger LOG = LoggerFactory.getLogger(SyncTimerTask.class);

    private Set<FileServerInfo> servers;

    public SyncTimerTask(Set<FileServerInfo> servers) {
        this.servers = servers;
    }

    @Override
    public void run() {
        // Iterate through all servers and for each file, get its max/latest version.
        // Send updateContents request to all nodes to update its contents.

        try {
            // we need to know the list of files before initiating the sync operation.
            // We can get the list of files from any node.
            List<String> filenames = getAllFilenames(servers.iterator().next());
            boolean anyOutOfSync = false;
            for (String filename : filenames) {
                long latestVersion = Long.MIN_VALUE;
                FileServerInfo maxVersionServer = null;
                // find max version for file
                for (FileServerInfo fileServerInfo : servers) {
                    long version = getVersionFromFileServer(fileServerInfo, filename);
                    if (version > latestVersion) {
                        latestVersion = version;
                        maxVersionServer = fileServerInfo;
                    }
                }

                // get contents of latest version
                ReadResponse readResponse = getReadResponse(maxVersionServer, filename);
                String latestContents = readResponse.getContents();

                List<FileServerInfo> outOfSyncServers = new ArrayList<FileServerInfo>();
                for (FileServerInfo fileServerInfo : servers) {
                    long version = getVersionFromFileServer(fileServerInfo, filename);
                    // out of sync if version number is lesser than latest version
                    if (version < latestVersion) {
                        outOfSyncServers.add(fileServerInfo);
                    }
                }

                // send message to out-of-sync servers to update contents
                for (FileServerInfo fileServerInfo : outOfSyncServers) {
                    anyOutOfSync = true;
                    sendUpdateContents(fileServerInfo, filename, latestContents, latestVersion);
                }
            }

            if (anyOutOfSync) {
                LOG.info("Successfully synced all files to its latest version!");
            } else {
                LOG.info("All file servers are up-to-date! Nothing to sync.");
            }
        } catch (TException e) {
            e.printStackTrace();
        }
    }

    private List<String> getAllFilenames(FileServerInfo server) throws TException {
        TTransport nodeSocket = new TSocket(server.getHostname(), server.getPort());
        nodeSocket.open();
        TProtocol protocol = new TBinaryProtocol(nodeSocket);
        FileServerEndPoints.Client client = new FileServerEndPoints.Client(protocol);
        FileServerMetaData metaData = client.getFileServerMetadata();
        List<String> filenames = new ArrayList<String>();
        for (FileInfo fileInfo : metaData.getFileinfos()) {
            filenames.add(fileInfo.getFilename());
        }
        nodeSocket.close();
        return filenames;
    }

    private long getVersionFromFileServer(FileServerInfo server, String filename) throws TException {
        TTransport nodeSocket = new TSocket(server.getHostname(), server.getPort());
        nodeSocket.open();
        TProtocol protocol = new TBinaryProtocol(nodeSocket);
        FileServerEndPoints.Client client = new FileServerEndPoints.Client(protocol);
        long version = client.getVersion(filename);
        nodeSocket.close();
        return version;
    }

    private ReadResponse getReadResponse(FileServerInfo server, String filename) throws TException {
        TTransport nodeSocket = new TSocket(server.getHostname(), server.getPort());
        nodeSocket.open();
        TProtocol protocol = new TBinaryProtocol(nodeSocket);
        FileServerEndPoints.Client client = new FileServerEndPoints.Client(protocol);
        ReadResponse response = client.readContents(filename);
        nodeSocket.close();
        return response;
    }

    private void sendUpdateContents(FileServerInfo server, String filename, String contents,
                                    long latestVersion) throws TException {
        TTransport nodeSocket = new TSocket(server.getHostname(), server.getPort());
        nodeSocket.open();
        TProtocol protocol = new TBinaryProtocol(nodeSocket);
        FileServerEndPoints.Client client = new FileServerEndPoints.Client(protocol);
        client.updateContentsToVersion(filename, contents, latestVersion);
        nodeSocket.close();
    }

}
