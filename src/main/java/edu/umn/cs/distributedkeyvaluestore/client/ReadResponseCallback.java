package edu.umn.cs.distributedkeyvaluestore.client;

import edu.umn.cs.distributedkeyvaluestore.ReadResponse;
import org.apache.thrift.async.AsyncMethodCallback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by jayapriya on 3/27/16.
 */
public class ReadResponseCallback implements AsyncMethodCallback<ReadResponse> {
    private static final Logger LOG = LoggerFactory.getLogger(ReadResponseCallback.class);
    private long start;
    private String filename;
    public ReadResponseCallback(String filename, long start) {
        this.filename = filename;
        this.start = start;
    }

    @Override
    public void onComplete(ReadResponse readResponse) {
        long end = System.currentTimeMillis();
        long totalExecTime = end - start;
        LOG.info("Successfully received read response: " + readResponse + " for file: " +
                filename + " in " + totalExecTime + " ms.");
    }

    @Override
    public void onError(Exception e) {
        LOG.error("Exception thrown for read request file: " + filename + " exception: " + e);
    }
}
