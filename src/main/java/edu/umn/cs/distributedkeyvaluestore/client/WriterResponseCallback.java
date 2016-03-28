package edu.umn.cs.distributedkeyvaluestore.client;

import edu.umn.cs.distributedkeyvaluestore.WriteResponse;
import org.apache.thrift.async.AsyncMethodCallback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by jayapriya on 3/27/16.
 */
public class WriterResponseCallback implements AsyncMethodCallback<WriteResponse> {
    private static final Logger LOG = LoggerFactory.getLogger(WriterResponseCallback.class);
    private long start;
    private String filename;
    private String contents;

    public WriterResponseCallback(String filename, String contents, long startTime) {
        this.start = startTime;
        this.filename = filename;
        this.contents = contents;
    }

    @Override
    public void onComplete(WriteResponse writeResponse) {
        long end = System.currentTimeMillis();
        long totalExecTime = end - start;
        LOG.info("Successfully received write response: " + writeResponse + " for file: " +
                filename + " in " + totalExecTime + " ms.");
    }

    @Override
    public void onError(Exception e) {
        LOG.error("Exception thrown for write request file: " + filename + " contents: " + contents + " exception: " + e);
    }
}
