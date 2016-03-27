package edu.umn.cs.distributedkeyvaluestore.common;

import java.io.IOException;
import java.net.ServerSocket;

/**
 * Created by jayapriya on 3/27/16.
 */
public class Utilities {
    public static int getRandomPort() throws IOException {
        ServerSocket socket = new ServerSocket(0);
        int randomPort = socket.getLocalPort();
        socket.close();
        return randomPort;
    }
}
