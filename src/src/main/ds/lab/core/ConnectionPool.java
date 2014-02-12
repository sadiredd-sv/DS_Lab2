package ds.lab.core;

import java.io.IOException;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map.Entry;

import javax.net.SocketFactory;

import ds.lab.util.ConfigParser.Node;

/**
 * Connection pool
 */
class ConnectionPool {

    private HashMap<Node, Socket> mConnectionMap = null;

    public ConnectionPool() {
        mConnectionMap = new HashMap<Node, Socket>();
    }

    public synchronized Socket openSocket(Node node) {
        // if (mConnectionMap != null && mConnectionMap.containsKey(node)) {
        // return mConnectionMap.get(node);
        // } else {
        // Socket socket = createSocketByNode(node);
        // if (socket != null) {
        // mConnectionMap.put(node, socket);
        // return socket;
        // } else {
        // return null;
        // }
        // }
        return createSocketByNode(node);
    }

    public synchronized void closeSocket(Node node) {
        if (mConnectionMap != null && mConnectionMap.containsKey(node)) {
            Socket socket = mConnectionMap.get(node);
            if (socket != null) {
                try {
                    socket.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            mConnectionMap.remove(node);
        }
    }

    public void release() {
        Iterator<Entry<Node, Socket>> iter = mConnectionMap.entrySet()
                .iterator();
        while (iter.hasNext()) {
            Entry<Node, Socket> entry = iter.next();
            Socket socket = entry.getValue();
            if (socket != null) {
                try {
                    socket.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        mConnectionMap.clear();
    }

    private Socket createSocketByNode(Node node) {
        Socket socket = null;
        try {
            socket = SocketFactory.getDefault().createSocket(node.getIp(),
                    Integer.valueOf(node.getPort()));
        } catch (NumberFormatException e) {
            e.printStackTrace();
        } catch (UnknownHostException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return socket;
    }
}
