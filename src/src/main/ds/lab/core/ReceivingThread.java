package ds.lab.core;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.Iterator;
import java.util.concurrent.BlockingQueue;

import ds.lab.core.clock.ClockService;
import ds.lab.entity.Message;
import ds.lab.entity.TimeStamp;
import ds.lab.entity.TimeStampedMessage;

/**
 * Receiving thread
 */
class ReceivingThread extends Thread {

    private boolean mIsRunning = false;

    private BlockingQueue<Message> mReceiveQueue = null;

    // private BlockingQueue<Message> mDelayedReceivingQueue = null;

    private int mPort = -1;

    private ClockService mClockService = null;

    /**
     * 
     * @param receiveQueue
     * @param configParser
     */
    public ReceivingThread(BlockingQueue<Message> receiveQueue,
            BlockingQueue<Message> delayedReceivingQueue, int port,
            ClockService clockService) {
        mReceiveQueue = receiveQueue;
        // mDelayedReceivingQueue = delayedReceivingQueue;
        mPort = port;
        mClockService = clockService;
    }

    public void stopServer() {
        mIsRunning = false;
    }

    @Override
    public void run() {
        mIsRunning = true;
        startServer(mPort);
    }

    /**
     * Start NIO server with specific port
     * 
     * @param port specific port when server is running
     */
    private void startServer(int port) {
        Selector selector = null;
        ServerSocketChannel serverSocketChannel = null;
        try {
            // Open selector
            selector = Selector.open();

            // Create a new server socket and set to non blocking mode
            serverSocketChannel = ServerSocketChannel.open();
            serverSocketChannel.configureBlocking(false);

            // Bind the server socket to the local host and port
            serverSocketChannel.socket().setReuseAddress(true);
            serverSocketChannel.socket().bind(new InetSocketAddress(port));

            // Register accepts on the server socket with the selector.
            serverSocketChannel.register(selector, SelectionKey.OP_ACCEPT);

            while (mIsRunning) {
                selector.select();
                Iterator<SelectionKey> it = selector.selectedKeys().iterator();

                while (it.hasNext()) {
                    SelectionKey readyKey = it.next();
                    it.remove();

                    readData(selector, readyKey);
                }
            }
        } catch (ClosedChannelException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                if (selector != null) {
                    selector.close();
                }
                if (serverSocketChannel != null) {
                    serverSocketChannel.close();
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    private void readData(Selector selector, SelectionKey readyKey) {
        if (readyKey != null) {
            if (readyKey.isAcceptable()) {
                ServerSocketChannel server = (ServerSocketChannel) readyKey
                        .channel();
                SocketChannel channel = null;
                try {
                    channel = server.accept();
                    channel.configureBlocking(false);
                    channel.register(selector, SelectionKey.OP_READ);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            } else if (readyKey.isReadable()) {
                SocketChannel channel = (SocketChannel) readyKey.channel();
                Message message = readData(channel);
                if (message != null) {
                    syncClock(message);
                    timeStampMessage(message);
                    mReceiveQueue.add(message);
                }
            }
        }
    }

    private Message readData(SocketChannel channel) {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ByteBuffer buffer = ByteBuffer.allocate(1024);
        Message message = null;
        try {
            byte[] bytes = null;
            int size = 0;
            while ((size = channel.read(buffer)) > 0) {
                buffer.flip();
                bytes = new byte[size];
                buffer.get(bytes);
                baos.write(bytes);
                buffer.clear();
            }
            bytes = baos.toByteArray();
            if (bytes.length > 0) {
                message = deserialize(bytes);
            } else {
                channel.close();
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                if (baos != null) {
                    baos.close();
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        return message;
    }

    private Message deserialize(byte[] bytes) {
        ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
        ObjectInputStream ois = null;
        Message message = null;
        try {
            ois = new ObjectInputStream(bais);
            message = (Message) ois.readObject();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } finally {
            try {
                if (ois != null) {
                    ois.close();
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        return message;
    }

    private void syncClock(Message message) {
        if (message != null && message instanceof TimeStampedMessage) {
            if (mClockService != null) {
                TimeStamp timeStamp = ((TimeStampedMessage) message)
                        .getTimeStamp();
                mClockService.synchClock(timeStamp);
            }
        }
    }
    
    private void timeStampMessage(Message message) {
    	if (message != null && message instanceof TimeStampedMessage) {
    		((TimeStampedMessage)message).setTimeStamp(mClockService.getTimeStamp());
    	}
    }
}
