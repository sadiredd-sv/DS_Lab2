package ds.lab.core;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.concurrent.BlockingQueue;

import ds.lab.core.clock.ClockService;
import ds.lab.entity.Message;
import ds.lab.entity.TimeStampedMessage;
import ds.lab.util.ConfigParser;
import ds.lab.util.ConfigParser.Node;

/**
 * Sending thread
 */
class SendingThread extends Thread {

    private boolean mIsRunning = false;

    private ConnectionPool mConnectionPool = null;

    private BlockingQueue<Message> mSendQueue = null;

    private BlockingQueue<Message> mDelayedMessageQueue = null;

    private ConfigParser mConfigParser = null;

    private ClockService mClockService = null;

    public SendingThread(BlockingQueue<Message> sendQueue,
            BlockingQueue<Message> delayedMessageQueue,
            ConfigParser configParser, ClockService clockService) {
        mConnectionPool = new ConnectionPool();
        mSendQueue = sendQueue;
        mDelayedMessageQueue = delayedMessageQueue;
        mConfigParser = configParser;
        mClockService = clockService;
    }

    public void stopThread() {
        mIsRunning = false;
        mConnectionPool.release();
    }

    @Override
    public void run() {
        mIsRunning = true;
        while (mIsRunning) {
            Message message = getMessage();
            timeStampMessage(message);
            if (message != null) {
                sendData(message);
            }
        }
    }

    private Message getMessage() {
        Message message = null;
        if (mSendQueue != null) {
            try {
                message = mSendQueue.take();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        return message;
    }

    private void sendData(Message message) {
        if (message != null) {
            Node node = mConfigParser.getConfiguration(message.getDest());
            // Socket socket = mConnectionPool.openSocket(node);
            SocketChannel socketChannel = null;
            ObjectOutputStream oos = null;
            try {
                SocketAddress socketAddress = new InetSocketAddress(
                        node.getIp(), node.getPort());
                //System.out.println("node " + node.getIp() + node.getPort());
                socketChannel = SocketChannel.open();
                socketChannel.connect(socketAddress);
                // oos = new ObjectOutputStream(socket.getOutputStream());
                // oos.writeObject(message);
                // oos.flush();
                byte[] bytes = serialize(message);
                ByteBuffer buffer = ByteBuffer.wrap(bytes);
                socketChannel.write(buffer);
                trasferDelayedMessage(mSendQueue, mDelayedMessageQueue);
            } catch (IOException e) {
                e.printStackTrace();
            } finally {
                try {
                    if (oos != null) {
                        oos.close();
                    }
                    if (socketChannel != null) {
                        socketChannel.close();
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    private void trasferDelayedMessage(BlockingQueue<Message> sendQueue,
            BlockingQueue<Message> delayedMessageQueue) {
        if (sendQueue != null && delayedMessageQueue != null) {
            int size = delayedMessageQueue.size();
            for (int i = 0; i < size; ++i) {
                sendQueue.add(delayedMessageQueue.remove());
            }
        }
    }

    private byte[] serialize(Object object) {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ObjectOutputStream oos = null;
        byte[] bytes = null;
        try {
            oos = new ObjectOutputStream(baos);
            oos.writeObject(object);
            bytes = baos.toByteArray();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                if (oos != null) {
                    oos.close();
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        return bytes;
    }

    private void timeStampMessage(Message message) {
        if (message != null && message instanceof TimeStampedMessage) {
            if (mClockService != null) {
                ((TimeStampedMessage) message).setTimeStamp(mClockService
                        .generateTimeStamp());
            }
        }
    }
}
