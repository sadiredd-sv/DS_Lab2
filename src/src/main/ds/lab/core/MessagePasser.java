package ds.lab.core;

import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.logging.Level;
import java.util.logging.Logger;

import ds.lab.core.clock.ClockService;
import ds.lab.core.clock.ClockServiceFactory;
import ds.lab.entity.Message;
import ds.lab.entity.TimeStampedMessage;
import ds.lab.util.ConfigParser;
import ds.lab.util.ConfigParser.Node;
import ds.lab.util.Constant;

public class MessagePasser {

    // Sending queue
    private BlockingQueue<Message> mSendQueue = null;

    // Receiving queue
    private BlockingQueue<Message> mReceiveQueue = null;

    // Delayed sending message queue
    private BlockingQueue<Message> mDelayedSendingQueue = null;

    // Delayed receiving message queue
    private BlockingQueue<Message> mDelayedReceivingQueue = null;

    // Configuration file name
    private String mConfigFileName = null;

    private String mLocalName = null;

    // Current sequence number that is added into sending message
    private int mCurrentSeqNum = 1;

    // Configuration parser with YAML format
    private ConfigParser mConfigParser = null;

    private SendingThread mSendingThread = null;

    private ReceivingThread mReceivingThread = null;

    private HashMap<Message, Boolean> mCheckedDelayedMessage = null;

    private HashMap<Message, Boolean> mCheckedDuplciatedMessage = null;

    private Node mNode = null;

    private boolean mEnableLogger = false;

    private ClockService mClockService = null;

    private static Logger mLogger = null;

    static {
        mLogger = Logger.getLogger(MessagePasser.class.getSimpleName());
        mLogger.setLevel(Level.ALL);
    }

    /**
     * Message passer constructor
     * 
     * @param configurationFilename
     * @param localName
     */
    public MessagePasser(String configurationFilename, String localName,
            int clockType) {
        mLogger.log(Level.INFO, "MessagePasser");
        mSendQueue = new LinkedBlockingQueue<Message>();
        mReceiveQueue = new LinkedBlockingQueue<Message>();
        mDelayedSendingQueue = new LinkedBlockingQueue<Message>();
        mDelayedReceivingQueue = new LinkedBlockingQueue<Message>();

        mConfigFileName = configurationFilename;
        mLocalName = localName;
        
        mConfigParser = new ConfigParser(configurationFilename);
        mNode = mConfigParser.getConfiguration(localName);
        
        List<Node> configurationList = mConfigParser.getConfiguration();
        int processIndex = configurationList.indexOf(mNode);
        int numProcesses = configurationList.size() - 1;
        mClockService = ClockServiceFactory.createClockService(clockType, processIndex, numProcesses);

        mSendingThread = new SendingThread(mSendQueue, mDelayedSendingQueue,
                mConfigParser, mClockService);
        mReceivingThread = new ReceivingThread(mReceiveQueue,
                mDelayedReceivingQueue, mNode.getPort(), mClockService);
        mCheckedDelayedMessage = new HashMap<Message, Boolean>();
        mCheckedDuplciatedMessage = new HashMap<Message, Boolean>();
    }

    public void start() {
        if (mSendingThread != null) {
            mSendingThread.start();
        }
        if (mReceivingThread != null) {
            mReceivingThread.start();
        }
    }

    /**
     * Send message
     * 
     * @param message
     */
    public void send(Message message) {
        //mLogger.log(Level.INFO, "send message dest = " + message.getDest()
         //       + ", message source " + message.getSource());
        if (message != null) {
            checkUpdate(mConfigFileName);
            message.setSequenceNumber(mCurrentSeqNum++);
            message.setSource(mLocalName);
            String action = mConfigParser.getSendAction(message);
            if (action == null) {
                // If action equals null
                // No matching rule
                // Normal case
                sendMessage(message);
                return;
            }
            if (action.equals(Constant.ACTION_DROP)) {
                // Drop message
                // Do nothing
            } else if (action.equals(Constant.ACTION_DELAY)) {
                sendDelayedMessage(message);
            } else if (action.equals(Constant.ACTION_DUP)) {
                sendDuplicatedMessage(message);
            } else {
                sendMessage(message);
            }
        }
    }
    
    /**
     * Receive messages
     * 
     * @return message that is received
     */
    public Message receive() {
        Message m = receiveMessage();

        if (m != null && mLocalName != Constant.LOGGER) {
            if (getEnableLog()) {
                Message m2 = createLogMessage(m);
                if (m2 != null) {
                    mSendQueue.add(m2);
                }
            }
        }
        return m;
    }

    public Message receiveMessage() {
        Message message = null;
        if (mReceiveQueue != null) {
            try {
                message = mReceiveQueue.take();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            
            checkUpdate(mConfigFileName);
            String action = mConfigParser.getReceiveAction(message);
            /*
            mLogger.log(Level.INFO,
                    "receive message dest = " + message.getDest()
                            + ", message source " + message.getSource()
                            + ", dup = " + message.isDuplicate()
                            + ", seqNum = " + message.getSequenceNumber());
                            */
            if (action == null) {
                // If action equals null
                // No matching rule
                // Normal case
                trasferDelayedMessage(mReceiveQueue, mDelayedReceivingQueue);
                return message;
            }
            if (action.equals(Constant.ACTION_DROP)) {
                // Drop message
                return null;
            } else if (action.equals(Constant.ACTION_DELAY)) {
                if (isCheckedDelayedMessage(message)) {
                    // If message is checked, then remove it from map
                    mCheckedDelayedMessage.remove(message);
                } else {
                    // Delay
                    receiveDelayedMessage(message);
                    return null;
                }
            } else if (action.equals(Constant.ACTION_DUP)) {
                // Duplicate
                if (isCheckedDuplicatedMessage(message)) {
                    mCheckedDuplciatedMessage.remove(message);
                } else {
                    receiveDuplicateMessage(message);
                }
                trasferDelayedMessage(mReceiveQueue, mDelayedReceivingQueue);
            } else {
                trasferDelayedMessage(mReceiveQueue, mDelayedReceivingQueue);
                return message;
            }
        }
        return message;
    }

    public List<Node> getNodeList() {
        if (mConfigParser != null) {
            return mConfigParser.getConfiguration();
        }
        return null;
    }

    public boolean getEnableLog() {
        return mEnableLogger;
    }

    public void setEnableLogger(boolean flag) {
        mEnableLogger = flag;
    }

    /**
     * Send message
     * 
     * @param message add message into sending queue
     */
    private void sendMessage(Message message) {
        if (mSendQueue != null) {
            mSendQueue.add(message);
            if (getEnableLog()) {
                mSendQueue.add(createLogMessage(message));
            }
        }
    }

    /**
     * Send delayed message
     * 
     * @param message add message into delayed message queue
     */
    private void sendDelayedMessage(Message message) {
        mLogger.log(Level.INFO, "sendDelayedMessagreceiveDelayedMessagee");
        if (mSendQueue != null) {
            // If sending queue has messages, then put delayed message into it.
            // Otherwise, put delayed message into delayed message queue for
            // later sending
            if (mSendQueue.size() == 0) {
                mDelayedSendingQueue.add(message);
                if (getEnableLog()) {
                    mDelayedSendingQueue.add(createLogMessage(message));
                }
            } else if (mSendQueue.size() > 0) {
                mSendQueue.add(message);
                if (getEnableLog()) {
                    mSendQueue.add(createLogMessage(message));
                }
            }
        }
    }

    /**
     * Send duplicated message
     * 
     * @param message duplicate message and send
     */
    private void sendDuplicatedMessage(Message message) {
        mLogger.log(Level.INFO, "sendDelayedMessage");
        if (mSendQueue != null) {
            mSendQueue.add(message);
            Message duplicatedMessage = duplicateMessage(message);
            mSendQueue.add(duplicatedMessage);
            if (getEnableLog()) {
                mSendQueue.add(createLogMessage(message));
                mSendQueue.add(createLogMessage(duplicatedMessage));
            }
        }
    }

    /**
     * Receive delayed message
     * 
     * @param message add delayed message into delayed message queue
     */
    private void receiveDelayedMessage(Message message) {
        mLogger.log(Level.INFO, "receiveDelayedMessage");
        if (mCheckedDelayedMessage != null) {
            mCheckedDelayedMessage.put(message, true);
        }
        if (mReceiveQueue != null) {
            // If sending queue has messages, then put delayed message into it.
            // Otherwise, put delayed message into delayed message queue for
            // later sending
            if (mReceiveQueue.size() == 0) {
                mDelayedReceivingQueue.add(message);
            } else if (mReceiveQueue.size() > 0) {
                mReceiveQueue.add(message);
            }
        }
    }

    /**
     * Receive duplicated message
     * 
     * @param message duplicate message and receive
     */
    private void receiveDuplicateMessage(Message message) {
        mLogger.log(Level.INFO, "receiveDuplicateMessage");
        if (mReceiveQueue != null) {
            Message dupMessage = duplicateMessage(message);
            if (mCheckedDuplciatedMessage != null) {
                mCheckedDuplciatedMessage.put(dupMessage, true);
            }
            mReceiveQueue.add(dupMessage);
        }
    }

    /**
     * Duplicate message that needs to be sent
     * 
     * @param message the message that needs to be duplicated
     * @return duplicated message
     */
    private Message duplicateMessage(Message message) {
        if (message != null) {
            Message dupMsg = new Message(message.getDest(), message.getKind(),
                    message.getData());
            dupMsg.setSource(message.getSource());
            dupMsg.setSequenceNumber(message.getSequenceNumber());
            dupMsg.setDuplicate(true);
            return dupMsg;
        }
        return null;
    }

    /**
     * Check update for config file
     * 
     * @param configFileName config file name
     */
    private void checkUpdate(String configFileName) {
        try {
            mConfigParser.checkUpdate(mConfigFileName);
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void trasferDelayedMessage(BlockingQueue<Message> receiveQueue,
            BlockingQueue<Message> delayedReceivingQueue) {
        if (receiveQueue != null && delayedReceivingQueue != null) {
            int size = delayedReceivingQueue.size();
            for (int i = 0; i < size; ++i) {
                receiveQueue.add(delayedReceivingQueue.remove());
            }
        }
    }

    private boolean isCheckedDelayedMessage(Message message) {
        return mCheckedDelayedMessage.containsKey(message)
                && mCheckedDelayedMessage.get(message);
    }

    private boolean isCheckedDuplicatedMessage(Message message) {
        return mCheckedDuplciatedMessage.containsKey(message)
                && mCheckedDuplciatedMessage.get(message);
    }

    private Message createLogMessage(Message message) {
        if (message != null && message instanceof TimeStampedMessage) {
            Node node = mConfigParser.getConfiguration(Constant.LOGGER);
            Message msg = new Message(node.getName(), Constant.LOGGER, message);
            msg.setSource(mLocalName);
            msg.setSequenceNumber(mCurrentSeqNum++);
            return msg;
        }
        return null;
    }
    
    public BlockingQueue<Message> getSendQueue() {
    	return mSendQueue;
    }
}
