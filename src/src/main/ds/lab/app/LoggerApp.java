package ds.lab.app;

import java.awt.BorderLayout;
import java.awt.Dimension;
import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.swing.Box;
import javax.swing.BoxLayout;
import javax.swing.JButton;
import javax.swing.JFrame;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.JTextArea;

import ds.lab.core.MessagePasser;
import ds.lab.entity.Message;
import ds.lab.entity.TimeStamp;
import ds.lab.entity.TimeStampedMessage;
import ds.lab.util.Constant;

public class LoggerApp {

    private JPanel mLeftPanel = null;

    private JPanel mRightPanel = null;

    private JFrame mAppFrame = null;

    private JTextArea mLogText = null;

    private JButton mRequestButton = null;

    private static String CONFIG_FILE_PATH = "config.txt";
    
    private Map<String, List<TimeStamp>> mTimeStamps;

    public LoggerApp(String localName) {
        initUI(localName);
        
        mTimeStamps = new HashMap<String, List<TimeStamp>>();
    }

    public void initUI(String localName) {
        initMainWindow(localName);
        initLeftPanel();
        initRightPanel();
    }

    private void initMainWindow(String localName) {
        mAppFrame = new JFrame(TestApp.class.getSimpleName() + " " + localName);
        mAppFrame.setLayout(new BorderLayout());
        mAppFrame.setSize(600, 400);
        mAppFrame.setLocationRelativeTo(null);
        mAppFrame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
    }

    private void initLeftPanel() {
        mLeftPanel = new JPanel();
        mLeftPanel.setAlignmentY(1f);
        mLeftPanel.setLayout(new BoxLayout(mLeftPanel, BoxLayout.Y_AXIS));
        mLeftPanel.add(Box.createRigidArea(new Dimension(0, 10)));
        mRequestButton = new JButton("Request Logs");
        mLeftPanel.add(mRequestButton);
        mAppFrame.add(mLeftPanel, BorderLayout.WEST);
    }

    private void initRightPanel() {
        mRightPanel = new JPanel(new BorderLayout());
        mLogText = new JTextArea("Logs:");
        mLogText.setEditable(false);
        JScrollPane scrollPane = new JScrollPane(mLogText);
        scrollPane.setVerticalScrollBarPolicy(JScrollPane.VERTICAL_SCROLLBAR_AS_NEEDED);
        scrollPane.setHorizontalScrollBarPolicy(JScrollPane.HORIZONTAL_SCROLLBAR_AS_NEEDED);
        mRightPanel.add(scrollPane);
        mAppFrame.add(mRightPanel, BorderLayout.CENTER);
    }

    public void display() {
        if (mAppFrame != null) {
            mAppFrame.setVisible(true);
        }
    }

    public void updateMessage(Message message) {
        if (message != null) {
            // System.out.println("updateMessage");
            String source = message.getSource();
            TimeStampedMessage timeStampedMsg = ((TimeStampedMessage) message
                    .getData());
            System.out.println(timeStampedMsg.getSource() + " -> "
                    + timeStampedMsg.getDest() + " msg: "
                    + timeStampedMsg.getData());
            TimeStamp timestamp = timeStampedMsg.getTimeStamp();
            if (!mTimeStamps.containsKey(source)) {
                mTimeStamps.put(source, new ArrayList<TimeStamp>());
            }
            mTimeStamps.get(source).add(timestamp);

            updateLogText();
        }
    }
    
    private void updateLogText() {
    	mLogText.setText("");

    	for (Map.Entry<String, List<TimeStamp>> entry : mTimeStamps.entrySet()) {
    		String nodeName = entry.getKey();

    		mLogText.append("TimeStamps in " + nodeName + ":\n");
    		List<TimeStamp> timestamps = entry.getValue();
    		for (TimeStamp t : timestamps) {
    			mLogText.append(t + "\n");
    		}
    		mLogText.append("\n");
    	}

    	mLogText.append("Concurrent TimeStamps:\n");

    	for (Map.Entry<String, List<TimeStamp>> ea : mTimeStamps.entrySet()) {
    		for (Map.Entry<String, List<TimeStamp>> eb : mTimeStamps.entrySet()) {
    			String nodeNameA = ea.getKey();
    			List<TimeStamp> listA = ea.getValue();
    			String nodeNameB = eb.getKey();
    			List<TimeStamp> listB = eb.getValue();

    			if (nodeNameA == nodeNameB) continue;

    			for (int i = 0; i < listA.size(); ++i) {
    				for (int j = 0; j < listB.size(); ++j) {
    					TimeStamp ta = listA.get(i);
    					TimeStamp tb = listB.get(j);

    					if (ta.compareTo(tb) == 0) {
    						mLogText.append(String.format("%s:%s and %s: %s\n", nodeNameA, ta, nodeNameB, tb));
    					}
    				}
    			}
    		}
    	}
    }

    /**
     * @param args
     */
    public static void main(String[] args) {
        MessagePasser messagePasser = null;
        LoggerApp app = null;
        if (args != null) {
            String configFilePath = null;
            String localName = null;

            HashMap<String, Integer> clockMap = new HashMap<String, Integer>();
            clockMap.put("logical", Constant.LOGICAL_CLOCK);
            clockMap.put("vector", Constant.VECTOR_CLOCK);

            int clockType = 0;
            switch (args.length) {
            case 3:
                configFilePath = args[0];
                localName = args[1];
                clockType = clockMap.get(args[2]);
                break;
            case 2:
                configFilePath = args[0];
                localName = args[1];
                break;
            case 1:
                configFilePath = CONFIG_FILE_PATH;
                localName = args[0];
                break;
            default:
                System.out
                        .println("Usage: java TestApp [config_file_path] [local_name] [logical | vector]");
                return;
            }
            if (new File(configFilePath).exists()) {
                messagePasser = new MessagePasser(configFilePath, localName,
                        clockType);
                messagePasser.start();
                app = new LoggerApp(localName);
                app.display();
            } else {
                System.out.println("Config file is not found!");
            }
        } else {
            System.out
                    .println("Usage: java TestApp [config_file_path] [local_name]");
        }
        while (messagePasser != null) {
            Message message = messagePasser.receive();
            // System.out.println(message);
            app.updateMessage(message);
        }
    }
}
