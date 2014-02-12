package ds.lab.app;

import java.awt.BorderLayout;
import java.awt.Color;
import java.awt.Dimension;
import java.awt.event.MouseEvent;
import java.awt.event.MouseListener;
import java.io.File;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import javax.swing.Box;
import javax.swing.BoxLayout;
import javax.swing.JButton;
import javax.swing.JFrame;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.JTextArea;
import javax.swing.JTextField;

import ds.lab.entity.Message;
import ds.lab.entity.TimeStampedMessage;
import ds.lab.multicast.MulticastMessage;
import ds.lab.multicast.MulticastProcess;
import ds.lab.util.ConfigParser.Node;
import ds.lab.util.Constant;

public class TestApp {

    private JPanel mLeftPanel = null;

    private JPanel mRightPanel = null;

    private JFrame mAppFrame = null;

    private JTextField mKindText = null;

    private JTextField mSendText = null;

    private JButton mSendButton = null;

    private JButton mEnableLogButton = null;

    private JTextArea mReceivedText = null;
    
    private MulticastProcess mMulticastProcess;

    private HashMap<Node, Boolean> mSelectedNodeMap = null;
    
    private HashMap<String, Boolean> mSelectedGroupMap = null;

    public TestApp(String localName, MulticastProcess mp,
            List<Node> nodeList) {
        mMulticastProcess = mp;
        mSelectedNodeMap = new HashMap<Node, Boolean>();
        mSelectedGroupMap = new HashMap<String, Boolean>();
        initUI(localName, nodeList);
    }

    private void initUI(String localName, List<Node> nodeList) {
        initMainWindow(localName);
        initLeftPanel(nodeList);
        initRightPanel();
    }

    private void initMainWindow(String localName) {
        mAppFrame = new JFrame(TestApp.class.getSimpleName() + " " + localName);
        mAppFrame.setLayout(new BorderLayout());
        mAppFrame.setSize(600, 400);
        mAppFrame.setLocationRelativeTo(null);
        mAppFrame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
    }

    private void initLeftPanel(List<Node> nodeList) {
        mLeftPanel = new JPanel();
        mLeftPanel.setAlignmentY(1f);
        mLeftPanel.setLayout(new BoxLayout(mLeftPanel, BoxLayout.Y_AXIS));
        mLeftPanel.add(Box.createRigidArea(new Dimension(0, 10)));
        JLabel nodeLabel = new JLabel("Nodes:");
        mLeftPanel.add(nodeLabel);
        mLeftPanel.add(Box.createRigidArea(new Dimension(0, 10)));
        for (final Node node : nodeList) {
            mSelectedNodeMap.put(node, false);
            if (node.getName().equals(Constant.LOGGER)) {
                continue;
            }
            final JTextField nodeField = new JTextField("Name: "
                    + node.getName() + " IP : " + node.getIp());
            nodeField.setEditable(false);
            mLeftPanel.add(nodeField);
            mLeftPanel.add(Box.createRigidArea(new Dimension(0, 20)));
            nodeField.addMouseListener(new MouseListener() {

                @Override
                public void mouseReleased(MouseEvent e) {

                }

                @Override
                public void mousePressed(MouseEvent e) {

                }

                @Override
                public void mouseExited(MouseEvent e) {

                }

                @Override
                public void mouseEntered(MouseEvent e) {

                }

                @Override
                public void mouseClicked(MouseEvent e) {
                    Color foregroundColor = nodeField.getForeground();
                    nodeField.setForeground(nodeField.getBackground());
                    nodeField.setBackground(foregroundColor);
                    mSelectedNodeMap.put(node, !mSelectedNodeMap.get(node));
                }
            });
        }
        for (Map.Entry<String, List<String>> e : mMulticastProcess.getGroups().entrySet()) {
        	final String group = e.getKey();
        	List<String> members = e.getValue();
        	mSelectedGroupMap.put(group, false);
        	
        	final JTextField nodeField = new JTextField("Group: "
                    + group);
            nodeField.setEditable(false);
            mLeftPanel.add(nodeField);
            mLeftPanel.add(Box.createRigidArea(new Dimension(0, 20)));
            nodeField.addMouseListener(new MouseListener() {

                @Override
                public void mouseReleased(MouseEvent e) {

                }

                @Override
                public void mousePressed(MouseEvent e) {

                }

                @Override
                public void mouseExited(MouseEvent e) {

                }

                @Override
                public void mouseEntered(MouseEvent e) {

                }

                @Override
                public void mouseClicked(MouseEvent e) {
                    Color foregroundColor = nodeField.getForeground();
                    nodeField.setForeground(nodeField.getBackground());
                    nodeField.setBackground(foregroundColor);
                    mSelectedGroupMap.put(group, !mSelectedGroupMap.get(group));
                }
            });
        }
        JLabel kindTextLabel = new JLabel("Kind of message: ");
        mLeftPanel.add(kindTextLabel);
        mLeftPanel.add(Box.createRigidArea(new Dimension(0, 10)));
        mKindText = new JTextField("Ack");
        mLeftPanel.add(mKindText);
        mLeftPanel.add(Box.createRigidArea(new Dimension(0, 10)));
        JLabel sendTextLabel = new JLabel("Send text:");
        mLeftPanel.add(sendTextLabel);
        mLeftPanel.add(Box.createRigidArea(new Dimension(0, 10)));
        mSendText = new JTextField("Input your message here");
        mLeftPanel.add(mSendText);
        mLeftPanel.add(Box.createRigidArea(new Dimension(0, 10)));
        mSendButton = new JButton("Send");
        mSendButton.addMouseListener(new MouseListener() {

            @Override
            public void mouseReleased(MouseEvent e) {

            }

            @Override
            public void mousePressed(MouseEvent e) {

            }

            @Override
            public void mouseExited(MouseEvent e) {

            }

            @Override
            public void mouseEntered(MouseEvent e) {

            }

            @Override
            public void mouseClicked(MouseEvent e) {
                Iterator<Entry<Node, Boolean>> iter = mSelectedNodeMap
                        .entrySet().iterator();
                while (iter.hasNext()) {
                    Entry<Node, Boolean> entry = iter.next();
                    if (entry.getValue()) {
                        TimeStampedMessage message = new TimeStampedMessage(
                                entry.getKey().getName(), mKindText.getText(),
                                mSendText.getText());
                        mMulticastProcess.send(message);
                    }
                }
                
                for (Map.Entry<String, Boolean> ent: mSelectedGroupMap.entrySet()) {
                	if (ent.getValue()) {
	                	String group = ent.getKey();
	                	MulticastMessage m = new MulticastMessage(group, null, mKindText.getText(), mSendText.getText());
	                	mMulticastProcess.multicast(m);
                	}
                }
            }
        });
        mLeftPanel.add(mSendButton);
        mLeftPanel.add(Box.createRigidArea(new Dimension(0, 10)));
        mEnableLogButton = new JButton("Enable Log");
        mEnableLogButton.addMouseListener(new MouseListener() {

            @Override
            public void mouseReleased(MouseEvent e) {

            }

            @Override
            public void mousePressed(MouseEvent e) {

            }

            @Override
            public void mouseExited(MouseEvent e) {

            }

            @Override
            public void mouseEntered(MouseEvent e) {

            }

			@Override
			public void mouseClicked(MouseEvent arg0) {
				// TODO Auto-generated method stub
				
			}

            /*
            @Override
            public void mouseClicked(MouseEvent e) {
                if (mMulticastProcess != null) {
                    if (mMulticastProcess.getEnableLog()) {
                        mMulticastProcess.setEnableLogger(false);
                        mEnableLogButton.setText("Enable Log");
                    } else {
                        mMulticastProcess.setEnableLogger(true);
                        mEnableLogButton.setText("Disable Log");
                    }
                }
            }
            */
        });
        mLeftPanel.add(mEnableLogButton);
        mLeftPanel.add(Box.createRigidArea(new Dimension(0, 10)));
        mAppFrame.add(mLeftPanel, BorderLayout.WEST);
    }

    private void initRightPanel() {
        mRightPanel = new JPanel(new BorderLayout());
        mReceivedText = new JTextArea("Received messages");
        mReceivedText.setEditable(false);
        mRightPanel.add(mReceivedText);
        mAppFrame.add(mRightPanel, BorderLayout.CENTER);
    }

    public void display() {
        if (mAppFrame != null) {
            mAppFrame.setVisible(true);
        }
    }

    public void updateMessage(Message message) {
        if (message != null) {
            mReceivedText.append("\nFrom: " + message.getSource() + " Data: "
                    + message.getData());
        }
    }

    private static String CONFIG_FILE_PATH = "config.txt";

    /**
     * @param args
     */
    public static void main(String[] args) {
        MulticastProcess multicastProcess = null;
        TestApp app = null;
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
                //messagePasser = new MessagePasser(configFilePath, localName,
                //        clockType);
            	multicastProcess = new MulticastProcess(configFilePath, localName, clockType);
                List<Node> nodeList = multicastProcess.getNodeList();
                multicastProcess.start();
                app = new TestApp(localName, multicastProcess, nodeList);
                app.display();
            } else {
                System.out.println("Config file is not found!");
            }
        } else {
            System.out
                    .println("Usage: java TestApp [config_file_path] [local_name]");
        }
        while (multicastProcess != null) {
            Message message = multicastProcess.receive();
            //System.out.println(message);
            app.updateMessage(message);
        }
    }
}
