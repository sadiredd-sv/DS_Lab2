package ds.lab.multicast;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.logging.Level;
import java.util.logging.Logger;

import ds.lab.core.MessagePasser;
import ds.lab.entity.Message;
import ds.lab.entity.VectorTimeStamp;
import ds.lab.util.ConfigParser;
import ds.lab.util.ConfigParser.Node;

public class MulticastProcess {
	
	
	static Logger logger;
	{
		logger = Logger.getLogger(MessagePasser.class.getSimpleName());
        logger.setLevel(Level.ALL);
    }

	
	//	Each process pi ( i = 1  2     N ) maintains its own vector timestamp
	//	(see Section 14.4).The entries in the timestamp count the number of
	//	multicast messages from each process that happened-before the next 
	//	message to be multicast.
	private String name;
	
	private int seqNum;
	
	private MessagePasser messagePasser;
	
	private ConfigParser config;
	
	private int processIndex;
	
	private int numProcesses;
	
	private Map<String, List<String>> groups;
	
	private Map<String, VectorTimeStamp> groupTimestamps;
	
	private BlockingQueue<MulticastMessage> holdbackQueue;
	
	private BlockingQueue<Message> deliveryQueue;
	
	private Set<MulticastMessage> receivedSet;
	
	public MulticastProcess(String configName, String name, int clockType) {
		this.name = name;
		messagePasser = new MessagePasser(configName, name, clockType);
		config = new ConfigParser(configName);
		processIndex = config.getProcessIndex(name);
		numProcesses = config.getNumProcesses();
		groups = config.getGroups();
		seqNum = 0;
		
		groupTimestamps = new HashMap<String, VectorTimeStamp>();
		for (Map.Entry<String, List<String>> e: groups.entrySet()) {
			String groupName = e.getKey();
			//List<String> members = e.getValue();
			groupTimestamps.put(groupName, new VectorTimeStamp(0, processIndex, numProcesses));
		}
		
		holdbackQueue = new LinkedBlockingQueue<MulticastMessage>();
		deliveryQueue = new LinkedBlockingQueue<Message>();
		receivedSet = new HashSet<MulticastMessage>();
	}
	
	public void start() {
		(new Thread(new Receiver())).start();
	}
	
	public void send(Message m) {
		messagePasser.send(m);
	}
	
	public List<Node> getNodeList() {
		return messagePasser.getNodeList();
	}
	
	public void multicast(MulticastMessage m) {
		m.setMultiSeqNum(++seqNum);
		CO_multicast(m);
	}
	
	public void R_multicast(MulticastMessage msg) {
		B_multicast(msg);
	}
	
	public void B_multicast(MulticastMessage msg) {
		String group = msg.getGroup();
		//System.out.println(groups.get(group));
		for (String dest : groups.get(group)) {
			//System.out.println(dest);
			MulticastMessage m2 = new MulticastMessage(group, dest, msg.getKind(), msg.getData());
			m2.setMultiSeqNum(msg.getMultiSeqNum());
			m2.setVec(msg.getVec());
			messagePasser.send(m2);
		}
	}
	
	public void CO_multicast(MulticastMessage msg) {
		//	To CO-multicast a message to group g, the process adds 1 to its 
		//	entry in the timestamp and B-multicasts the message along with 
		//	its timestamp to g.
		String group = msg.getGroup();
		VectorTimeStamp timestamp = groupTimestamps.get(group);
		timestamp = timestamp.next(1);
		groupTimestamps.put(group, timestamp);
		//System.out.println(timestamp);
		msg.setVec(timestamp);
		R_multicast(msg);
	}
	
	public Message receive() {
		Message m = null;
		try {
			m = deliveryQueue.take();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		return m;
	}
	

	
	public class Receiver implements Runnable {
		
		@Override
		public void run() {
			messagePasser.start();
			while (true) {
				Message m = messagePasser.receive();
				if (m == null) {
					continue;
				} else if (m instanceof MulticastMessage) {
					// XXX
					R_deliver((MulticastMessage)m);
				} else {
					try {
						deliveryQueue.put(m);
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
				}
			}
		}
		
		public void R_deliver(MulticastMessage msg) {
			System.out.println("========\n" + msg);
			if (receivedSet.contains(msg)) {
				System.out.println(name + " ******received " + msg.getSource());
				return;
			}
			receivedSet.add(msg);
			CO_deliver(msg);

			if (msg.getSource() != name) {
				B_multicast(msg);
			}
		}
		
		public void CO_deliverHelper(MulticastMessage msg) {
			holdbackQueue.remove(msg);
			try {
				deliveryQueue.put(msg);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
			
		public void CO_deliver(MulticastMessage msg) {
			// a process can immediately CO-deliver to itself any message that it CO-multicasts
			if (msg.getSource().equals(name)) {
				CO_deliverHelper(msg);
//				String group = msg.getGroup();
//				VectorTimeStamp t = groupTimestamps.get(group);
//				int j = ((VectorTimeStamp) msg.getTimeStamp()).getProcessIndex();
//				t.set(j, t.get(j) + 1);
				return;
			}
			
			try {
				holdbackQueue.put(msg);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			
			String group = msg.getGroup();
			VectorTimeStamp vi = groupTimestamps.get(group);
			
			
			for (MulticastMessage m : holdbackQueue) {
				VectorTimeStamp vj = m.getVec();
				System.out.println(vj);
				int j = vj.getProcessIndex();
				
				if (vj.get(j) == vi.get(j) + 1) {
					boolean canDeliver = true;
					for (int k = 0; k < numProcesses; ++k) {
						if (k != j && vj.get(k) > vi.get(k)) {
							canDeliver = false;
							break;
						}
					}
					if (canDeliver) {
						CO_deliverHelper(m);
						vi.set(j, vi.get(j) + 1);
					}
				}
			}
		}
	}
	
	public Map<String, List<String>> getGroups() {
		return config.getGroups();
	}
}
