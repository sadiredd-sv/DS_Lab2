package ds.lab.multicast;

import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import ds.lab.core.MessagePasser;
import ds.lab.entity.TimeStampedMessage;
import ds.lab.entity.VectorTimeStamp;

public class MulticastMessage extends TimeStampedMessage {

	private static final long serialVersionUID = -7313377293222780558L;

	private VectorTimeStamp vec;
	private int multiSeqNum = 0;
	private String group;
	
	public VectorTimeStamp getVec() {
		return vec;
	}
	
	public void setVec(VectorTimeStamp v) {
		vec = v.clone();
	}
	
	public MulticastMessage(String group, String dest, String kind, Object data) {
		super(dest, kind, data);
		this.group = group;
	}
	
	
	public int getMultiSeqNum() {
		return multiSeqNum;
	}
	
	public void setMultiSeqNum(int multiSeqNum) {
		this.multiSeqNum = multiSeqNum;
	}


	public String getGroup() {
		return group;
	}

	@Override
	public int hashCode() {
		return group.hashCode() ^
			getDest().hashCode() ^
			getKind().hashCode() ^
			getData().hashCode() ^
			getVec().hashCode();
			//getMultiSeqNum();
	}
	
	@Override
	public boolean equals(Object o) {
		if (!(o instanceof MulticastMessage)) {
			return false;
		}
		MulticastMessage m = (MulticastMessage)o;
		return group.equals(m.group) &&
			getDest().equals(m.getDest()) &&
			getKind().equals(m.getKind()) &&
			getData().equals(m.getData()) &&
			getVec().equals(m.getVec());
			//getMultiSeqNum() == m.getMultiSeqNum();
	}
	
	@Override
	public String toString() {
		return String.format("%s -> %s %d", getSource(), getDest(), multiSeqNum);
	}
	
	public static void main(String[] args) {
		MulticastMessage a = new MulticastMessage("G", "D", "K", "Data");
		MulticastMessage b = new MulticastMessage("G", "D", "K", "Data");
	}
}