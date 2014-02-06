import java.util.Arrays;

public class MulticastMessage extends TimeStampedMessage implements Serializable {
	
	private int seqNumber;
	private int[] multicastTimestamp;/* vector used in multicast */
	
	public MulticastMessage(String dest, String kind, String data, Object timestamp,
			ClockService clockservice) {
		super(dest, kind, data, timestamp, clockservice);
		
	}
	
	public void setSeqNumber(int seqNumber){
		this.seqNumber = seqNumber;
	}
	
	public int getSeqNumber(){
		return seqNumber;
	}
	
	public void setMulticastTimeStamp(int[] multicastTimestamp){
		this.multicastTimestamp = multicastTimestamp;
	}
	public int[] getMulticastTimeStamp(){
		return multicastTimestamp;
	}
	
	public String toString()
	{
		return (super.toString() + ", SequenceNumber: " + seqNumber + ", MulticastTimestamp:" + Arrays.toString(multicastTimestamp));
	}

}

