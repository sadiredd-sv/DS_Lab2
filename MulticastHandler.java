package Lab1;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;
import java.net.*;

public class MulticastHandler extends Thread
{
	private MessagePasser mp;
	private String local_name;
	private boolean flag = true;

	public DeliverWorker(MessagePasser mp, String local_name)
	{
		this.mp = mp;
		this.local_name = local_name;
	}
	public void setFlag(){flag = false;}
	public void run()
	{
		TimeStampedMessage t_msg = null;
		BlockingQueue<TimeStampedMessage> receiveQueue = mp.getReceiveQueue();
		ConcurrentLinkedQueue<TimeStampedMessage> appReceiveQueue = mp.getAppReceiveQueue();
		ConcurrentLinkedQueue<MulticastMessage> holdbackQueue = mp.getHoldbackQueue();
		HashMap<Integer, MulticastMessage> sentMessages = mp.getSentMsgs();

		while(flag)
		{
			try
			{
				t_msg = receiveQueue.take();
			}
			catch(InterruptedException x)
			{
				if(!flag)
					break;
				else
					x.printStackTrace();
			}
			if(!(t_msg instanceof MulticastMessage))
			{
				appReceiveQueue.add(t_msg);
				continue;
			}
			
			if(t_msg.getKind().equals("M_NACK"))
			{
				System.out.println("Received NACK from: " + t_msg.getSrc());
				int request_seqnum = ((MulticastMessage)t_msg).getSeqNum();
				
				/* Send t_msg.src's all missing msgs */
				while(request_seqnum < mp.getSeqNum())
				{
					if(sentMessages.get(request_seqnum) == null)
						System.out.println("null request_seqnum: " + request_seqnum);
					MulticastMessage m_msg = sentMessages.get(request_seqnum).copy();
					m_msg.setDest(t_msg.getSrc());
					
					mp.send(m_msg);
					request_seqnum++;
					System.out.println("resend: " + m_msg);
				}
				continue;
			}
			mp.getClockFactory().syncWithMulticastClock((MulticastMessage)t_msg); // Only vector clock is used
			
			/** Clock Synchronization method for multicastMessage in VectorClock.java :
			synchronized public void syncWithMulticastClock(MulticastMessage mm_msg)
			{
				for(int i = 0; i < timestamp.length; i++)
				{
					timestamp[i] = Math.max(timestamp[i], (mm_msg.getMTimeStamp())[i]);
				}
				timestamp[local_id]++;
			} 
			
			 */
			
			System.out.println("Received in MulticastHandler: " + t_msg);
			
			if(((MulticastMessage)t_msg).getSeqNum() == (mp.getRqg().get(t_msg.getSrc()) + 1))
			{
				/*----------------- Deliver it -----------------*/
				appReceiveQueue.add(t_msg);
				mp.getRqg().put(t_msg.getSrc(), ((MulticastMessage)t_msg).getSeqNum()); // R_qg++
				
				/* Deliver all pending msgs from t_msg.src that could be delivered*/
				for(MulticastMessage mm_msg: holdbackQueue)
				{
					if(mm_msg.getSrc().equals(t_msg.getSrc()))
					{
						if(mm_msg.getSeqNum() == mp.getRqg().get(t_msg.getSrc()) + 1)
						{
							holdbackQueue.remove(mm_msg);
							appReceiveQueue.add(mm_msg);
							mp.getRqg().put(t_msg.getSrc(), mm_msg.getSeqNum());//R_qg++
						}
					}
				}
			}
			else if(((MulticastMessage)t_msg).getSeqNum() > (mp.getRqg().get(t_msg.getSrc()) + 1))
			{
				holdbackQueue.add((MulticastMessage)t_msg);
				/* Send M_NACK to src for missing msgs */
				MulticastMessage tm_msg = new MulticastMessage(local_name, t_msg.getSrc(), "M_NACK", null, null, null);
				tm_msg.setSeqNum(mp.getRqg().get(t_msg.getSrc())+1);
				mp.send(tm_msg);
				System.out.println("send NACK to: " + tm_msg.getDest());
			}
			else
				;
		}
	}
}
