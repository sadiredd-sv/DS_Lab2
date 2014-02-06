package Lab1;

import java.io.*;
import java.util.ArrayList;

public class Initiate
{
	private static final String PROMPT = "> ";

	public static String parseInput(String input)
	{
		if(input == null)
			return null;
		String[] temp = input.trim().split(" ");
		if(temp.length > 4)
		{
			/* Usage */
			System.out.println("Options: \n 1. send <dest> <kind> <data> <clocktype>\n 2. receive\n 3. multicast <group_id>\n 4. EXIT");
			return null;
		}
		if
		(
				(temp.length == 1 && !(temp[0].equals("receive")) && !(temp[0].equals("debug")) && !(temp[0].equals("groups")))
				||
				(temp.length == 2 && !(temp[0].equals("multicast")))
				||
				(temp.length == 3 && !(temp[0].equals("send")))
				|| 
				(temp.length == 4 && (!(temp[0].equals("send")) || !(temp[3].equals("log"))))
		)
		{
			System.out.println("Options: \n 1. send <dest> <kind> <data> <clocktype>\n 2. receive\n 3. multicast <group_id>\n 4. EXIT");
			return null;
		}
		return input.trim();
	}

	public static void main(String[] args)
	{
		if(args.length != 2)
		{
			System.out.println("Usage: java lab2 <ConfigFile> <ProcessName>");
			System.exit(1);
		}
		
		MessagePasser mp = new MessagePasser(args[0], args[1]); //clockFactory and clockFactory.getClockType()-- logical or vector :: This is written in MessagePasser class
		
		Receiver recv = new Receiver(mp, args[1]);
		recv.start();
		MulticastHandler dw = new MulticastHandler(mp, args[1]);
		dw.start();

		String input = null;
		BufferedReader br = null;
		
		System.out.println("Options: \n 1. send <dest> <kind> <data> <clocktype>\n 2. receive\n 3. multicast <group_id>\n 4. exit");
		try{
			br = new BufferedReader(new InputStreamReader(System.in));
			while(true)
			{
				/* Get user input */
				input = br.readLine();
				if(input != null)
				{
					if(input.equals("exit"))
						System.exit(1);
					if((input = parseInput(input)) != null)
					{
						if(input.equals("receive"))
						{
							ArrayList<TimeStampedMessage> tm_arr = mp.receive();
							if(tm_arr == null || tm_arr.size() == 0)
								System.out.println("No new message.");
							else
							{
								for(TimeStampedMessage tm: tm_arr)
								{
									System.out.println(tm);
								}
							}
						}
						else if(input.equals("groups"))
						{
							mp.show_groups();
						}
						else if(input.startsWith("multicast"))
						{
							String[] temp = input.split(" ");
							try
							{
								int group_num = Integer.parseInt(temp[1]);
								int status = 0;
								if((status = mp.check_group(args[1], group_num)) == 0)
								{
									System.out.print("Kind: ");
									String kind = br.readLine();
									System.out.print("Input message: ");
									input = br.readLine();

									MulticastMessage r_msg = new MulticastMessage(args[1], null, kind, input, wq.getClock().inc(), wq.getClock());
									mp.R_multicast(r_msg, group_num);
									
									/** This is what I have written in Message Passer
									 * 
									private HashMap<Integer, MulticastMessage> sent_msgs = new HashMap<Integer, MulticastMessage>();
									
									private HashMap<Integer, ArrayList<String> > groups = new HashMap<Integer, ArrayList<String> >(); //This groups is set after parsing the config file
									
									public void R_multicast(MulticastMessage r_msg, int group_num) throws InterruptedException
									{
										r_msg.setSeqNumber(seq_num);
										r_msg.setMulticastTimeStamp(clockFactory.getClockType().increment());
										sent_msgs.put(seq_num, r_msg);
										seq_num++;
										for(String dest: groups.get(group_num))
										{
											MulticastMessage t_rmsg = r_msg.Copy();
											t_rmsg.setDest(dest);
											send(t_rmsg);
										}
									}
									**/
								}
								else if(status == -1)
								{
									System.out.println("No such group: " + group_num + ", use command \"groups\" to check group information.");
								}
								else if(status == -2)
								{
									System.out.println(args[1] + " isn't in group " + group_num + ", use command \"groups\" to check group information.");
								}
							}
							catch(NumberFormatException nex)
							{
								System.out.println("<group_id> should be an integer.");
							}
						}
						else
						{
							String[] temp = input.split(" ");
							if(mp.getUsers().get(temp[1]) != null)
							{
								System.out.print("Input message: ");
								input = br.readLine();
								TimeStampedMessage tm = new TimeStampedMessage(args[1], temp[1], temp[2], input, wq.getClock().inc(), wq.getClock());
								tm.set_id(mp.incId());
								mp.send(tm);
								if(temp.length == 4)
								{
									TimeStampedMessage tm_2 = tm.copy(); // This Copy() method clones the TimeStampedMessage
									tm_2.setSrc(tm_2.getSrc() + "$" + tm_2.getDest());
									tm_2.setDest("logger");
									mp.getSendQueue().put(tm_2);
								}
							}
							else
							{
								System.out.println("Error! No such user: " + temp[1]);
							}
						}
					}
					else
						System.out.println("");
				}
			}
		}
		catch(Exception ioe)
		{
			ioe.printStackTrace();
		}
		finally
		{
			try{
				if(br != null)
					br.close();
			}
			catch(IOException ioe)
			{
				ioe.printStackTrace();
			}
		}
	}
}