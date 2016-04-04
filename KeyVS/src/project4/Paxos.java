package project4;

import java.io.IOException;
import java.io.Serializable;
import java.net.InetAddress;
import java.net.MalformedURLException;
import java.net.UnknownHostException;
import java.rmi.AlreadyBoundException;
import java.rmi.Naming;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Semaphore;

import project4.UtilityClasses.*;

import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.FileAppender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;

/*
 * 
 * Responsible for managing a sequence of agreed upon values to provide consensus.
 * Paxos helper for each server exists that handles network failures, partitions and message loss.
 * Not capable of performing well in a crash-restart scenario as the statusMap, acceptorMap is
 * not persisted in disk. However, this could be objective of future iterations.
 */

public class Paxos extends UnicastRemoteObject implements PaxosInterface{
	/**
	 * 
	 */
	final static String PATTERN = "%d [%p|%c|%C{1}] %m%n";
	private static final long serialVersionUID = 1L;
	final static Logger log = Logger.getLogger(Paxos.class);
	public static final String OK = "OK";
	public static final String REJECT = "REJECT";
	public List<HostPorts> peers;
	public static int me;
	public Semaphore mutex = new Semaphore(1);
	public static volatile HashMap<Integer, AcceptorState> acceptorStateMap;	
	public static volatile HashMap<Integer, Status> statusMap;
	public static volatile int max;
	public static volatile HashMap<Integer, Integer> peersDoneValue;
	public static volatile int min;
	public static volatile boolean crashed;
	private static final long CRASH_DURATION = 1500;
	protected Paxos(List<HostPorts> peers, int me) throws AlreadyBoundException, IOException {
		super();
		configureLogger();
		this.peers = peers;
		this.me = me;
		int length = peers.size();
		acceptorStateMap = new HashMap<Integer,AcceptorState>();
		statusMap = new HashMap<Integer, Status>();
		max =-1;
		min =0;
		peersDoneValue = new HashMap<Integer, Integer>();
		for(int i =0; i < length; i++)
			peersDoneValue.put(i, -1);	
	}
	
	/* Configure the log4j appenders
	 */
	static void configureLogger()
	{
		ConsoleAppender console = new ConsoleAppender(); //create appender
		//configure the appender
		console.setLayout(new PatternLayout(PATTERN)); 
		console.setThreshold(Level.ALL);
		console.activateOptions();
		//add appender to any Logger (here is root)
		log.addAppender(console);
		// This is for the rmi_server log file
		FileAppender fa = new FileAppender();
		fa.setName("FileLogger");
		fa.setFile("log/_rmi_server.log");
		fa.setLayout(new PatternLayout("%d %-5p [%c{1}] %m%n"));
		fa.setThreshold(Level.ALL);
		fa.setAppend(true);
		fa.activateOptions();

		//add appender to any Logger (here is root)
		log.addAppender(fa);
		log.setAdditivity(false);
		//repeat with all other desired appenders
	}
		
	/*
	 * Quorum reaching constraint.
	 */
	public boolean isMajority(int num)
	{
		return num > peers.size()/2;
	}
	
	
	/*
	 * Need to ensure that the proposal number is 
	 * increasing with each round and must be unique per instance across all nodes, higher than any sequence
	 * previously seen.
	 */
	public int GenerateProposalNumber(int maxsofar)
	{
		int length = peers.size();
		return maxsofar + length - (maxsofar%length)+ me;
	}
	/*
	 * This checks the local status map to check if an instance has been decided on 
	 * and if it has been decided on, then return the accepted value to the the invoker
	 * it checks only the local status status map. Not asking the other Paxos peers 
	 * their status.
	 */
	public Status Status(int sequence) throws InterruptedException
	{
		try{
		mutex.acquire();
		
		boolean exists = statusMap.containsKey(sequence);
		if(exists)
		{
			return statusMap.get(sequence);
		}	
		return new Status(null, false);
		}
		finally{
			mutex.release();
		}
	}
	
	
	/*
	 * The RMIServerInterfaceImpl invokes its Paxos helper's Start method
	 * to start agreement on sequence number, sequence and the proposed value, value.
	 * This method returns right away, however, the Status method is invoked
	 * to find if an agreement is reached on a particular sequence number.
	 * 
	 */
	public void StartConsensus(final int sequence, final String value) throws Exception
	{

		try{
		mutex.acquire();
		updateMax(sequence);
		
		if(!statusMap.containsKey(sequence) && sequence >= min)
		{
			statusMap.put(sequence, new Status(null, false));
			
			Thread t = new Thread(new Runnable() {
			    public void run() {
			    	try {
						StartProposing(sequence,value);
					} catch (MalformedURLException | RemoteException
							| UnknownHostException | NotBoundException
							| InterruptedException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
			    }
			});

			t.start();
			
		}
		}
		finally{
		mutex.release();
		}
	}

	// proposer(v):
	//  choose n, unique and higher than any n seen so far
	//  send prepare(n) to all servers including self
	//  if prepare_ok(n_a, v_a) from majority:
	//    v' = v_a with highest n_a; choose own v otherwise
	//    send accept(n, v') to all
	//    if accept_ok(n) from majority:
	//      send decided(v') to all

	//Phase 1. 
	//(a) A proposer selects a proposal number n and sends a prepare
	//    request with number n to a majority of acceptors.
	//(b) If an acceptor receives a prepare request with number n greater
	//    than that of any prepare request to which it has already responded,
	//    then it responds to the request with a promise not to accept any more
	//    proposals numbered less than n and with the highest-numbered proposal (if any) that it has accepted.
	
	public void StartProposing(int sequence, String value) throws MalformedURLException, RemoteException, NotBoundException, InterruptedException, UnknownHostException
	{
		int maxProposalNo =0;
		int length = peers.size();
		while(true)
		{
			boolean Learned = false;
			mutex.acquire();
			Status now = (statusMap.containsKey(sequence)) ? statusMap.get(sequence) : null;
			if(now != null)
			{
				Learned = now.isDone();
			}	
			int current_min = min;


			mutex.release();

			if(Learned ||   sequence < current_min)
				break;
			else
			{
				int n = GenerateProposalNumber(maxProposalNo);
				int prepare_ok_count = 0;
				int n_a = 0;
				int count = 0;
				String v_a = null;
				for(HostPorts peer: peers)
				{

					PrepareArgs prepareArgs = new PrepareArgs(sequence, n,0);
					PrepareReply prepareReply = null;
					if (count ++ == me)
					{
						prepareReply = Prepare(prepareArgs);
					}
					else
					{
						String host = InetAddress.getByName(peer.getHostName()).getHostAddress();
						PaxosInterface peerImpl = (PaxosInterface) Naming.lookup("rmi://" + host.trim() + ":" + peer.getPort() + "/Paxos" );
						prepareReply = peerImpl.Prepare(prepareArgs);
					}
					
					if(prepareReply != null)
					{
						if(prepareReply.getHighestPrepareNo() > maxProposalNo)
						{
							maxProposalNo = prepareReply.getHighestPrepareNo();
						}
						if(prepareReply.isOk())
						{
							prepare_ok_count++;
							if(prepareReply.getHighestPrepareNo() > n_a)
							{
								n_a = prepareReply.getHighestProposalNo();
								v_a = prepareReply.getValue();
							}
						}
					}
				}
				String v_prime;
				if(prepare_ok_count > (length/2))
				{
					v_prime = value;
					if(v_a != null)
						v_prime = v_a;
					
					int accept_ok_count =0;
					count = 0;
					AcceptReply acceptReply = null;
					for (HostPorts peer: peers)
					{

						AcceptArgs acceptArgs = new AcceptArgs(sequence, n,v_prime );
						if(count ++ == me)
						{
								acceptReply = Accept(acceptArgs);
						
						}
						else
						{
								PaxosInterface peerImpl = (PaxosInterface) Naming.lookup("rmi://" + peer.getHostName() + ":" + peer.getPort() + "/Paxos" );
								try{
								acceptReply = peerImpl.Accept(acceptArgs);
								}
								catch(Exception e)
								{	
									acceptReply = null;
									log.info(peer.getHostName() + ":" + peer.getPort() + " unreachable didn't accept " );
								}

						}
						if(acceptReply != null)
							if(acceptReply.isOK())
								accept_ok_count++;
							else
								log.info(peer.getHostName() + ":" + peer.getPort() + " simply didn't accept " );
					}
					log.info("Accepted number = " + accept_ok_count);
//				   if accept_ok(n) from majority:
//			       send decided(v') to all
					if(accept_ok_count > (length /2))
					{
						log.info(" REQUEST ACCEPTED :)");
						mutex.acquire();
						Integer MaxDoneSeq = peersDoneValue.get(me);
						mutex.release();
						count =0;
						for(HostPorts peer: peers)
						{
							LearnArgs LearnArgs = new LearnArgs(sequence, v_prime, me, MaxDoneSeq);

							if(count ++ == me)
								Learn(LearnArgs);
							else
							{
								PaxosInterface peerImpl = (PaxosInterface) Naming.lookup("rmi://" + peer.getHostName() + ":" + peer.getPort() + "/Paxos" );
								peerImpl.Learn(LearnArgs);
							}
						}

					}
					else
						log.info(" REQUEST REJECTED :)");
				}
			}
		}
	}	
	/*
	 * The RMIServerInterfaceImpl can invoke this method to 
	 * know the max sequence number that is known to this Paxos helper
	 */
	public int Max()
	{
		return max;	
	}
	/*
	 * this is defined as the minimum over all the Paxos peers
	 * so Min() cannot increase until all the the peers have been heard from.
	 * Each Paxos peers will call its Done() method, which in turn calls the 
	 * cleanUpMemory() method to implement the Min() on its highest known sequence numbers.
	 * This means that until all the peers call their Done() methods on a sequence numbers > =j
	 * the Min() will not increase on any peer beyond j.
	 * This Min() is implemented so that if a server suddenly becomes unreachable due to a network partition or
	 * a heavy congestion in the network packets are being dropped, or it has simply crashed due to lack of memory to accomadate
	 * new requests, then once it comes back alive( note: if it restarts, this code won't work as statusMap and acceptorMaps are not persisted to disk)
	 * then all the sequence numbers from its current Min() up to the the catchup sequence number need to be applied
	 * This would involve calling the peers and asking them what the proposed value for a sequence number was
	 * so the peers cannot forget about these instances.
	 
	 * 
	 */
	public int Min()
	{
		return min;
	}
	
	/*
	 * This server has applied all updates for instances
	 * up to the the sequence number provided in the arguments.
	 * i.e all instances <= sequence have been applied.
	 * Calling a Done(x) would render a Start(y) on any y <= x illegal.
	 * We are claiming that all sequence nunmbers less than sequence have been safely updated to its log.
	 * And we can clean up the memory for this, i.e historical information of the statusMap for instances <= sequence.
	 * We clean this memory so as to save virtual memory heap size from overflowing in 
	 * long running servers.  
	 */
	public void Done(int sequence) throws InterruptedException
	{
		try{
		mutex.acquire();
		peersDoneValue.put(me,sequence);
		cleanMemory();
		}
		finally{
			mutex.release();
		}
	}
	
	/*
	 * updates the max sequence number seen on this Paxos helper.
	 */
	public void updateMax(int sequence)
	{
		if(sequence > max )
			max = sequence;
	}
	@Override
	public LearnReply Learn(LearnArgs LearnArgs) throws RemoteException, InterruptedException
	{
		try{
		mutex.acquire();
		int seqNo = LearnArgs.getSequenceNo();
		updateMax(seqNo);
		if( LearnArgs.getSequenceNo() >= min)
		{
			statusMap.put(LearnArgs.getSequenceNo(), new Status(LearnArgs.getValue(),true));
		}
		peersDoneValue.put(LearnArgs.getMe(), LearnArgs.getMaxDoneSeq());
		cleanMemory();
		return new LearnReply(true);
		}
		finally{
			mutex.release();
		}
	}
	/*
	 * Forget instances uptil the min number
	 * to free up up heap on long running server instances.
	 */
	public void cleanMemory()
	{
		int length = peersDoneValue.size();
		int min = Integer.MAX_VALUE;
		if(length >0)
		{
			for(int i : peersDoneValue.keySet())
				if(peersDoneValue.get(i) < min)
					min = peersDoneValue.get(i);
		}
		min = min +1;
		
		
		Iterator<Integer> iter = acceptorStateMap.keySet().iterator();

		while (iter.hasNext()) {
		    int i = iter.next();

		    if (i < min)
		        iter.remove();
		}
		iter = statusMap.keySet().iterator();

		while (iter.hasNext()) {
		    int i = iter.next();

		    if (i < min)
		        iter.remove();
		}
	
	}
	//  Prepare method:
	//  if n > n_p
	//    n_p = n
	//    reply prepare_ok(n_a, v_a)
	//  else
	//    reply prepare_reject
	@Override
	public PrepareReply Prepare(PrepareArgs prepareArgs) throws RemoteException, InterruptedException{
		try
		{
		mutex.acquire();
		PrepareReply prepareReply = new PrepareReply(0,0,null, false);
		int seqNo = prepareArgs.getSequenceNo();
		int propNo = prepareArgs.getProposalNo();
		boolean exists =  acceptorStateMap.containsKey(seqNo);
		updateMax(seqNo);
		if (seqNo >= min) {
			if (exists){
				AcceptorState state = acceptorStateMap.get(seqNo);
				if(propNo > state.getN_p()){
					acceptorStateMap.put(seqNo, new AcceptorState(propNo, state.getN_a(), state.getValue()));
					prepareReply = new PrepareReply(state.getN_p(),state.getN_a(), state.getValue(), true); 
				} else {
					prepareReply = new PrepareReply(state.getN_p(),state.getN_a(), state.getValue(), false); 
				}
			} else {
				acceptorStateMap.put(seqNo, new AcceptorState(propNo, 0, null));   
				prepareReply = new PrepareReply(0,0, null, true);
			}
		}
		return prepareReply;
		}
		finally{
			mutex.release();
		}
	}
	// Cite :: http://academic.csuohio.edu/zhao_w/teaching/Old/EEC688-S11/lecture11.ppt
	// Phase 2.
	// (a) If the proposer receives a response to its prepare requests
	//	     (numbered n) from a majority of acceptors, then it sends an accept
	//	     request to each of those acceptors for a proposal numbered n with a
	//	     value v, where v is the value of the highest-numbered proposal among
	//	     the responses, or is any value if the responses reported no proposals.
	// (b) If an acceptor receives an accept request for a proposal numbered
	//	     n, it accepts the proposal unless it has already responded to a prepare
	//	     request having a number greater than n.
	// acceptor's accept(n, v) handler:
	//   if n >= n_p
	//	     n_p = n
	//	     n_a = n
	//	     v_a = v
	//	     reply accept_ok(n)
	//   else
	//	     reply accept_reject

	/* if(crashed) component
	 * Simulates a crashed experience.
	 * Is invoked by the accept method to check if the server has crashed, if so then 
	 * stall the server for CRASH_DURATION number of milliseconds.
	 * On the other hand, the RMISocketFactory has been set up to timeout after CRASH_DURATION - delta milliseconds,
	 * where delta > 0 , so the client to this paxos helper will treat the request sent as having crashed, and choose
	 * to continue operation by tolerating this crash. 
	 */
	@Override
	public AcceptReply Accept(AcceptArgs acceptArgs) throws RemoteException, InterruptedException
	{	
		AcceptReply acceptReply = null;
		if(crashed){
			Thread.sleep(CRASH_DURATION);
			return acceptReply;
		}
		else{
		try{
		mutex.acquire();
		acceptReply = null;
		int seqNo = acceptArgs.getSequenceNo();
		int proposalNo = acceptArgs.getProposalNo();
		String value = acceptArgs.getValue();
		updateMax(seqNo);
		if(seqNo >= min)
		{
			boolean exists = acceptorStateMap.containsKey(seqNo);
			if (exists ) 
			{
				AcceptorState state = acceptorStateMap.get(seqNo);
				if( proposalNo < state.getN_p())
					acceptReply = new AcceptReply(false);
			}
			if(acceptReply == null)
			{
				acceptorStateMap.put(seqNo, new AcceptorState(proposalNo, proposalNo, value));	
				acceptReply = new AcceptReply(true);
			}
		}
		return acceptReply;
		}
		finally{
			mutex.release();
		}
		
	}
	}

}

