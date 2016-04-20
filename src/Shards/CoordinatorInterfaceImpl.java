package Shards;
import java.io.BufferedReader;

import java.io.FileReader;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.rmi.RemoteException;
import java.rmi.server.RMISocketFactory;
import java.rmi.server.UnicastRemoteObject;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Semaphore;

import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.FileAppender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;

import Paxos.Paxos;
import Utility.UtilityClasses;
import Utility.UtilityClasses.*;


@SuppressWarnings("serial")
public class CoordinatorInterfaceImpl extends UnicastRemoteObject implements CoordinatorInterface{
	final static Logger log = Logger.getLogger(CoordinatorInterfaceImpl.class);
	final static String PATTERN = "%d [%p|%c|%C{1}] %m%n";
	private static String[][] hostPorts;
	private static int port;
	private static String hostname;
	private final long CRASH_DURATION = 1500;
	private static int me;
	private volatile int currentSeqNo;
	private static volatile HashMap<UUID, ShardReply> responseLog;
	private static final int numReplicas = 10;
	private Paxos paxosHelper;
	static final int hostPortColumn = 2;
	private static final int TIMEOUT = 1000;
	static volatile boolean crashed;
	volatile Semaphore mutex = new Semaphore(1);
	
	static final String JOIN = "JOIN";
	static final String LEAVE = "LEAVE";
	static final String POLL = "POLL";

	public Paxos getPaxosHelper()
	{
		return paxosHelper;
	}

	public static void configureRMI() throws IOException
	{
		RMISocketFactory.setSocketFactory( new RMISocketFactory()
		{
			public Socket createSocket( String host, int port )
					throws IOException
					{
				Socket socket = new Socket();
				socket.setSoTimeout(TIMEOUT );
				socket.setSoLinger( false, 0 );
				socket.connect( new InetSocketAddress( host, port ),TIMEOUT );
				return socket;
					}

			public ServerSocket createServerSocket( int port )
					throws IOException
					{
				return new ServerSocket( port );
					}
		} );
	}
	protected CoordinatorInterfaceImpl(String host, int portNumber) throws RemoteException,Exception {
		super();
		port = portNumber;
		hostname = host;
		initializeServer();
		configureRMI();
		currentSeqNo =0;
		responseLog= new HashMap<UUID, ShardReply>();
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
		fa.setFile("log/shardcoord.log");
		fa.setLayout(new PatternLayout("%d %-5p [%c{1}] %m%n"));
		fa.setThreshold(Level.ALL);
		fa.setAppend(true);
		fa.activateOptions();

		//add appender to any Logger (here is root)
		log.addAppender(fa);
		log.setAdditivity(false);
		//repeat with all other desired appenders
	}
	public int me()
	{
		return me;
	}
	/*
	 * Setup server when constructor is called
	 */
	protected void initializeServer() throws Exception
	{
		configureLogger();
		hostPorts= readConfigFile();
		List<HostPorts> peers = new ArrayList<HostPorts>();
		me = 0;
		for(int a=0; a< numReplicas; a++)
		{	
			HostPorts newHostPort = new HostPorts(hostPorts[a][0], Integer.parseInt(hostPorts[a][1]));
			if( hostPorts[a][0].equalsIgnoreCase(hostname) &&  Integer.parseInt(hostPorts[a][1]) == port)
				me =a;
			peers.add(newHostPort);
		}
		paxosHelper = new Paxos(peers,me, "/ShardCoordinatorPaxos", "log/shardcoord.log");

	}



	public static String[][] readConfigFile()
	{		
		String hostPorts [][] = new String[numReplicas][hostPortColumn];
		try {
			BufferedReader fileReader = new BufferedReader(new FileReader("../configs.txt"));			
			int c = 0;

			while(c++!=numReplicas)
			{
				hostPorts[c-1] = fileReader.readLine().split("\\s+");
				if(hostPorts[c-1][0].isEmpty() || !hostPorts[c-1][1].matches("[0-9]+") || hostPorts[c-1][1].isEmpty())
				{
					log.info("You have made incorrect entries for addresses in config file, please investigate.");
					System.exit(-1);
				}
			}
			fileReader.close();
		} catch (IOException e) {
			log.info("System exited with error " +e.getMessage());
			System.exit(-1);
		}
		return hostPorts;
	}

	public void applyOperation(ShardOperation operation)
	{
		ShardReply result = new InvalidReply();
		switch(operation.type.trim().toUpperCase()){
		case JOIN:
			result = joinOperation((JoinArgs)operation.shardArgs);
			responseLog.put(operation.shardArgs.getUUID(), result);
			break;
		case LEAVE:
			result = leaveOperation((LeaveArgs)operation.shardArgs);
			responseLog.put(operation.shardArgs.getUUID(), result);
			break;
		case POLL:
			result = pollOperation((PollArgs)operation.shardArgs);
			responseLog.put(operation.shardArgs.getUUID(), result);
			break;
		default:
			String response = "Invalid operation " + operation + " was received";
			log.error(response);
			break;
		}
	}


	public ShardReply commitOperation(ShardOperation operation)  throws Exception
	{
		ShardReply shardReply = new InvalidReply();
		try{
			mutex.acquire();

			//check to see if a Status 
			if(responseLog.containsKey(operation.shardArgs.getUUID()))
			{
				log.info("request id already exists, returning stored result");
				return responseLog.get(operation.shardArgs.getUUID());
			}
			int sequenceAssigned = DoPaxos(operation);
			// apply updates from currentSequence to sequenceAssigned by Paxos algorithm

			updateResponseLog(currentSeqNo, sequenceAssigned);

			shardReply = responseLog.get(operation.shardArgs.getUUID());

			paxosHelper.Done(sequenceAssigned);

			currentSeqNo = sequenceAssigned + 1;

			return shardReply;  
		}
		finally{
			mutex.release();
		}
	}
	/*
	 * Generate a sequence number for the given operation.
	 * Starts consensus on current sequence number and returns with minimum viable sequence number
	 * that hasn't been utilized by any other operation 
	 * during Paxos
	 */
	public int DoPaxos(ShardOperation operation) throws IOException, Exception
	{
		int sequence = currentSeqNo;
		for(;;)
		{
			paxosHelper.StartConsensus(sequence, UtilityClasses.encodeShardOperation(operation));

			long sleepFor = 60;
			ShardOperation processed = null;
			for(;;)
			{ Status status = paxosHelper.Status(sequence);

			if(status.done)
			{
				processed = UtilityClasses.decodeShardOperation(status.value);
				break;
			}
			else
			{
				Thread.sleep(sleepFor);
				if(sleepFor < CRASH_DURATION )
					sleepFor *= 2;
			}
			}
			if(operation.shardArgs.getUUID().compareTo(processed.shardArgs.getUUID()) == 0)
				break;
			else	
				sequence ++;
		}

		return sequence;
	}
	/*
	 * Applies missed updates
	 * We are currently at currentSeqNo
	 * But it seems like the Paxos returned a sequenceAssigned that greater than or equal to currentSeqNo
	 * so for the case that the assigned number for this operation is larger than currentSeqNo we
	 * apply the updates that have been missed by Paxos either due to network partition or delay in message delivery.
	 */
	public void updateResponseLog(int currentSeqNo, int sequenceAssigned) throws Exception
	{
		
		for(int i= currentSeqNo ; i <= sequenceAssigned; i ++)
		{
			Status status = paxosHelper.Status(i);
			if(status.done)
			{
				ShardOperation consensusOperation = UtilityClasses.decodeShardOperation(status.getValue());
				if(!responseLog.containsKey((consensusOperation.shardArgs.getUUID())))
				{
					log.info("Apply update at server " + (me()+1) + ":"  + consensusOperation.toString());
					applyOperation(consensusOperation);
				}
			}
		}

	}

	public JoinReply joinOperation(JoinArgs joinArgs)
	{
		log.info(" Called join Args");
		return new JoinReply();
		
	}
	
	public LeaveReply leaveOperation(LeaveArgs leaveArgs)
	{
		log.info(" Called leaveArgs");
		return new LeaveReply();
	}
	
	public PollReply pollOperation(PollArgs pollArgs)
	{
		log.info(" Called pollOperation");
		return new PollReply(null);
	}
	
	@Override
	public ShardReply Join(JoinArgs joinArgs) throws Exception {
		ShardOperation shardOp = new ShardOperation(JOIN,joinArgs);
		return commitOperation(shardOp);
	}

	@Override
	public ShardReply Leave(LeaveArgs leaveArgs) throws Exception {
		ShardOperation shardOp = new ShardOperation(LEAVE,leaveArgs);
		return commitOperation(shardOp);
	}

	@Override
	public ShardReply Poll(PollArgs pollArgs) throws Exception {
		ShardOperation shardOp = new ShardOperation(POLL, pollArgs);
		return commitOperation(shardOp);
	}

}
