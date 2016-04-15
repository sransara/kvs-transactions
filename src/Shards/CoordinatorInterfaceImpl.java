package Shards;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.rmi.Naming;
import java.rmi.RemoteException;
import java.rmi.server.RMISocketFactory;
import java.rmi.server.UnicastRemoteObject;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;

import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.FileAppender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;
import org.mapdb.*;

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
	private static volatile HashMap<UUID, Response> responseLog;
	private static final int numReplicas = 5;
	private Paxos paxosHelper;
	static final int hostPortColumn = 2;
	private static final int TIMEOUT = 1000;
	static volatile boolean crashed;
	volatile Semaphore mutex = new Semaphore(1);
	
	static final String JOIN = "JOIN";
	static final String MOVE = "MOVE";
	static final String QUERY = "QUERY";
	static final String LEAVE = "LEAVE";

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
		crashed = false;
		Paxos.crashed = false;
		currentSeqNo =0;
		responseLog= new HashMap<UUID, Response>();
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
		paxosHelper = new Paxos(peers,me);

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
			// TODO Auto-generated catch block
			log.info("System exited with error " +e.getMessage());
			System.exit(-1);
		}
		return hostPorts;
	}

	public void applyOperation(ShardOperation operation)
	{
		
	}


	public Response threePhaseCommit(ShardOperation operation)  throws Exception
	{
		Response toSendBack = new Response("", false);
		try{
			mutex.acquire();

			//check to see if a Status 
			if(responseLog.containsKey(operation.requestId))
			{
				log.info("request id already exists, returning stored result");
				return responseLog.get(operation.requestId);
			}
			int sequenceAssigned = DoPaxos(operation);
			// apply updates from currentSequence to sequenceAssigned by Paxos algorithm

			updateResponseLog(currentSeqNo, sequenceAssigned);

			toSendBack = responseLog.get(operation.requestId);

			paxosHelper.Done(sequenceAssigned);

			currentSeqNo = sequenceAssigned + 1;

			return toSendBack;  
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
			if(operation.requestId.compareTo(processed.requestId) == 0)
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
				if(!responseLog.containsKey((consensusOperation.requestId)))
				{
					log.info("Apply update at server " + (me()+1) + ":"  + consensusOperation.toString());
					applyOperation(consensusOperation);
				}
			}
		}

	}
	/*
	 * Pseudo-Random generator that either crashes the node or conducts a three phase commit 
	 * on the original request.
	 */
	public Response stallIfCrashedExecuteIfNot(ShardOperation operation) throws Exception
	{
		if(crashed)
			Thread.sleep(CRASH_DURATION);
		else{
			Random r = new Random();
			//crash rate 12.5% i.e 1/8
			int randInt = r.nextInt(7);
			if ( randInt == 5){
		 		log.info("FAIL/CRASH server " + (me+1) + ": CAUSED BY RANDOM FAILURE");
				crashed = true;
				Paxos.crashed = true;
				try {
					Thread.sleep((numReplicas+1)*CRASH_DURATION);
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				crashed = false;
				Paxos.crashed = false;
		 		log.info("RESUSCITATED ZOMBIE SERVER " + (me+1) + " : IS ALIVE AGAIN! ") ;
				return new Response(" ", false);
			}
			else
			{
				return threePhaseCommit(operation);
			}
		}
		return new Response(" ", false);
	}

	@Override
	public void KILL() throws Exception {
 		crashed = true;
 		Paxos.crashed = true;
 		log.info("FAIL/CRASH server " + (me+1) + ": CAUSED BY RMI CALL TO ShardMaster KILL");
 		try {
 			Thread.sleep((numReplicas+1)*CRASH_DURATION);
 		} catch (InterruptedException e) {
 			// TODO Auto-generated catch block
 			e.printStackTrace();
 		}
 		crashed = false;
 		Paxos.crashed = false;
 		log.info("RESUSCITATED ZOMBIE SERVER " + (me+1) + " : IS ALIVE AGAIN! ") ;
	    return;
	  
	}

	@Override
	public Response Join(JoinArgs joinArgs) throws Exception {
		ShardOperation shardOp = new ShardOperation(JOIN,joinArgs.groupId, joinArgs.servers,0, joinArgs.uuid);
		return stallIfCrashedExecuteIfNot(shardOp);	
	}
	@Override
	public Response Query(QueryArgs queryArgs) throws Exception {
		ShardOperation shardOp = new ShardOperation(QUERY, null,null, 0, queryArgs.uuid);
		return stallIfCrashedExecuteIfNot(shardOp);
	}

	@Override
	public Response Leave(LeaveArgs leaveArgs) throws Exception {
		ShardOperation shardOp = new ShardOperation(LEAVE,null,null, 0, leaveArgs.uuid);
		return stallIfCrashedExecuteIfNot(shardOp);
	}

	@Override
	public Response Move(MoveArgs moveArgs) throws Exception {
		ShardOperation shardOp = new ShardOperation(MOVE,null,null, 0, moveArgs.uuid);
		return stallIfCrashedExecuteIfNot(shardOp);
	}
}
