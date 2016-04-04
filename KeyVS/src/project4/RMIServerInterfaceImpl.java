package project4;
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

import project4.UtilityClasses.Response;
import project4.UtilityClasses.*;

import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.FileAppender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;
import org.mapdb.*;


@SuppressWarnings("serial")
public class RMIServerInterfaceImpl extends UnicastRemoteObject implements RMIServerInterface{
	final static Logger log = Logger.getLogger(RMIServerInterfaceImpl.class);
	final static String PATTERN = "%d [%p|%c|%C{1}] %m%n";
	private static PersistentHash hash ;
	private static String[][] hostPorts;
	private static int port;
	private static String hostname;
	private static final String ACK = "ACK";
	private static final long CRASH_DURATION = 1500;
	private static int me;
	private static volatile int currentSeqNo;
	private static volatile HashMap<UUID, Response> responseLog;
	private static final int numReplicas = 5;
	private Paxos paxosHelper;
	final static int hostPortColumn = 2;
	private static final int TIMEOUT = 1000;
	public static volatile boolean crashed;
	public Semaphore mutex = new Semaphore(1);

	/*
	 * Flavor of hash that persists to disk
	 */
	private static class PersistentHash{

		private static ConcurrentNavigableMap<String,String> treeMap;
		private static DB db;
		public PersistentHash()
		{
			initializeDB();
		}
		public static void initializeDB()
		{
			db = DBMaker.fileDB(new File("testdb"))
					.closeOnJvmShutdown()
					.make();
			treeMap = db.treeMap("map");
		}
		public boolean containsKey(String key)
		{
			return treeMap.containsKey(key);
		}
		public String get(String key)
		{
			return treeMap.get(key);
		}
		public void remove(String key)
		{
			treeMap.remove(key);
			db.commit();
		}
		public void put(String key,String value)
		{
			treeMap.put(key, value);
			db.commit();
		}
	}

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
	protected RMIServerInterfaceImpl(String host, int portNumber) throws RemoteException,Exception {
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
	protected  void initializeServer() throws Exception
	{
		configureLogger();
		hash = new PersistentHash();
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
	/*
	 * (non-Javadoc)
	 * @see project3.RMIServerInterface#PUT(boolean, java.lang.String, java.lang.String, java.lang.String)
	 * Two phase commit for PUT operation. Only if go is true. we would commit to disk.
	 */
	@Override
	public Response PUT(String clientId, String key,String value, UUID requestId) throws Exception
	{	
		//	log.info("Server at " + hostname + ":" + port + " "+ "received [PUT " + key +"|"+value.trim() + "] from client " + clientId);
		Operation putOp = new Operation("PUT", key, value, clientId, requestId);
		return stallIfCrashedExecuteIfNot(putOp);
	}

	@Override
	public Response GET(String clientId, String key, UUID requestId) throws Exception
	{
		//log.info("Server at " + hostname + ":" + port + " "+ "received [GET " + key + "] from client " + clientId + " has crashed " + crashed);
		Operation getOp = new Operation("GET", key, null, clientId, requestId);
		return stallIfCrashedExecuteIfNot(getOp);
	}

	@Override
	public Response DELETE( String clientId, String key, UUID requestId) throws Exception
	{
		//log.info("Server at " + hostname + ":" + port + " "+ "received [DELETE " + key + "] from client " + clientId);
		Operation deleteOp = new Operation("DELETE", key, null, clientId, requestId);
		return stallIfCrashedExecuteIfNot(deleteOp);
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

	/*
	 * Below are helper methods for the get, delete and put operations
	 * that directly update the db.
	 */

	public String getOperation(String key)
	{
		String response;
		if(hash.containsKey(key))
			response = hash.get(key);
		else
		{
			response = "No key "+ key + " matches db ";
		}
		return response;
	}

	public String putOperation(String key, String value)
	{
		String response = "";
		// this would overwrite the values of the key
		hash.put(key,value);
		response = ACK;
		return response;
	}

	public String deleteOperation(String key)
	{
		String response = "";
		if(hash.containsKey(key))
		{
			hash.remove(key);
			response = ACK;
		}
		else
		{
			response = "No such key - " + key +  " exists";
		}
		return response;

	}
	/*
	 * Generic method that takes an operation argument and applies it to the 
	 * db directly.
	 */
	public void applyOperation(Operation operation)
	{
		String result ="";
		switch(operation.type.trim().toUpperCase()){
		case "GET":
			result = getOperation(operation.key);
			responseLog.put(operation.requestId, new Response(result,true));
			break;
		case "PUT":
			result = putOperation(operation.key,operation.value);
			responseLog.put(operation.requestId, new Response(result,true));
			break;
		case "DELETE":
			result = deleteOperation(operation.key);
			responseLog.put(operation.requestId, new Response(result,true));
			break;
		default:
			String response = "Client "+ operation.from + ":" +  "Invalid command " + operation.key + " was received";
			log.error(response);
			break;
		}
	}


	public Response threePhaseCommit(Operation operation)  throws Exception
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
	public int DoPaxos(Operation operation) throws IOException, Exception
	{
		int sequence = currentSeqNo;
		for(;;)
		{
			paxosHelper.StartConsensus(sequence, UtilityClasses.encodeOperation(operation));

			long sleepFor = 60;
			Operation processed = null;
			for(;;)
			{ Status status = paxosHelper.Status(sequence);

			if(status.done)
			{
				processed = UtilityClasses.decodeOperation(status.value);
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
				Operation consensusOperation = UtilityClasses.decodeOperation(status.getValue());
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
	public Response stallIfCrashedExecuteIfNot(Operation operation) throws Exception
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
 		log.info("FAIL/CRASH server " + (me+1) + ": CAUSED BY RMI CALL TO KILL");
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
}
