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
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.UUID;
import java.util.concurrent.Semaphore;

import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.FileAppender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;

import Paxos.Paxos;
import Utility.UtilityClasses;
import Utility.UtilityClasses.*;


@SuppressWarnings("serial")
public class CoordinatorInterfaceImpl extends UnicastRemoteObject implements CoordinatorInterface{
	final static Logger log = Logger.getLogger(CoordinatorInterfaceImpl.class);
	final static String PATTERN = "%d [%p|%c|%C{1}] %m%n";
	
	final static Integer NUM_SHARDS = 20;
	private static String[][] hostPorts;
	private static int port;
	private static String hostname;
	private final long CRASH_DURATION = 1500;
	private static int me;
	private volatile int maximumConfigurationNo;
	private volatile List<Configuration> configurations;
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
		maximumConfigurationNo = 0;
		configurations = new ArrayList<Configuration>();
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
		paxosHelper = new Paxos(peers,me,"/ShardCoordinatorPaxos", "log/shardcoord.log", true);

	}



	public static String[][] readConfigFile()
	{		
		String hostPorts [][] = new String[numReplicas][hostPortColumn];
		try {
			BufferedReader fileReader = new BufferedReader(new FileReader("configs.txt"));			
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
		JoinReply joinReply = new JoinReply();
		log.info(" Join operation invoked");
		Configuration newConfiguration = null;
		
		// Check if servers provided are on some other replication group already
		
		
		
		if(maximumConfigurationNo == 0)
		{
			HashMap<UUID, List<HostPorts>> replicaGroupMap = new HashMap<UUID, List<HostPorts>>();
			replicaGroupMap.put(joinArgs.groupId, joinArgs.servers);
			HashMap<Integer, UUID> shardToGroupId = new HashMap<Integer, UUID>();
			for(Integer a = 0; a < NUM_SHARDS; a++ )
				shardToGroupId.put(a, joinArgs.groupId);
			newConfiguration = new Configuration(maximumConfigurationNo,shardToGroupId, replicaGroupMap);
		}
		else
		{
			try{
			Configuration oldConfiguration = configurations.get(maximumConfigurationNo-1);
			HashMap<UUID, List<HostPorts>> replicaGroupMap =new HashMap<UUID, List<HostPorts>>(oldConfiguration.replicaGroupMap);
			
			// Check to see if servers provided are on some other replication group already
			for(HostPorts hostPort: joinArgs.servers)
			{
				for(UUID key : replicaGroupMap.keySet())
				{
					for(HostPorts checkHostPort: replicaGroupMap.get(key))
					{
						if(hostPort.getHostName().equalsIgnoreCase(checkHostPort.getHostName()) && hostPort.getPort() == checkHostPort.getPort())
						{
							joinReply.message = "Error: Can't Join. The server " + checkHostPort.getHostName() + " already exists in replica group " + key;
							return joinReply;
						}
					}
				}
			}
			
			
			HashMap<Integer, UUID> shardToGroupId = new HashMap<Integer, UUID>(oldConfiguration.shardToGroupIdMap);
			log.info(" Shard to group id map is " + shardToGroupId);
			if(shardToGroupId.isEmpty())
			{
				log.info(" Shard to group id map was empty initializing" + shardToGroupId);
				for(Integer a = 0; a < NUM_SHARDS; a++ )
					shardToGroupId.put(a, joinArgs.groupId);
			}
			replicaGroupMap.put(joinArgs.groupId, joinArgs.servers);
			newConfiguration = new Configuration(maximumConfigurationNo,shardToGroupId,replicaGroupMap);
			reconfigure(newConfiguration);
			}
			catch(Exception e)
			{
				e.printStackTrace();
			}
			
		}
		configurations.add(newConfiguration);
		maximumConfigurationNo = maximumConfigurationNo + 1;
		
		joinReply.message = "Joined with groupId " + joinArgs.groupId;
		return joinReply;
	}
	
	public LeaveReply leaveOperation(LeaveArgs leaveArgs)
	{
		
		log.info(" Called leaveArgs");
		LeaveReply leaveReply = new LeaveReply();
		try{
		if(maximumConfigurationNo == 0)
		{
			log.info("Error: No prior configurations. Request to leave with groupId " + leaveArgs.groupId + " failed.");
			leaveReply.message = "Error: No prior configurations. Request to leave with groupId " + leaveArgs.groupId + " failed.";
			return leaveReply;
		}
		Configuration oldConfiguration = configurations.get(maximumConfigurationNo-1);
		HashMap<Integer, UUID> shardToGroupId = new HashMap<Integer, UUID>(oldConfiguration.shardToGroupIdMap);
		HashMap<UUID, List<HostPorts>> replicaGroupMap  = new HashMap<UUID, List<HostPorts>>(oldConfiguration.replicaGroupMap);
		if(replicaGroupMap.containsKey(leaveArgs.groupId))
		{
			replicaGroupMap.remove(leaveArgs.groupId);
			log.info(" Group id " + leaveArgs.groupId + " removed.");		
			
		}
		else
		{
			log.info("Error: Request to leave failed. Group Id " + leaveArgs.groupId + " doesn't exist in latest configuration.");
			leaveReply.message = "Error: Request to leave failed. Group Id " + leaveArgs.groupId + " doesn't exist in latest configuration.";
			return leaveReply;
		}
		Configuration newConfiguration = new Configuration(maximumConfigurationNo,shardToGroupId, replicaGroupMap);
		configurations.add(newConfiguration);
		reconfigure(newConfiguration);
		maximumConfigurationNo = maximumConfigurationNo + 1;
		log.info(" Group id " + leaveArgs.groupId + " has left.");
		leaveReply.message = " Group id " + leaveArgs.groupId + " has left.";
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
		return leaveReply;
	}
	
	public PollReply pollOperation(PollArgs pollArgs)
	{
		log.info(" Called pollOperation");
		if(configurations.isEmpty() || pollArgs.configurationSequenceNumber >= maximumConfigurationNo)
			return new PollReply(new Configuration(0, new HashMap<Integer, UUID> () ,new HashMap<UUID, List<HostPorts>> ()));
		else if(pollArgs.configurationSequenceNumber < 0)
			return new PollReply(configurations.get(maximumConfigurationNo-1));
		else
			return new PollReply(configurations.get(pollArgs.configurationSequenceNumber));
	}
	
	private void reconfigure(Configuration configuration)
	{
		HashMap<UUID, List<HostPorts>> replicaGroupMap = configuration.replicaGroupMap;
		Map<Integer, UUID> shardToGroupIdMap = configuration.shardToGroupIdMap;
		int noOfGroups = replicaGroupMap.keySet().size();
		HashMap<UUID, List<Integer>> groupToShards = new HashMap<UUID, List<Integer>>();
		Multimap<UUID, Integer> multiMap = HashMultimap.create();
		for (Entry<Integer, UUID> entry : shardToGroupIdMap.entrySet()) {
		  multiMap.put(entry.getValue(), entry.getKey());
		}
		for (Entry<UUID, Collection<Integer>> entry : multiMap.asMap().entrySet()) {
			List<Integer> shards = new ArrayList<Integer>();
			shards.addAll(entry.getValue());
			if(replicaGroupMap.containsKey(entry.getKey()))
				groupToShards.put(entry.getKey(), shards);

		}
		for (Entry<UUID, Collection<Integer>> entry : multiMap.asMap().entrySet()) {
			List<Integer> shards = new ArrayList<Integer>();
			shards.addAll(entry.getValue());
			if(!replicaGroupMap.containsKey(entry.getKey()))
			{
				if(noOfGroups>0){
					UUID firstKey = groupToShards.keySet().iterator().next();
					groupToShards.get(firstKey).addAll(shards);
				}
			}
		}
		for(UUID group: replicaGroupMap.keySet())
			if (!groupToShards.containsKey(group))
				groupToShards.put(group, new ArrayList<Integer>());
		
		if(noOfGroups >0){
		int min = NUM_SHARDS/noOfGroups;
		int max = NUM_SHARDS/noOfGroups;
		if(NUM_SHARDS % noOfGroups != 0)
			max = max + 1;
		UUID groupA_WithMaxShards = getmaxshard(groupToShards);
		UUID groupB_WithMinShards = getminshard(groupToShards);
		int sizeGroupA = groupToShards.get(groupA_WithMaxShards).size();
		int sizeGroupB = groupToShards.get(groupB_WithMinShards).size();		
		while(sizeGroupA > max || sizeGroupB < min)
		{
			int diff1 = (max- sizeGroupB);
			int diff2 = (sizeGroupA - min);
			
			int diff = (diff1<diff2)? diff1 : diff2;
			List<Integer> maxGroupShards = groupToShards.get(groupA_WithMaxShards);
			List<Integer> minGroupShards = groupToShards.get(groupB_WithMinShards);
			
			for (int a = 0; a < diff; a ++)
			{	
				Integer removed = maxGroupShards.remove(0);
				minGroupShards.add(removed);	
			}
			groupA_WithMaxShards = getmaxshard(groupToShards);
			groupB_WithMinShards = getminshard(groupToShards);
			sizeGroupA = groupToShards.get(groupA_WithMaxShards).size();
			sizeGroupB = groupToShards.get(groupB_WithMinShards).size();
		}
		
		for (Entry<UUID, List<Integer>> entry : groupToShards.entrySet())
			  for(Integer shard: entry.getValue())
				  shardToGroupIdMap.put(shard, entry.getKey());
		}
		else
		{
			shardToGroupIdMap.clear();
		}
	}
	
	
	private UUID getminshard(HashMap<UUID, List<Integer>> groupToShards)
	{
		int min = NUM_SHARDS+1;
		UUID minToReturn = null;
		for( UUID group : groupToShards.keySet())
		{	
			if(groupToShards.get(group).size() < min)
			{
				min = groupToShards.get(group).size();
				minToReturn = group;
			}
		}
		return minToReturn;
	}
	
	
	private UUID getmaxshard(HashMap<UUID, List<Integer>> groupToShards)
	{
		int max = 0;
		UUID maxToReturn = null;
		for( UUID group : groupToShards.keySet())
			if(groupToShards.get(group).size() > max)
			{
				max = groupToShards.get(group).size();
				maxToReturn = group;
			}
		return maxToReturn;
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
