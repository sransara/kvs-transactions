package Client;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.rmi.Naming;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.FileAppender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;

import Shards.CoordinatorInterface;
import Utility.UtilityClasses.*;


public class CoordinatorClient {
	final static Logger log = Logger.getLogger(CoordinatorClient.class);
	final static int hostPortColumn = 2;
	final static String JOIN = "JOIN";
	final static String LEAVE = "LEAVE";
	final static String POLL = "POLL";
	public static final int totalServers = 97;
	final static String PATTERN = "%d [%p|%c|%C{1}] %m%n";
	private static String[][] hostPorts;
	public static void main(String args[])
	{
		configureLogger();
		hostPorts = readConfigFile();
		if(args.length > 0)
		{
			if(args[0].equalsIgnoreCase(JOIN))
			{
				if(args.length >=6){
				UUID groupId = UUID.randomUUID();
				UUID uuid = UUID.randomUUID();
				List<HostPorts> servers = new ArrayList<HostPorts>();
				for(int a = 1; a < args.length; a++)
				{
					try{
						int serverIndex = Integer.parseInt(args[a]);
						if(serverIndex <totalServers && serverIndex >=10)
						{
						log.error("Fatal error:  You have entered an invalid server index. Must be [10-96]");
						System.exit(-1);
						
						}
						else
						{
						HostPorts hostPort = new HostPorts(hostPorts[serverIndex][0], Integer.parseInt(hostPorts[serverIndex][1]));
						servers.add(hostPort);
						}
					}
					catch(Exception e)
					{
						log.error("Fatal error:  You have entered an invalid server index. Must be [10-96]");
						System.exit(-1);
						e.printStackTrace();
					}
				}
				
				if(servers.size() <5)
				{
					log.error("Fatal error:  There must be atleast 5 servers in a replication group.");
				}
				
				JoinArgs joinArgs = new JoinArgs(groupId, servers, uuid);
				JoinReply joinReply;
				for(int i = 0; i < 9; i++){
					
					try {
						CoordinatorInterface hostImpl = (CoordinatorInterface) Naming.lookup("rmi://" + hostPorts[0][0] + ":" + hostPorts[0][1] + "/ShardCoordinator");
						joinReply = (JoinReply) hostImpl.Join(joinArgs);
						log.info(joinReply.message);
						break;
					} catch (Exception e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
					}
				}
			}
			else if (args[0].equalsIgnoreCase(LEAVE))
			{
			
			}
			else
			{
			
			}
		}
	}
	
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
		fa.setFile("log/coordClient.log");
		fa.setLayout(new PatternLayout("%d %-5p [%c{1}] %m%n"));
		fa.setThreshold(Level.ALL);
		fa.setAppend(true);
		fa.activateOptions();

		//add appender to any Logger (here is root)
		log.addAppender(fa);
		log.setAdditivity(false);
		//repeat with all other desired appenders
	}
	
	
	public static String[][] readConfigFile()
	{		
		String hostPorts [][] = new String[totalServers][hostPortColumn];
		try {
			BufferedReader fileReader = new BufferedReader(new FileReader("configs.txt"));			
			int c = 0;

			while(c++!=totalServers)
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
}
