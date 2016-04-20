package Shards;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.rmi.Naming;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;

import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.FileAppender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;

import Db.DbServer;
import Paxos.PaxosInterface;

public class Coordinator {
	
	final static Logger log = Logger.getLogger(Coordinator.class);
	final static String PATTERN = "%d [%p|%c|%C{1}] %m%n";
	
	static void configureLogger()
	{
		ConsoleAppender console = new ConsoleAppender(); //create appender
		//configure the appender
		console.setLayout(new PatternLayout(PATTERN)); 
		console.setThreshold(Level.ALL);
		console.activateOptions();
		//add appender to any Logger (here is root)
		log.addAppender(console);

		// This is for the tcp_client log file
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
	
	public Coordinator(String hostname, int portNumber) throws RemoteException, Exception
	{
		// create the registry 
		CoordinatorInterfaceImpl shardMasterImpl;
		CoordinatorInterface shardMethods = (shardMasterImpl = new CoordinatorInterfaceImpl(hostname, portNumber));
		PaxosInterface paxosMethods = shardMasterImpl.getPaxosHelper();
		LocateRegistry.createRegistry(portNumber);
		//bind the method to this name so the client can search for it
		String bindMeShardMaster = "rmi://" + hostname + ":" + portNumber + "/ShardCoordinator";
		String bindMePaxos = "rmi://" + hostname + ":" + portNumber + "/ShardCoordinatorPaxos";
		Naming.bind(bindMeShardMaster, shardMethods);
		Naming.bind(bindMePaxos, paxosMethods);
		log.info("Coordinator " + (shardMasterImpl.me()+1) + " and its Paxos assistant started successfully");
	}
	
	public static void main(String args[])
	{
		configureLogger();
		if(args.length != 2 )
		{
			log.info("Enter hostname");
			BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
			args = new String[2];
			try{
				log.info("Enter hostname");
				args[0] = br.readLine();
				log.info("Enter port");
				args[1] = br.readLine();
				}
			catch(Exception e)
			{
				log.fatal("Fatal error : Usage - host port#" );
				System.exit(-1);
			}
		}
		Integer portNumber =0;
		try{
			portNumber = Integer.parseInt(args[1]);
		}
		catch(Exception e)
		{
			log.fatal("Fatal error : Usage - hostname port#, error caused due to " + e.getMessage());
			System.exit(-1);
		}
		try{
			new Coordinator(args[0],portNumber);
		}
		catch(Exception e)
		{
			log.error("RMI server binding failed with " + e.getMessage());
		}
	}

}
