package Shards;

import java.rmi.Naming;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.util.concurrent.Semaphore;

import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.FileAppender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;

import Paxos.PaxosInterface;

public class Coordinator {
	
	final static Logger log = Logger.getLogger(Coordinator.class);
	final static String PATTERN = "%d [%p|%c|%C{1}] %m%n";
	private volatile Semaphore mutex ;
	
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
	
	public Coordinator(String hostname, int portNumber) throws RemoteException, Exception
	{
		// create the registry 
		CoordinatorInterfaceImpl shardMasterImpl;
		CoordinatorInterface shardMethods = (shardMasterImpl = new CoordinatorInterfaceImpl(hostname, portNumber));
		PaxosInterface paxosMethods = shardMasterImpl.getPaxosHelper();
		LocateRegistry.createRegistry(portNumber);
		//bind the method to this name so the client can search for it
		String bindMeShardMaster = "rmi://" + hostname + ":" + portNumber + "/Shards";
		String bindMePaxos = "rmi://" + hostname + ":" + portNumber + "/ShardPaxos";
		Naming.bind(bindMeShardMaster, shardMethods);
		Naming.bind(bindMePaxos, paxosMethods);
		log.info("Paxos RMIServer " + (shardMasterImpl.me()+1) + "  started successfully");
	}
	
	
	public static void main(String args[])
	{
		
		
	}
}
