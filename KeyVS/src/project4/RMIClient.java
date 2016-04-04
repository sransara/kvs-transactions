package project4;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Serializable;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.rmi.Naming;
import java.rmi.server.RMISocketFactory;
import java.util.UUID;



import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.FileAppender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;

import project4.UtilityClasses.Response;
import project4.UtilityClasses.Status;

public class RMIClient {
	final static Logger log = Logger.getLogger(RMIClient.class);
	final static String PATTERN = "%d [%p|%c|%C{1}] %m%n";
	final static int numReplicas = 5;
	final static int hostPortColumn = 2;
	private static final int TIMEOUT = 1000;
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
		fa.setFile("log/_rmi_client.log");
		fa.setLayout(new PatternLayout("%d %-5p [%c{1}] %m%n"));
		fa.setThreshold(Level.ALL);
		fa.setAppend(true);
		fa.activateOptions();

		//add appender to any Logger (here is root)
		log.addAppender(fa);
		log.setAdditivity(false);
		//repeat with all other desired appenders
	}
	public static void configureRMI() throws IOException
	{
		RMISocketFactory.setSocketFactory( new RMISocketFactory()
		{
			public Socket createSocket( String host, int port )
					throws IOException
					{
				Socket socket = new Socket();
				socket.setSoTimeout((numReplicas + 1)*TIMEOUT);
				socket.setSoLinger( false, 0 );
				socket.connect( new InetSocketAddress( host, port ), (numReplicas + 1)*TIMEOUT );
				return socket;
					}

			public ServerSocket createServerSocket( int port )
					throws IOException
					{
				return new ServerSocket( port );
					}
		} );
	}

	public static int getNumReplicas()
	{
		return numReplicas;
	}

	public static String[][] readConfigFile()
	{		
		String hostPorts [][] = new String[numReplicas][hostPortColumn];
		try {
			BufferedReader fileReader = new BufferedReader(new FileReader("../configs.txt"));			
			log.info("Loading configurations from configs.txt..");
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

	public static void main(String args[]) throws IOException
	{
		configureLogger();
		configureRMI();
		String clientId = "NA";
		// 4 arguments must be passed in.
		String hostPorts [][] = new String[numReplicas][hostPortColumn];
		String command = "";
		String key = "";
		String inkVal[];
		String values = "";		
		int serverNum =0;
		BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
		if(args.length == 0)
		{
			log.info("Give client an Identifier, any number,string etc");
			try {
				clientId = br.readLine();
				log.info("Enter, instruction key value");
				inkVal = br.readLine().split("\\s+");
				command = inkVal[0];
				key = inkVal[1];
				for(int a = 2; a <inkVal.length; a++)
					values +=" " + inkVal[a];
				hostPorts = readConfigFile();
				log.info("Which replica to invoke? #1-5");
				serverNum = Integer.parseInt(br.readLine());
				log.info("You selected: RMI Server " + hostPorts[serverNum-1][0] + ":"+ hostPorts[serverNum-1][1]);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				log.info("Please follow instructions closely on next run");
				log.info("Error with message, " + e.getMessage());
				System.exit(-1);
			}
		}
		else if(args.length < 2  )
		{
			log.fatal("Fatal error : instruction key value" );
			System.exit(-1);
		}
		else
		{
			hostPorts = readConfigFile();
			command = args[0];
			key = args[1];
			for(int a = 2; a <args.length; a++)
				values +=" " + args[a];
			serverNum =  Integer.parseInt(System.getProperty("serverChoice"));
		}

		try{

			Response response = new Response("",false);
			UUID uuid = UUID.randomUUID();
			log.info("This request has id :" + uuid);
			if(System.getProperty("clientId") != null)
				clientId = System.getProperty("clientId");
			while(!response.done){
				String hostname = hostPorts[serverNum-1][0];
				String port = hostPorts[serverNum-1][1];
				// call the corresponding methods
				key = key.trim();
				values = values.trim();
				try{
					// locate the remote object initialize the proxy using the binder
					RMIServerInterface hostImpl = (RMIServerInterface) Naming.lookup("rmi://" + hostname + ":" + port + "/Calls" );
					switch(command.trim().toUpperCase()){
					case "GET":
						response = hostImpl.GET(clientId,key, uuid);
						log.info("RESULT FROM " + hostname + ":" + port + "!!!: " + "Client "+ clientId + ":" + response.getValue() );
						break;
					case "PUT":
						response = hostImpl.PUT(clientId,key,values, uuid);
						log.info("RESULT FROM " + hostname + ":" + port + "!!!: " + "Client "+ clientId + ":" +response.getValue());
						break;
					case "DELETE":
						response = hostImpl.DELETE(clientId,key,uuid);
						log.info("RESULT FROM " + hostname + ":" + port + "!!!: " +"Client "+ clientId + ":" + response.getValue());
						break;
					case "KILL":
						log.info("RESULT FROM " + hostname + ":" + port + "!!!: " + "Client "+ clientId + ":" + " killing " + hostname + " : " + port);
						response = new Response("",true);
						hostImpl.KILL();
						break;
					default:
						String errorResponse = "Client "+clientId + ":" +  "Invalid command " + command + " was received";
						log.error(errorResponse);
						break;
					}
				}
				catch(Exception e)
				{
					log.info("Contact server " + serverNum + " at hostname:port " + hostname + " : " + port + " FAILED. Trying others..");

				}
				serverNum = (serverNum + 1) % getNumReplicas();
			}
		}
		catch(Exception e)
		{
			log.error("Error occured while connecting to RMI server with error, " + e.getMessage());
		}
	}}

