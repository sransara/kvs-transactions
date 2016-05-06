package Transactions;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.rmi.RemoteException;
import java.rmi.server.RMIClientSocketFactory;
import java.rmi.server.RMIServerSocketFactory;
import java.rmi.server.RMISocketFactory;
import java.rmi.server.UnicastRemoteObject;
import java.util.UUID;
import java.util.concurrent.Semaphore;

import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.FileAppender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;

import Db.DbServerInterface;
import Paxos.Paxos;
import Utility.UtilityClasses.DbOperation;
import Utility.UtilityClasses.Response;
import Utility.UtilityClasses.TransactionContext;

public class AcyclicTransaction extends UnicastRemoteObject implements DbServerInterface {
	
    private static int port;
    private static String hostname;
    private Semaphore mutex = new Semaphore(1);
	
    /**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	protected AcyclicTransaction(int port, RMIClientSocketFactory csf,
			RMIServerSocketFactory ssf) throws RemoteException {
		super(port, csf, ssf);
	}

	public AcyclicTransaction(String hostname, Integer portNumber) throws IOException {
		
		 initializeServer();
		 this.port = portNumber;
		 this.hostname = hostname;
		 
	     
		
		
		// TODO Auto-generated constructor stub
	}
	
	
	public static void initializeServer() throws IOException
	{
		configureLogger();
		configureRMI();
		
	
		
	}
	
	
	private Paxos paxosHelper;
	final static Logger log = Logger.getLogger(TwoPhaseCommit.class);
    final static String PATTERN = "%d [%p|%c|%C{1}] %m%n";
    private static final int TIMEOUT = 1000;
	private static void configureRMI() throws IOException {
	        RMISocketFactory.setSocketFactory(new RMISocketFactory() {
	            public Socket createSocket(String host, int port)
	                    throws IOException {
	                Socket socket = new Socket();
	                socket.setSoTimeout(TIMEOUT);
	                socket.setSoLinger(false, 0);
	                socket.connect(new InetSocketAddress(host, port), TIMEOUT);
	                return socket;
	            }

	            public ServerSocket createServerSocket(int port)
	                    throws IOException {
	                return new ServerSocket(port);
	            }
	        });
	    }

	 
	    static void configureLogger() {
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
	        fa.setFile("log/acyclic.log");
	        fa.setLayout(new PatternLayout("%d %-5p [%c{1}] %m%n"));
	        fa.setThreshold(Level.ALL);
	        fa.setAppend(true);
	        fa.activateOptions();

	        //add appender to any Logger (here is root)
	        log.addAppender(fa);
	        log.setAdditivity(false);
	        //repeat with all other desired appenders
	    }
	 
	    public Paxos getPaxosHelper() {
	        return paxosHelper;
	    }

	 
	    
	    @Override
	    public Response PUT(String clientId, String key, String value, UUID requestId) throws Exception {
	        //	log.info("Server at " + hostname + ":" + port + " "+ "received [PUT " + key +"|"+value.trim() + "] from client " + clientId);
	        DbOperation putOp = new DbOperation("PUT", key, value, clientId, requestId);
	        return  new Response();
	    }

	    @Override
	    public Response GET(String clientId, String key, UUID requestId) throws Exception {
	        //log.info("Server at " + hostname + ":" + port + " "+ "received [GET " + key + "] from client " + clientId + " has crashed " + crashed);
	        DbOperation getOp = new DbOperation("GET", key, null, clientId, requestId);
	        return new Response();
	    }

	    @Override
	    public Response DELETE(String clientId, String key, UUID requestId) throws Exception {
	        //log.info("Server at " + hostname + ":" + port + " "+ "received [DELETE " + key + "] from client " + clientId);
	        DbOperation deleteOp = new DbOperation("DELETE", key, null, clientId, requestId);
	        return new Response();
	    }

	    @Override
	    public Response TRY_COMMIT(String clientId, TransactionContext txContext, UUID requestId) throws Exception {
	        DbOperation tryCommitOp = new DbOperation("TRY_COMMIT", txContext, clientId, requestId);
	        return  new Response();
	    }

	    @Override
	    public Response DECIDE_COMMIT(String clientId, TransactionContext txContext, UUID requestId) throws Exception {
	        DbOperation decideCommitOp = new DbOperation("DECIDE_COMMIT", txContext, clientId, requestId);
	        return  new Response();
	    }


		@Override
		public void KILL() throws Exception {
			// TODO Auto-generated method stub
			
		}

}
