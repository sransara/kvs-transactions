package Transactions;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.rmi.Naming;
import java.rmi.RemoteException;
import java.rmi.server.RMISocketFactory;
import java.rmi.server.UnicastRemoteObject;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;

import Db.DbServerInterface;
import Shards.CoordinatorInterface;
import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.FileAppender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;

import Paxos.Paxos;
import Utility.UtilityClasses;
import Utility.UtilityClasses.*;
import org.apache.log4j.lf5.util.StreamUtils;


@SuppressWarnings("serial")
public class TwoPhaseCommit extends UnicastRemoteObject implements DbServerInterface {
    final static Logger log = Logger.getLogger(TwoPhaseCommit.class);
    final static String PATTERN = "%d [%p|%c|%C{1}] %m%n";
    final static int COORDINATOR_NUM_REPLICAS = 10;

    private static ConcurrentHashMap<String, Object> hash;
    private static HashMap<String, HashMap<UUID, Character>> rowLocks;

    private static int port;
    private static String hostname;
    public static int me;

    private static final String ACK = "ACK";
    private static final long CRASH_DURATION = 1500;
    private static volatile int currentSeqNo;
    private static volatile HashMap<UUID, Response> responseLog;

    private static UtilityClasses.Configuration configuration;
    private static UUID myReplicaGroupId;

    private static final int numReplicas = 60;
    private Paxos paxosHelper;
    final static int hostPortColumn = 2;
    private static final int TIMEOUT = 1000;
    private static volatile boolean crashed;
    private Semaphore mutex = new Semaphore(1);

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

    public TwoPhaseCommit(String host, int portNumber) throws RemoteException, Exception {
        super();
        port = portNumber;
        hostname = host;
        initializeServer();
        configureRMI();
        crashed = false;
        Paxos.crashed = false;
        currentSeqNo = 0;
        responseLog = new HashMap<UUID, Response>();
    }

    public Paxos getPaxosHelper() {
        return paxosHelper;
    }

    /* Configure the log4j appenders
     */
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
        fa.setFile("log/2pc.log");
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
     * Setup server when constructor is called
     */
    protected void initializeServer() throws Exception {
        configureLogger();
        hash = new ConcurrentHashMap<>();
        rowLocks = new HashMap<>();

        String coordinators[][] = readCoordinatorConfig();
        configuration = getShardConfig(coordinators);

        HostPorts hostPortMe = new HostPorts(hostname, port);

        List<HostPorts> peers = new ArrayList<HostPorts>();

        me = -1;
        for (Map.Entry<UUID, List<HostPorts>> entry : configuration.replicaGroupMap.entrySet()) {
            List<HostPorts> p = entry.getValue();
            int i = p.indexOf(hostPortMe);
            if (i > -1) {
                me = i;
                peers.addAll(p);
                myReplicaGroupId = entry.getKey();
                break;
            }
        }

        if (me == -1) {
            log.error("Coordinator doesn't know about me: " + hostname);
        } else {
            paxosHelper = new Paxos(peers, me, "/Paxos", "log/dbserver.log", false);
        }
    }

    /*
     * (non-Javadoc)
     * @see project3.RMIServerInterface#PUT(boolean, java.lang.String, java.lang.String, java.lang.String)
     * Two phase commit for PUT operation. Only if go is true. we would commit to disk.
     */
    @Override
    public Response PUT(String clientId, String key, String value, UUID requestId) throws Exception {
        //	log.info("Server at " + hostname + ":" + port + " "+ "received [PUT " + key +"|"+value.trim() + "] from client " + clientId);
        DbOperation putOp = new DbOperation("PUT", key, value, clientId, requestId);
        return stallIfCrashedExecuteIfNot(putOp);
    }

    @Override
    public Response GET(String clientId, String key, UUID requestId) throws Exception {
        //log.info("Server at " + hostname + ":" + port + " "+ "received [GET " + key + "] from client " + clientId + " has crashed " + crashed);
        DbOperation getOp = new DbOperation("GET", key, null, clientId, requestId);
        return stallIfCrashedExecuteIfNot(getOp);
    }

    @Override
    public Response DELETE(String clientId, String key, UUID requestId) throws Exception {
        //log.info("Server at " + hostname + ":" + port + " "+ "received [DELETE " + key + "] from client " + clientId);
        DbOperation deleteOp = new DbOperation("DELETE", key, null, clientId, requestId);
        return stallIfCrashedExecuteIfNot(deleteOp);
    }

    @Override
    public Response TRY_COMMIT(String clientId, TransactionContext txContext, UUID requestId) throws Exception {
        DbOperation tryCommitOp = new DbOperation("TRY_COMMIT", txContext, clientId, requestId);
        return stallIfCrashedExecuteIfNot(tryCommitOp);
    }

    @Override
    public Response DECIDE_COMMIT(String clientId, TransactionContext txContext, UUID requestId) throws Exception {
        DbOperation decideCommitOp = new DbOperation("DECIDE_COMMIT", txContext, clientId, requestId);
        return stallIfCrashedExecuteIfNot(decideCommitOp);
    }

	/*
     * Below are helper methods for the get, delete and put operations
	 * that directly update the db.
	 */

    public String getOperation(String key) {
        String response;
        if (hash.containsKey(key))
            response = hash.get(key).toString();
        else {
            response = "No key " + key + " matches db ";
        }
        return response;
    }

    public String putOperation(String key, String value) {
        String response = "";
        // this would overwrite the values of the key
        hash.put(key, value);
        response = ACK;
        return response;
    }

    public String deleteOperation(String key) {
        String response = "";
        if (hash.containsKey(key)) {
            hash.remove(key);
            response = ACK;
        } else {
            response = "No such key - " + key + " exists";
        }
        return response;

    }

    public String tryCommitOperation(TransactionContext txContext) {
        UUID requesterId = txContext.requesterId;
        HashMap<UUID, Character> lockMap = null;

        for (String key : txContext.readSet.keySet()) {
            if (rowLocks.containsKey(key)) {
                lockMap = rowLocks.get(key);
            } else {
                rowLocks.put(key, new HashMap<>());
                lockMap = rowLocks.get(key);
            }

            for (UUID ukey : lockMap.keySet()) {
                if (ukey.equals(requesterId)) {
                    continue;
                }

                // some concurrent transaction has already locked this row
                if (lockMap.get(ukey).equals('W')) {
                    log.info("abort");
                    return "ABORT";
                }

                // the value seen by the client is wrong
                if (hash.contains(key)) {
                    if (!txContext.readSet.get(key).equals(hash.get(key))) {
                        log.info("abort");
                        return "ABORT";
                    }
                }
            }
        }

        for (String key : txContext.writeSet.keySet()) {
            if (rowLocks.containsKey(key)) {
                lockMap = rowLocks.get(key);
            } else {
                rowLocks.put(key, new HashMap<>());
                lockMap = rowLocks.get(key);
            }

            for (UUID ukey : lockMap.keySet()) {
                if (ukey.equals(requesterId)) {
                    continue;
                }

                if (lockMap.get(ukey).equals('W')) {
                    log.info("abort");
                    return "ABORT";
                }

                if (lockMap.get(ukey).equals('R')) {
                    log.info("abort");
                    return "ABORT";
                }
            }
        }

        for (String key : txContext.readSet.keySet()) {
            lockMap = rowLocks.get(key);
            lockMap.put(requesterId, 'R');
        }

        for (String key : txContext.writeSet.keySet()) {
            lockMap = rowLocks.get(key);
            // lockmap must be either empty or 0 at this point
            lockMap.put(requesterId, 'W');
        }

        return "ACCEPT";
    }

    public String decideCommitOperation(TransactionContext txContext) {
        UUID requesterId = txContext.requesterId;
        HashMap<UUID, Character> lockMap;

        if (txContext.commitSuccessful) {
            hash.putAll(txContext.writeSet);
        }

        // remove any shared locks
        for (String key : txContext.readSet.keySet()) {
            lockMap = rowLocks.get(key);
            lockMap.remove(requesterId, 'R');
        }

        // remove any modified locks
        for (String key : txContext.writeSet.keySet()) {
            lockMap = rowLocks.get(key);
            lockMap.remove(requesterId, 'W');
        }

        return ACK;
    }

    /*
     * Generic method that takes an operation argument and applies it to the
     * db directly.
     */
    public void applyOperation(DbOperation operation) {
        String result = "REFRESH_CONFIG";
        String op = operation.type.trim().toUpperCase();

        // CONFIG VALIDATION
        switch (op) {
            case "GET":
                // fall through
            case "PUT":
                // fall through
            case "DELETE":
                // fall through
                if (!CheckKeyOwner(operation.key)) {
                    responseLog.put(operation.requestId, new Response(result, true));
                    return;
                }
                break;
            case "TRY_COMMIT":
                // fall through
            case "DECIDE_COMMIT":
                ArrayList<String> keys = new ArrayList<>();
                keys.addAll(operation.txContext.readSet.keySet());
                keys.addAll(operation.txContext.writeSet.keySet());

                if (!CheckKeyOwner(keys.toArray(new String[0]))) {
                    responseLog.put(operation.requestId, new Response(result, true));
                    return;
                }
                break;
        }

        switch (op) {
            case "GET":
                result = getOperation(operation.key);
                responseLog.put(operation.requestId, new Response(result, true));
                break;
            case "PUT":
                result = putOperation(operation.key, operation.value);
                responseLog.put(operation.requestId, new Response(result, true));
                break;
            case "DELETE":
                result = deleteOperation(operation.key);
                responseLog.put(operation.requestId, new Response(result, true));
                break;
            case "TRY_COMMIT":
                result = tryCommitOperation(operation.txContext);
                responseLog.put(operation.requestId, new Response(result, true));
                break;
            case "DECIDE_COMMIT":
                result = decideCommitOperation(operation.txContext);
                responseLog.put(operation.requestId, new Response(result, true));
                break;
            default:
                String response = "Client " + operation.from + ":" + "Invalid command " + operation.key + " was received";
                log.error(response);
                break;
        }
    }


    private Response threePhaseCommit(DbOperation operation) throws Exception {
        Response toSendBack = new Response("", false);
        try {
            mutex.acquire();

            //check to see if a Status
            if (responseLog.containsKey(operation.requestId)) {
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
        } finally {
            mutex.release();
        }
    }

    private boolean CheckKeyOwner(String... keys) {
        for (String key : keys) {
            Integer shardNo = UtilityClasses.atoi(key) % configuration.NUM_SHARDS;
            UUID shardOwner = configuration.shardToGroupIdMap.get(shardNo);
            if (!shardOwner.equals(myReplicaGroupId)) {
                return false;
            }
        }

        return true;
    }

    /*
     * Generate a sequence number for the given operation.
     * Starts consensus on current sequence number and returns with minimum viable sequence number
     * that hasn't been utilized by any other operation
     * during Paxos
     */
    private int DoPaxos(DbOperation operation) throws IOException, Exception {
        int sequence = currentSeqNo;
        for (; ; ) {
            paxosHelper.StartConsensus(sequence, UtilityClasses.encodeDbOperation(operation));

            long sleepFor = 60;
            DbOperation processed = null;
            for (; ; ) {
                Status status = paxosHelper.Status(sequence);

                if (status.done) {
                    processed = UtilityClasses.decodeDbOperation(status.value);
                    break;
                } else {
                    Thread.sleep(sleepFor);
                    if (sleepFor < CRASH_DURATION)
                        sleepFor *= 2;
                }
            }
            if (operation.requestId.compareTo(processed.requestId) == 0)
                break;
            else
                sequence++;
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
    private void updateResponseLog(int currentSeqNo, int sequenceAssigned) throws Exception {

        for (int i = currentSeqNo; i <= sequenceAssigned; i++) {
            Status status = paxosHelper.Status(i);
            if (status.done) {
                DbOperation consensusOperation = UtilityClasses.decodeDbOperation(status.getValue());
                if (!responseLog.containsKey((consensusOperation.requestId))) {
                    // log.info("Apply update at server " +hostname +":"+ (me() + 1) + ":" + consensusOperation.toString());
                    applyOperation(consensusOperation);
                }
            }
        }

    }

    /*
     * Pseudo-Random generator that either crashes the node or conducts a three phase commit
     * on the original request.
     */
    private Response stallIfCrashedExecuteIfNot(DbOperation operation) throws Exception {
        if (crashed)
            Thread.sleep(CRASH_DURATION);
        else {
            //Random r = new Random();
            //crash rate 12.5% i.e 1/8
            int randInt = 6;
            //randInt = r.nextInt(10000);
            if (randInt == 5) {
                log.info("FAIL/CRASH server " + hostname + ": CAUSED BY RANDOM FAILURE");
                crashed = true;
                Paxos.crashed = true;
                try {
                    Thread.sleep((numReplicas + 1) * CRASH_DURATION);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                crashed = false;
                Paxos.crashed = false;
                log.info("RESUSCITATED ZOMBIE SERVER " + hostname + " : IS ALIVE AGAIN! ");
                return new Response(" ", false);
            } else {
                return threePhaseCommit(operation);
            }
        }
        return new Response(" ", false);
    }

    @Override
    public void KILL() throws Exception {
        crashed = true;
        Paxos.crashed = true;
        log.info("FAIL/CRASH server " + hostname + ": CAUSED BY RMI CALL TO KILL");
        try {
            Thread.sleep((numReplicas + 1) * CRASH_DURATION);
        } catch (InterruptedException e) {

            e.printStackTrace();
        }
        crashed = false;
        Paxos.crashed = false;
        log.info("RESUSCITATED ZOMBIE SERVER " + hostname + " : IS ALIVE AGAIN! ");
        return;

    }

    private static UtilityClasses.Configuration getShardConfig(String[][] coordinators) {
        UtilityClasses.PollReply pollReply;
        UUID reqId = UUID.randomUUID();
        log.info("This request has id :" + reqId);
        int serverNum = new Random().nextInt(COORDINATOR_NUM_REPLICAS);
        int startServerNum = serverNum;
        while (true) {
            String hostname = coordinators[serverNum][0];
            String port = coordinators[serverNum][1];

            try {
                // locate the remote object initialize the proxy using the binder
                CoordinatorInterface hostImpl = (CoordinatorInterface) Naming.lookup("rmi://" + hostname + ":" + port + "/ShardCoordinator");
                pollReply = (UtilityClasses.PollReply) hostImpl.Poll(new UtilityClasses.PollArgs(-1, reqId));
                break;
            } catch (Exception e) {
                log.info("Contact server " + serverNum + " at hostname:port " + hostname + " : " + port + " FAILED. Trying others..");

            }
            serverNum = (serverNum + 1) % COORDINATOR_NUM_REPLICAS;

            if(serverNum == startServerNum) {
                log.info("No Coordinators available");
                System.exit(-1);
            }
        }

        return pollReply.getConfiguration();
    }

    private static String[][] readCoordinatorConfig() {
        String hostPorts[][] = new String[COORDINATOR_NUM_REPLICAS][2];
        try {
            BufferedReader fileReader = new BufferedReader(new FileReader("configs.txt"));
            log.info("Loading configurations from configs.txt..");
            for (int c = 0; c < COORDINATOR_NUM_REPLICAS; c++) {
                hostPorts[c] = fileReader.readLine().split("\\s+");
                if (hostPorts[c][0].isEmpty() || !hostPorts[c][1].matches("[0-9]+") || hostPorts[c][1].isEmpty()) {
                    log.error("You have made incorrect entries for addresses in config file, please investigate.");
                    System.exit(-1);
                }
            }
            fileReader.close();
        } catch (IOException e) {
            log.info("System exited with error " + e.getMessage());
            System.exit(-1);
        }
        return hostPorts;
    }
}
