package Client;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.rmi.Naming;
import java.rmi.server.RMISocketFactory;
import java.util.*;
import java.util.concurrent.*;

import Shards.CoordinatorInterface;
import Utility.UtilityClasses;

import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.FileAppender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;

import Db.DbServerInterface;
import Utility.UtilityClasses.Response;

public class TransactionClient {
    final static Logger log = Logger.getLogger(TransactionClient.class);
    final static String PATTERN = "%d [%p|%c|%C{1}] %m%n";
    final static int coordinatorRepilcas = 5;
    private static final int TIMEOUT = 1000;

    private static HashMap<String, Object> localStore = new HashMap<>();
    private static UtilityClasses.LocalTransactionContext localTransactionContext = null;
    private static UtilityClasses.Configuration configuration;
    private static UUID clientId = UUID.randomUUID();
    private static String clientName = "N/A";
    private static char protocol; // [t]wophase / [p]axos / [a]cyclic


    private static void configureLogger() {
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
        fa.setFile("log/client.log");
        fa.setLayout(new PatternLayout("%d %-5p [%c{1}] %m%n"));
        fa.setThreshold(Level.ALL);
        fa.setAppend(true);
        fa.activateOptions();

        //add appender to any Logger (here is root)
        log.addAppender(fa);
        log.setAdditivity(false);
        //repeat with all other desired appenders
    }

    private static void configureRMI() throws IOException {
        RMISocketFactory.setSocketFactory(new RMISocketFactory() {
            public Socket createSocket(String host, int port)
                    throws IOException {
                Socket socket = new Socket();
                socket.setSoTimeout((coordinatorRepilcas + 1) * TIMEOUT);
                socket.setSoLinger(false, 0);
                socket.connect(new InetSocketAddress(host, port), (coordinatorRepilcas + 1) * TIMEOUT);
                return socket;
            }

            public ServerSocket createServerSocket(int port)
                    throws IOException {
                return new ServerSocket(port);
            }
        });
    }

    private static String[][] readCoordinatorConfig() {
        String hostPorts[][] = new String[coordinatorRepilcas][2];
        try {
            BufferedReader fileReader = new BufferedReader(new FileReader("coordinator_config.txt"));
            log.info("Loading configurations from coordinator_config.txt..");
            for(int c = 0; c < coordinatorRepilcas; c++) {
                hostPorts[c] = fileReader.readLine().split("\\s+");
                if (hostPorts[c][0].isEmpty() || !hostPorts[c][1].matches("[0-9]+") || hostPorts[c][1].isEmpty()) {
                    log.info("You have made incorrect entries for addresses in config file, please investigate.");
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

    public static void main(String args[]) throws IOException {
        configureLogger();
        configureRMI();
        String coordinators[][] = readCoordinatorConfig();
        configuration = getShardConfig(coordinators);
        if(args[1].startsWith("t") | args[1].startsWith("p") | args[1].startsWith("a")) {
            protocol = args[1].charAt(0);
        }
        else {
            log.error("Select transaction protocol for client");
            System.exit(-1);
        }
        clientName = InetAddress.getLocalHost().getHostName();

        String line;
        String tokens[];
        ArrayList<String> program = new ArrayList<>();
        int current = 0;

        BufferedReader br = new BufferedReader(new InputStreamReader(System.in));

        while(true) {
            if(current < program.size()) {
                line = program.get(current);
            }
            else {
                line = br.readLine();
                if(line == null) {
                    break;
                }
                line = line.trim();
                program.add(line);
            }

            tokens = line.split("\\s+");
            String command = tokens[0];

            String key, reg;
            Object value;
            boolean committed;

            switch (command) {
                case "GET":
                    // GET [KEY] INTO [$REGD]
                    if(localTransactionContext == null) {
                        localTransactionContext = new UtilityClasses.LocalTransactionContext(clientId, current);
                    }

                    key = tokens[1];
                    reg = tokens[3];
                    handleGet(key, reg, UUID.randomUUID());

                    committed = tryCommitTransaction(UUID.randomUUID());
                    if(!committed) {
                        current = localTransactionContext.startLine;
                        localTransactionContext = null;
                        continue;
                    }
                    else {
                        localTransactionContext = null;
                        break;
                    }

                case "PUT":
                    // PUT [VALUE] INTO [KEY]
                    if(localTransactionContext == null) {
                        localTransactionContext = new UtilityClasses.LocalTransactionContext(clientId, current);
                    }

                    value = valuate(tokens[1]);
                    key = tokens[3];
                    handlePut(key, value, UUID.randomUUID());

                    committed = tryCommitTransaction(UUID.randomUUID());
                    if(!committed) {
                        current = localTransactionContext.startLine;
                        localTransactionContext = null;
                        continue;
                    }
                    else {
                        localTransactionContext = null;
                        break;
                    }

                case "DELETE":
                    // DELETE [KEY]
                    if(localTransactionContext == null) {
                        localTransactionContext = new UtilityClasses.LocalTransactionContext(clientId, current);
                    }

                    key = tokens[2];
                    handleDelete(key, UUID.randomUUID());

                    committed = tryCommitTransaction(UUID.randomUUID());
                    if(!committed) {
                        current = localTransactionContext.startLine;
                        localTransactionContext = null;
                        continue;
                    }
                    else {
                        localTransactionContext = null;
                        break;
                    }

                case "ADDI":
                    // ADDI [$REG1] [$REG2] INTO [$REGD]

                    String reg1 = tokens[1]; // must be integer values
                    String reg2 = tokens[2];
                    String regd = tokens[4];

                    Object val1 = valuate(reg1);
                    Object val2 = valuate(reg2);
                    try {
                        if(val1 instanceof String) {
                            val1 = Integer.parseInt(val1.toString());
                        }

                        if(val2 instanceof String) {
                            val2 = Integer.parseInt(val2.toString());
                        }
                    }
                    catch(NumberFormatException ex) {
                        log.error("Argument register or literal values must be integers");
                    }

                    localTransactionContext.store.put(regd, (int) val1 + (int) val2);

                    break;

                case "PRINT":
                    reg = tokens[1];
                    log.info("Value of " + reg + " = " + valuate(reg));
                    break;

                case "START_TRANSACTOIN":
                    localTransactionContext = new UtilityClasses.LocalTransactionContext(clientId, current);
                    break;

                case "COMMIT_TRANSACTION":
                    committed = tryCommitTransaction(UUID.randomUUID());
                    if(!committed) {
                        current = localTransactionContext.startLine;
                        localTransactionContext = null;
                        continue;
                    }
                    else {
                        localTransactionContext = null;
                        break;
                    }

                default:
                    log.error("Unrecognized instruction");

            }

            current++;
        }
    }

    public static UtilityClasses.Configuration getShardConfig(String[][] coordinators) {
        UtilityClasses.PollReply pollReply;
        UUID reqId = UUID.randomUUID();
        log.info("This request has id :" + reqId);
        int serverNum = new Random().nextInt(coordinatorRepilcas);

        while (true) {
            String hostname = coordinators[serverNum][0];
            String port = coordinators[serverNum][1];

            try {
                // locate the remote object initialize the proxy using the binder
                CoordinatorInterface hostImpl = (CoordinatorInterface) Naming.lookup("rmi://" + hostname + ":" + port + "/Calls");
                pollReply = (UtilityClasses.PollReply) hostImpl.Poll(new UtilityClasses.PollArgs(-1, reqId));
                break;
            } catch (Exception e) {
                log.info("Contact server " + serverNum + " at hostname:port " + hostname + " : " + port + " FAILED. Trying others..");

            }
            serverNum = (serverNum + 1) % coordinatorRepilcas;
        }
        return pollReply.getConfiguration();
    }

    private static boolean handleGet(String key, String reg, UUID reqId) {
        String hostname;
        int port;

        Object value;

        if(localTransactionContext.store.containsKey(key)) {
            value = localTransactionContext.store.get(key);
        }
        else {
            List<UtilityClasses.HostPorts> hostPorts = configuration.getDbServersForKey(key);
            int i = new Random().nextInt(hostPorts.size());
            Response r = new Response("", false);
            while (!r.done) {
                UtilityClasses.HostPorts hostPort = hostPorts.get(i);
                hostname = hostPort.getHostName();
                port = hostPort.getPort();
                try {
                    DbServerInterface hostImpl = (DbServerInterface) Naming.lookup("rmi://" + hostname + ":" + port + "/Calls");
                    r = hostImpl.GET(clientName, key, reqId);
                } catch (Exception e) {
                    log.info("Contact server hostname:port " + hostname + " : " + port + " FAILED. Trying others..");
                }
                i = (i + 1) % hostPorts.size();
            }
            value = r.getValue();
            localTransactionContext.txContext.readSet.put(key, value);
        }

        localTransactionContext.store.put(reg, value);

        return true;
    }

    private static boolean handlePut(String key, Object value, UUID reqId) {
        localTransactionContext.txContext.writeSet.put(key, value);
        localTransactionContext.store.put(key, value);
        return true;
    }

    private static boolean handleDelete(String key, UUID reqId) {
        localTransactionContext.txContext.writeSet.put(key, null);
        localTransactionContext.store.put(key, null);
        return true;
    }

    private static boolean tryCommitTransaction(UUID reqId) {
        boolean retvalue;
        if(protocol == 't') {
            retvalue = tryTowPhaseCommitCommitTransaction(reqId);
        }
        else if(protocol == 'a') {
            retvalue = tryAcyclicCommitTransaction(reqId);
        }
        else { // if(protocol == 'p')
            retvalue = false;
        }
        return retvalue;
    }

    private static boolean tryTowPhaseCommitCommitTransaction(UUID reqId) {
        boolean commitSuccessful = true;

        Set<String> allkeys = new HashSet<>();
        allkeys.addAll(localTransactionContext.txContext.readSet.keySet());
        allkeys.addAll(localTransactionContext.txContext.writeSet.keySet());

        List<List<UtilityClasses.HostPorts>> groups = configuration.getDbServersForKeys(allkeys.toArray(new String[0]));

        ExecutorService executorService = Executors.newFixedThreadPool(groups.size());
        List<Future<Response>> futures = new ArrayList<>();

        for(List<UtilityClasses.HostPorts> group: groups) {
            futures.add(executorService.submit(new Callable<Response>() {
                @Override
                public Response call() throws Exception {
                    String hostname;
                    int port;
                    int i = new Random().nextInt(group.size());
                    Response r = new Response("", false);
                    while (!r.done) {
                        UtilityClasses.HostPorts hostPort = group.get(i);
                        hostname = hostPort.getHostName();
                        port = hostPort.getPort();
                        try {
                            DbServerInterface hostImpl = (DbServerInterface) Naming.lookup("rmi://" + hostname + ":" + port + "/Calls");
                            r = hostImpl.TRY_COMMIT(clientName, localTransactionContext.txContext, reqId);
                        } catch (Exception e) {
                            log.info("Contact server hostname:port " + hostname + " : " + port + " FAILED. Trying others..");
                        }
                        i = (i + 1) % group.size();
                    }
                    return r;
                }
            }));
        }

        for(Future<Response> f : futures) {
            try {
                Response r = f.get();
                if(r.value.equals("abort")) {
                    commitSuccessful = false;
                }
            }
            catch (Exception ex) {
                log.error(ex.getMessage());
            }
        }

        localTransactionContext.txContext.commitSuccessful = commitSuccessful;

        List<Callable<Response>> callables = new ArrayList<>();
        for(List<UtilityClasses.HostPorts> group: groups) {
            callables.add(new Callable<Response>() {
                @Override
                public Response call() throws Exception {
                    String hostname;
                    int port;
                    int i = new Random().nextInt(group.size());
                    Response r = new Response("", false);
                    while (!r.done) {
                        UtilityClasses.HostPorts hostPort = group.get(i);
                        hostname = hostPort.getHostName();
                        port = hostPort.getPort();
                        try {
                            DbServerInterface hostImpl = (DbServerInterface) Naming.lookup("rmi://" + hostname + ":" + port + "/Calls");
                            r = hostImpl.DECIDE_COMMIT(clientName, localTransactionContext.txContext, reqId);
                        } catch (Exception e) {
                            log.info("Contact server hostname:port " + hostname + " : " + port + " FAILED. Trying others..");
                        }
                        i = (i + 1) % group.size();
                    }
                    return r;
                }
            });
        }

        try {
            executorService.invokeAll(callables);
        } catch (InterruptedException e) {
            log.error(e.getMessage());
        }

        if(commitSuccessful) {
            // merge transaction store with localStore
            localStore.putAll(localTransactionContext.store);
            return true;
        }

        return commitSuccessful;
    }

    private static boolean tryAcyclicCommitTransaction(UUID reqId) {
        String hostname;
        int port;

        String readSetFirstKey = localTransactionContext.txContext.readSet.firstKey();
        String writeSetFirstKey = localTransactionContext.txContext.writeSet.firstKey();
        String firstKey = readSetFirstKey.compareTo(writeSetFirstKey) < 0 ? readSetFirstKey : writeSetFirstKey;

        List<UtilityClasses.HostPorts> hostPorts = configuration.getDbServersForKey(firstKey);
        int i = new Random().nextInt(hostPorts.size());
        Response r = new Response("", false);
        while (!r.done) {
            UtilityClasses.HostPorts hostPort = hostPorts.get(i);
            hostname = hostPort.getHostName();
            port = hostPort.getPort();
            try {
                DbServerInterface hostImpl = (DbServerInterface) Naming.lookup("rmi://" + hostname + ":" + port + "/Calls");
                // TODO: change next line
                r = hostImpl.TRY_COMMIT(clientName, localTransactionContext.txContext, reqId);
            } catch (Exception e) {
                log.info("Contact server hostname:port " + hostname + " : " + port + " FAILED. Trying others..");
            }
            i = (i + 1) % hostPorts.size();
        }

        // TODO: change next line
        if(r.getValue() == "commited") {
            // merge transaction store with localStore
            localStore.putAll(localTransactionContext.store);
            return true;
        }
        // else
        return false;
    }

    private static Object valuate(String input) {
        Object value = input;
        boolean isRegister = input.trim().startsWith("$");

        if(!isRegister) {
            return value; // return it as a literal value
        }
        else if (localStore.containsKey(input)) {
            return localStore.get(input);
        }
        else if(localTransactionContext != null) {
            if(localTransactionContext.store.containsKey(input)) {
                return localTransactionContext.store.get(input);
            }
        }

        log.error("Couldn't valuate register: " + input);
        return null;
    }
}
