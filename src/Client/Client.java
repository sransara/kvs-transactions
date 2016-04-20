package Client;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.rmi.Naming;
import java.rmi.server.RMISocketFactory;
import java.util.*;

import Shards.CoordinatorInterface;
import Utility.UtilityClasses;
import Utility.UtilityClasses.ClientTransaction;

import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.FileAppender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;

import Db.DbServerInterface;
import Utility.UtilityClasses.Response;

public class Client {
    final static Logger log = Logger.getLogger(Client.class);
    final static String PATTERN = "%d [%p|%c|%C{1}] %m%n";
    final static int coordinatorRepilcas = 5;
    private static final int TIMEOUT = 1000;


    private static HashMap<String, Object> localStore = new HashMap<>();
    private static ClientTransaction.LocalTransactionContext localTransactionContext = null;
    private static UtilityClasses.Configuration configuration;


    static void configureLogger() {
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

    public static void configureRMI() throws IOException {
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

    public static String[][] readCoordinatorConfig() {
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
        String clientId = "NA";
        String coordinators[][] = readCoordinatorConfig();
        configuration = getShardConfig(coordinators);

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
                        localTransactionContext = new ClientTransaction.LocalTransactionContext(current);
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
                        localTransactionContext = new ClientTransaction.LocalTransactionContext(current);
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
                        localTransactionContext = new ClientTransaction.LocalTransactionContext(current);
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
                    break;

                case "START_TRANSACTOIN":
                    localTransactionContext = new ClientTransaction.LocalTransactionContext(current);
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
                    log.info("Unrecognized instruction");

            }

            current++;
        }
    }

    public static UtilityClasses.Configuration getShardConfig(String[][] coordinators) {
        UtilityClasses.PollReply pollReply;
        UUID uuid = UUID.randomUUID();
        log.info("This request has id :" + uuid);
        int serverNum = new Random().nextInt(coordinatorRepilcas);

        while (true) {
            String hostname = coordinators[serverNum][0];
            String port = coordinators[serverNum][1];

            try {
                // locate the remote object initialize the proxy using the binder
                CoordinatorInterface hostImpl = (CoordinatorInterface) Naming.lookup("rmi://" + hostname + ":" + port + "/Calls");
                pollReply = (UtilityClasses.PollReply) hostImpl.Poll(new UtilityClasses.PollArgs(-1, uuid));
                break;
            } catch (Exception e) {
                log.info("Contact server " + serverNum + " at hostname:port " + hostname + " : " + port + " FAILED. Trying others..");

            }
            serverNum = (serverNum + 1) % coordinatorRepilcas;
        }
        return pollReply.getConfiguration();
    }

    public static boolean handleGet(String key, String reg, UUID uuid) {
        String hostname;
        int port;

        Object value;

        if (localStore.containsKey(key)) {
            value = localStore.get(key);
        }
        else if(localTransactionContext.store.containsKey(key)) {
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
                    // TODO: change next line
                    r = hostImpl.GET("N/A", key, uuid);
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

    public static boolean handlePut(String key, Object value, UUID uuid) {
        String hostname;
        int port;

        List<UtilityClasses.HostPorts> hostPorts = configuration.getDbServersForKey(key);
        int i = new Random().nextInt(hostPorts.size());
        Response r = new Response("", false);
        while (!r.done) {
            UtilityClasses.HostPorts hostPort = hostPorts.get(i);
            hostname = hostPort.getHostName();
            port = hostPort.getPort();
            try {
                DbServerInterface hostImpl = (DbServerInterface) Naming.lookup("rmi://" + hostname + ":" + port + "/Calls");
                // TODO: change next line
                r = hostImpl.PUT("N/A", key, value.toString(), uuid);
            } catch (Exception e) {
                log.info("Contact server hostname:port " + hostname + " : " + port + " FAILED. Trying others..");
            }
            i = (i + 1) % hostPorts.size();
        }

        localTransactionContext.txContext.writeSet.put(key, value);
        localTransactionContext.store.put(key, value);

        return true;
    }

    public static boolean handleDelete(String key, UUID uuid) {
        String hostname;
        int port;

        List<UtilityClasses.HostPorts> hostPorts = configuration.getDbServersForKey(key);
        int i = new Random().nextInt(hostPorts.size());
        Response r = new Response("", false);
        while (!r.done) {
            UtilityClasses.HostPorts hostPort = hostPorts.get(i);
            hostname = hostPort.getHostName();
            port = hostPort.getPort();
            try {
                DbServerInterface hostImpl = (DbServerInterface) Naming.lookup("rmi://" + hostname + ":" + port + "/Calls");
                // TODO: change next line
                r = hostImpl.DELETE("N/A", key, uuid);
            } catch (Exception e) {
                log.info("Contact server hostname:port " + hostname + " : " + port + " FAILED. Trying others..");
            }
            i = (i + 1) % hostPorts.size();
        }

        localTransactionContext.txContext.writeSet.put(key, null);
        localTransactionContext.store.put(key, null);

        return true;
    }

    public static boolean tryCommitTransaction(UUID uuid) {
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
                r = hostImpl.COMMIT("N/A", localTransactionContext.txContext, uuid);
            } catch (Exception e) {
                log.info("Contact server hostname:port " + hostname + " : " + port + " FAILED. Trying others..");
            }
            i = (i + 1) % hostPorts.size();
        }

        // TODO: change next line
        if(r.getValue() == "commited") {
            // merge transaction store with localStore
            for (Map.Entry<String, Object> entry : localTransactionContext.store.entrySet()) {
                String key = entry.getKey();
                Object value = entry.getValue();

                localTransactionContext.store.put(key, value);
            }
            return true;
        }
        // else
        return false;
    }

    public static Object valuate(String input) {
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
