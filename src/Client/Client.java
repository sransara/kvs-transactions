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
import java.util.HashMap;
import java.util.Random;
import java.util.UUID;

import Shards.CoordinatorInterface;
import Utility.UtilityClasses;
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


    private static HashMap<String, String> local = new HashMap<String, String>();
    private static ClientTransaction.Context transactionContext = new ClientTransaction.Context();
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

        BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
        while((line = br.readLine()) != null) {
            line = line.trim();

            tokens = line.split("\\s+");
            String command = tokens[0];

            UUID uuid = UUID.randomUUID();

            switch (command) {
                case "GET":
                    // GET [KEY] [$REG]
                    String key = tokens[1];
                    String reg = tokens[2];
                    handleGet(key, reg, uuid);

                    break;

                case "PUT":
                    // PUT [KEY], [VALUE]
                    break;

                case "DELETE":
                    // DELETE [KEY]
                    break;

                case "ADD":
                    // ADD [$REG1] [$REG2] [$REGD]
                    String reg1 = tokens[1];
                    String reg2 = tokens[2];
                    String regd = tokens[3];

                    local.put(regd, local.get(reg1) + local.get(reg2));
                    break;

                case "START_TRANSACTOIN":
                    // START_TRANSACTION
                    if(!transactionContext.isCommited) {
                        handleCommitTransaction();
                    }
                    transactionContext = new ClientTransaction.Context();
                    break;

                case "COMMIT_TRANSACTION":
                    // COMMIT_TRANSACTION
                    break;
            }
        }
    }

    public static UtilityClasses.Configuration getShardConfig(String[][] coordinators) {
        try {

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
                    pollReply = hostImpl.Poll(new UtilityClasses.PollArgs(-1, uuid));
                } catch (Exception e) {
                    log.info("Contact server " + serverNum + " at hostname:port " + hostname + " : " + port + " FAILED. Trying others..");

                }
                serverNum = (serverNum + 1) % coordinatorRepilcas;
            }
            return pollReply.getConfiguration();
        } catch (Exception e) {
            e.printStackTrace();
            log.error("Error occured while connecting to RMI server with error, " + e.getMessage());
        }
    }

    public static void handleGet(String key, String reg, UUID uuid) {
        String hostname;
        int port;

        String value;

        if (local.containsKey(key)) {
            value = local.get(key);
        } else {
            UtilityClasses.HostPorts hostPorts[] = configuration.getDbServersForKey(key);
            int i = new Random().nextInt(hostPorts.length);
            Response r = new Response("", false);
            while (!r.done) {
                UtilityClasses.HostPorts hostPort = hostPorts[i];
                hostname = hostPort.getHostName();
                port = hostPort.getPort();
                try {
                    DbServerInterface hostImpl = (DbServerInterface) Naming.lookup("rmi://" + hostname + ":" + port + "/Calls");
                    // TODO: change next line
                    r = hostImpl.GET("N/A", key, uuid);
                } catch (Exception e) {
                    log.info("Contact server hostname:port " + hostname + " : " + port + " FAILED. Trying others..");
                }
                i = (i + 1) % hostPorts.length;
            }
            value = (String) r.getValue();
            transactionContext.readSet.add(new ClientTransaction.KeyValue(key, value));
        }

        local.put(reg, value);
    }

    public static void handleCommitTransaction() {

    }
}

