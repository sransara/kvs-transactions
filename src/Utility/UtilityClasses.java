package Utility;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.*;

public class UtilityClasses {

    public static class TransactionContext implements Serializable {
        /**
         *
         */
        private static final long serialVersionUID = 1L;
        public UUID requesterId;
        public SortedMap<String, Object> readSet = new TreeMap<>();
        public SortedMap<String, Object> writeSet = new TreeMap<>();
        public boolean commitSuccessful = false;
    }

    public static class LocalTransactionContext {
        public TransactionContext txContext = new TransactionContext();
        public HashMap<String, Object> store = new HashMap<>();
        public int startLine = 0;

        public LocalTransactionContext(UUID requesterId, int startLine) {
            txContext.requesterId = requesterId;
            this.startLine = startLine;
        }
    }
    public static class Configuration implements Serializable {
        @Override
        public String toString() {
            return "Configuration [confNo=" + confNo + ", replicaGroupMap="
                    + System.lineSeparator() + replicaGroupMap + System.lineSeparator() + ", shardToGroupIdMap="
                    + System.lineSeparator()  + shardToGroupIdMap + " + ]";
        }

        public static final int NUM_SHARDS = 20;
        private static final long serialVersionUID = 1L;

        public Configuration(int confNo,
                             HashMap<Integer, UUID> shardToGroupId,
                             HashMap<UUID, List<HostPorts>> replicaGroupMap) {
            super();
            this.confNo = confNo;
            this.shardToGroupIdMap = shardToGroupId;
            this.replicaGroupMap = replicaGroupMap;
        }

        public int confNo;
        public HashMap<UUID, List<HostPorts>> replicaGroupMap;
        public HashMap<Integer, UUID> shardToGroupIdMap;


    }

    public static int atoi(String str) {
        if (str == null || str.length() < 1)
            return 0;

        // trim white spaces
        str = str.trim();
        str = str.toUpperCase();
        // check negative or positive
        int i = 0;
        // use double to store result
        double result = 0;

        // calculate value
        while (str.length() > i && str.charAt(i) >= '0' && str.charAt(i) <= '9') {
            result = result * 10 + (str.charAt(i) - '0');
            i++;
        }
        // handle max and min
        if (result > Integer.MAX_VALUE)
            return Integer.MAX_VALUE;

        if (result < Integer.MIN_VALUE)
            return Integer.MIN_VALUE;

        return (int) result;
    }


    /**
     * Write the object to a Base64 string.
     */
    private static String toString(Serializable o) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ObjectOutputStream oos = new ObjectOutputStream(baos);
        oos.writeObject(o);
        oos.close();
        return Base64.getEncoder().encodeToString(baos.toByteArray());
    }


    /**
     * Read the object from Base64 string.
     */
    private static Object fromString(String s) throws IOException,
            ClassNotFoundException {
        byte[] data = Base64.getDecoder().decode(s);
        ObjectInputStream ois = new ObjectInputStream(
                new ByteArrayInputStream(data));
        Object o = ois.readObject();
        ois.close();
        return o;
    }

    public static DbOperation decodeDbOperation(String s) throws ClassNotFoundException, IOException {
        DbOperation operation = (DbOperation) fromString(s);
        return operation;
    }

    public static String encodeDbOperation(DbOperation operation) throws IOException {
        return toString(operation);
    }

    public static ShardOperation decodeShardOperation(String s) throws ClassNotFoundException, IOException {
        ShardOperation operation = (ShardOperation) fromString(s);
        return operation;
    }

    public static String encodeShardOperation(ShardOperation operation) throws IOException {
        return toString(operation);
    }


    public static class PrepareArgs implements Serializable {
        /**
         *
         */
        private static final long serialVersionUID = 1L;
        int sequenceNo;

        public PrepareArgs(int sequenceNo, int proposalNo, int key) {
            super();
            this.sequenceNo = sequenceNo;
            this.proposalNo = proposalNo;
            this.key = key;
        }

        int proposalNo;
        int key;

        public int getSequenceNo() {
            return sequenceNo;
        }

        public void setSequenceNo(int sequenceNo) {
            this.sequenceNo = sequenceNo;
        }

        public int getProposalNo() {
            return proposalNo;
        }

        public void setProposalNo(int proposalNo) {
            this.proposalNo = proposalNo;
        }

        public int getKey() {
            return key;
        }

        public void setKey(int key) {
            this.key = key;
        }
    }

    public static class HostPorts implements Serializable {
        /**
         *
         */
        private static final long serialVersionUID = 1L;
		String hostName;
        @Override
		public String toString() {
			return "HostPorts [hostName=" + hostName + ", port=" + port + "]";
		}

		int port;

        public String getHostName() {
            return hostName;
        }

        public void setHostName(String hostName) {
            this.hostName = hostName;
        }

        public int getPort() {
            return port;
        }

        public HostPorts(String hostName, int port) {
            super();
            this.hostName = hostName;
            this.port = port;
        }

        public void setPort(int port) {
            this.port = port;
        }
    }

    public static class PrepareReply implements Serializable {
        /**
         *
         */
        private static final long serialVersionUID = 1L;
        int highestPrepareNo;
        int highestProposalNo;
        boolean ok;

        public PrepareReply(int highestPrepareNo, int highestProposalNo,
                            String value, boolean ok) {
            super();
            this.highestPrepareNo = highestPrepareNo;
            this.highestProposalNo = highestProposalNo;
            this.value = value;
            this.ok = ok;
        }

        public boolean isOk() {
            return ok;
        }

        public void setOk(boolean ok) {
            this.ok = ok;
        }

        public int getHighestPrepareNo() {
            return highestPrepareNo;
        }

        public void setHighestPrepareNo(int highestPrepareNo) {
            this.highestPrepareNo = highestPrepareNo;
        }

        public int getHighestProposalNo() {
            return highestProposalNo;
        }

        public void setHighestProposalNo(int highestProposalNo) {
            this.highestProposalNo = highestProposalNo;
        }

        public String getValue() {
            return value;
        }

        public void setValue(String value) {
            this.value = value;
        }

        String value;
    }

    public static class AcceptArgs implements Serializable {
        /**
         *
         */
        private static final long serialVersionUID = 1L;
        int sequenceNo;
        int proposalNo;
        String value;

        public int getSequenceNo() {
            return sequenceNo;
        }

        public void setSequenceNo(int sequenceNo) {
            this.sequenceNo = sequenceNo;
        }

        public int getProposalNo() {
            return proposalNo;
        }

        public void setProposalNo(int proposalNo) {
            this.proposalNo = proposalNo;
        }

        public String getValue() {
            return value;
        }

        public void setValue(String value) {
            this.value = value;
        }

        public AcceptArgs(int sequenceNo, int proposalNo, String value) {
            super();
            this.sequenceNo = sequenceNo;
            this.proposalNo = proposalNo;
            this.value = value;
        }

    }

    public static class AcceptReply implements Serializable {
        /**
         *
         */
        private static final long serialVersionUID = 1L;
        boolean OK;

        public boolean isOK() {
            return OK;
        }

        public void setOK(boolean oK) {
            OK = oK;
        }

        public AcceptReply(boolean oK) {
            super();
            OK = oK;
        }

    }

    public static class LearnArgs implements Serializable {
        /**
         *
         */
        private static final long serialVersionUID = 1L;
        int sequenceNo;
        String value;
        int me;

        public LearnArgs(int sequenceNo, String value, int me, int maxDoneSeq) {
            super();
            this.sequenceNo = sequenceNo;
            this.value = value;
            this.me = me;
            MaxDoneSeq = maxDoneSeq;
        }

        public int getSequenceNo() {
            return sequenceNo;
        }

        public void setSequenceNo(int sequenceNo) {
            this.sequenceNo = sequenceNo;
        }

        public String getValue() {
            return value;
        }

        public void setValue(String value) {
            this.value = value;
        }

        public int getMe() {
            return me;
        }

        public void setMe(int me) {
            this.me = me;
        }

        public int getMaxDoneSeq() {
            return MaxDoneSeq;
        }

        public void setMaxDoneSeq(int maxDoneSeq) {
            MaxDoneSeq = maxDoneSeq;
        }

        int MaxDoneSeq;
    }

    public static class LearnReply implements Serializable {
        /**
         *
         */
        private static final long serialVersionUID = 1L;
        boolean OK;

        public LearnReply(boolean oK) {
            super();
            OK = oK;
        }

        public boolean isOK() {
            return OK;
        }

        public void setOK(boolean oK) {
            OK = oK;
        }

    }

    public static class AcceptorState implements Serializable {
        /**
         *
         */
        private static final long serialVersionUID = 1L;
        int N_p;
        int N_a;
        String value;

        public int getN_p() {
            return N_p;
        }

        public AcceptorState(int n_p, int n_a, String value) {
            super();
            N_p = n_p;
            N_a = n_a;
            this.value = value;
        }

        public void setN_p(int n_p) {
            N_p = n_p;
        }

        public int getN_a() {
            return N_a;
        }

        public void setN_a(int n_a) {
            N_a = n_a;
        }

        public String getValue() {
            return value;
        }

        public void setValue(String value) {
            this.value = value;
        }
    }


    public static class Status implements Serializable {
        /**
         *
         */
        private static final long serialVersionUID = 1L;
        public String value;
        public boolean done;

        public String getValue() {
            return value;
        }

        public void setValue(String value) {
            this.value = value;
        }

        public boolean isDone() {
            return done;
        }

        public void setDone(boolean done) {
            this.done = done;
        }

        public Status(String value, boolean done) {
            super();
            this.value = value;
            this.done = done;
        }

    }

    public static class Response implements Serializable {
        /**
         *
         */
        private static final long serialVersionUID = 1L;
        public Object value;
        public boolean done;

        public Object getValue() {
            return value;
        }

        public void setValue(Object value) {
            this.value = value;
        }

        public boolean isDone() {
            return done;
        }

        public void setDone(boolean done) {
            this.done = done;
        }

        public Response(Object value, boolean done) {
            super();
            this.value = value;
            this.done = done;
        }

    }


    public static class DbOperation implements Serializable {
        /**
         *
         */
        private static final long serialVersionUID = 1L;
        public String type;
        public String key;
        public String value;
        public String from;
        public TransactionContext txContext;
        public UUID requestId;

        public DbOperation(String type, String key, String value, String from, UUID requestId) {
            super();
            this.type = type;
            this.key = key;
            this.value = value;
            this.from = from;
            this.requestId = requestId;
        }

        public DbOperation(String type, TransactionContext transactionContext, String from, UUID requestId) {
            super();
            this.type = type;
            this.txContext = transactionContext;
            this.from = from;
            this.requestId = requestId;
        }

        @Override
        public String toString() {
            return "Operation [" + type + " " + key +
                    " " + value + ", fromClient=" + from + ", requestId=" + requestId
                    + "]";
        }

    }

    public static class ShardOperation implements Serializable {

        /**
         *
         */
        private static final long serialVersionUID = 1L;

        @Override
        public String toString() {
            return "ShardOperation [type=" + type + ", shardArgs=" + shardArgs
                    + "]";
        }

        public String type;
        public ShardArgs shardArgs;

        public ShardOperation(String type, ShardArgs shardArgs) {
            super();
            this.type = type;
            this.shardArgs = shardArgs;
        }
    }

    public static interface ShardArgs extends Serializable {

        public UUID getUUID();

    }

    public static interface ShardReply extends Serializable {

    }

    public static class LeaveArgs implements ShardArgs {
        private static final long serialVersionUID = 1L;
        public UUID groupId;
        public UUID uuid;

        public LeaveArgs(UUID groupId, UUID uuid) {
            super();
            this.groupId = groupId;
            this.uuid = uuid;
        }

        @Override
        public UUID getUUID() {
            return uuid;
        }


    }

    public static class LeaveReply implements ShardReply {

        /**
         *
         */
        private static final long serialVersionUID = 1L;
        public String message;
    }

    public static class JoinReply implements ShardReply {

        /**
         *
         */
        private static final long serialVersionUID = 1L;
        public String message;

    }

    public static class JoinArgs implements ShardArgs {
        private static final long serialVersionUID = 1L;
        public UUID groupId;
        public UUID uuid;
        public List<HostPorts> servers;

        public JoinArgs(UUID groupId, List<HostPorts> servers, UUID uuid) {
            super();
            this.groupId = groupId;
            this.servers = servers;
            this.uuid = uuid;
        }

        @Override
        public UUID getUUID() {

            return uuid;
        }

    }

    public static class PollArgs implements ShardArgs {
        private static final long serialVersionUID = 1L;
        public int configurationSequenceNumber;
        public UUID uuid;

        public PollArgs(int configurationSequenceNumber, UUID uuid) {
            super();
            this.configurationSequenceNumber = configurationSequenceNumber;
            this.uuid = uuid;
        }

        @Override
        public UUID getUUID() {
            return uuid;
        }


    }

    public static class PollReply implements ShardReply {
        private static final long serialVersionUID = 1L;
        Configuration configuration;

        public PollReply(Configuration configuration) {
            super();
            this.configuration = configuration;
        }

        public Configuration getConfiguration() {
            return configuration;
        }

        public void setConfiguration(Configuration configuration) {
            this.configuration = configuration;
        }
    }

    public static class InvalidReply implements ShardReply {

        /**
         *
         */
        private static final long serialVersionUID = 1L;

    }

}
