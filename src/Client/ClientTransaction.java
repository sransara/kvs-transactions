package Client;

import java.io.Serializable;
import java.util.*;

public class ClientTransaction {
    public static class TransactionContext implements  Serializable{
        public SortedMap<String, Object> readSet = new TreeMap<>();
        public SortedMap<String, Object> writeSet = new TreeMap<>();
    }

    public static class LocalTransactionContext {
        TransactionContext txContext = new TransactionContext();
        HashMap<String, Object> store = new HashMap<>();
        int startLine = 0;

        public LocalTransactionContext(int startLine) {
            this.startLine = startLine;
        }
    }
}
