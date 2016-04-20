package Client;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class ClientTransaction {
    public static class KeyValue implements Serializable{
        String key;
        String value;

        public KeyValue(String key, String value) {
            this.key = key;
            this.value = value;
        }
    }

    public static class TransactionContext implements  Serializable{
        public List<KeyValue> readSet = new ArrayList<>();
        public List<String> writeSet = new ArrayList<>();
    }

    public static class LocalTransactionContext {
        TransactionContext txContext = new TransactionContext();
        HashMap<String, String> store = new HashMap<>();
        boolean tried = false;
        int startLine = 0;
    }
}
