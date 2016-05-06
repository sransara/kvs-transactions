package Db;

import java.rmi.Remote;
import java.util.UUID;


import Utility.UtilityClasses.*;

/*
 * Server interface- clients can call the following methods
 */
public interface DbServerInterface extends Remote {
    public Response PUT(String clientId, String key, Object value, UUID requestId) throws Exception;

    public Response GET(String clientId, String key, UUID requestId) throws Exception;

    public Response DELETE(String clientId, String key, UUID requestId) throws Exception;

    public Response TRY_COMMIT(String clientId, TransactionContext txContext, UUID requestId) throws Exception;

    public Response DECIDE_COMMIT(String client, TransactionContext txContext, UUID requestId) throws Exception;

    public void KILL() throws Exception;
}
