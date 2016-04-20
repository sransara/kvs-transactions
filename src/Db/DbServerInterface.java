package Db;

import java.rmi.Remote;
import java.util.UUID;

import Client.ClientTransaction;
import Utility.UtilityClasses.Response;
/*
 * Server interface- clients can call the following methods
 */
public interface DbServerInterface extends Remote{
	public Response PUT(String clientId, String key,String value, UUID requestId) throws Exception;
	public Response GET(String clientId,String key, UUID requestId)throws Exception;
	public Response DELETE(String clientId, String key, UUID requestId)throws Exception;
	public Response COMMIT(String clientId, ClientTransaction.TransactionContext txContext, UUID requestId)throws Exception;
	public void KILL() throws Exception;
}
