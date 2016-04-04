package project4;

import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.UUID;

import project4.UtilityClasses.Response;
import project4.UtilityClasses.Status;
/*
 * Server interface- clients can call the following methods
 */
public interface RMIServerInterface extends Remote{
	public Response PUT(String clientId, String key,String value, UUID requestId) throws Exception;
	public Response GET(String clientId,String key, UUID requestId)throws Exception;
	public Response DELETE(String clientId, String key, UUID requestId)throws Exception;
	public void KILL() throws Exception;
}
