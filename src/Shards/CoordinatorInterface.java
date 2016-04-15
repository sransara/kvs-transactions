package Shards;
import java.rmi.Remote;

import Utility.UtilityClasses.*;

public interface CoordinatorInterface extends Remote {

	public Response Join(JoinArgs joinArgs) throws Exception;
	public Response Query(QueryArgs queryArgs)throws Exception;
	public Response Leave(LeaveArgs leaveArgs) throws Exception;
	public Response Move(MoveArgs moveArgs)throws Exception;
	public void KILL() throws Exception;
	
}
