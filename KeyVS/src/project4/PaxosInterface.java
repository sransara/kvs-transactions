package project4;
import java.rmi.*;

import project4.UtilityClasses.*;
public interface PaxosInterface extends Remote {
	
	public PrepareReply Prepare(PrepareArgs prepareArgs) throws RemoteException, InterruptedException;
	public AcceptReply Accept(AcceptArgs acceptArgs) throws RemoteException, InterruptedException;
	public LearnReply Learn(LearnArgs LearnArgs) throws RemoteException,InterruptedException;	
}
