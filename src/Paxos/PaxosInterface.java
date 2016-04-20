package Paxos;
import java.rmi.*;

import Utility.UtilityClasses.*;
public interface PaxosInterface extends Remote {
	
	public PrepareReply Prepare(PrepareArgs prepareArgs) throws RemoteException, InterruptedException;
	public AcceptReply Accept(AcceptArgs acceptArgs) throws RemoteException, InterruptedException;
	public LearnReply Learn(LearnArgs LearnArgs) throws RemoteException,InterruptedException;	
}
