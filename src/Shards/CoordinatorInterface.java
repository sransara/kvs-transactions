package Shards;

import java.rmi.Remote;

import Utility.UtilityClasses.*;

public interface CoordinatorInterface extends Remote {

    public ShardReply Join(JoinArgs joinArgs) throws Exception;

    public ShardReply Leave(LeaveArgs leaveArgs) throws Exception;

    public ShardReply Poll(PollArgs pollArgs) throws Exception;
}
