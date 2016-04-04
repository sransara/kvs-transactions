package project4;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.Base64;
import java.util.UUID;

public class UtilityClasses {
	
	 /** Write the object to a Base64 string. */
    private static String toString( Serializable o ) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ObjectOutputStream oos = new ObjectOutputStream( baos );
        oos.writeObject( o );
        oos.close();
        return Base64.getEncoder().encodeToString(baos.toByteArray()); 
    }


    /** Read the object from Base64 string. */
   private static Object fromString( String s ) throws IOException ,
                                                       ClassNotFoundException {
        byte [] data = Base64.getDecoder().decode( s );
        ObjectInputStream ois = new ObjectInputStream( 
                                        new ByteArrayInputStream(  data ) );
        Object o  = ois.readObject();
        ois.close();
        return o;
   }
   public static Operation decodeOperation(String s) throws ClassNotFoundException, IOException
   {
	   Operation operation = (Operation) fromString(s);
	   return operation;
   }
   public static String encodeOperation(Operation operation) throws IOException
   {
	   return toString(operation);
   }
   
	public static class PrepareArgs implements Serializable
	{
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
	
	public static class HostPorts implements Serializable
	{
		/**
		 * 
		 */
		private static final long serialVersionUID = 1L;
		String hostName;
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
	public static class PrepareReply implements Serializable
	{
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
	public static class AcceptArgs implements Serializable
	{
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
	
	public static class AcceptReply implements Serializable
	{
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
	
	public static class LearnArgs implements Serializable
	{
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
	
	public static class LearnReply implements Serializable
	{
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

	public static class AcceptorState implements Serializable
	{
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

	
	public static class Status implements Serializable
	{ 
		/**
		 * 
		 */
		private static final long serialVersionUID = 1L;
		String value;
		boolean done;
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
	
	public static class Response implements Serializable
	{ 
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
		public Response(String value, boolean done) {
			super();
			this.value = value;
			this.done = done;
		}
		
	}
	
	
	public static class Operation implements Serializable{
		 /**
		 * 
		 */
		private static final long serialVersionUID = 1L;
		public String type;
		 public String key;
		 public String value;
		 public String from;
		 public UUID requestId;
		public Operation(String type, String key, String value, String from,
				UUID requestId) {
			super();
			this.type = type;
			this.key = key;
			this.value = value;
			this.from = from;
			this.requestId = requestId;
		}
		@Override
		public String toString() {
			return "Operation [" + type +" "+ key +
					" " + value + ", fromClient=" + from + ", requestId=" + requestId
					+ "]";
		}
		
	}
		
}
