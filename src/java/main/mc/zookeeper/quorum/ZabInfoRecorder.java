package mc.zookeeper.quorum;

import java.rmi.Remote;
import java.rmi.RemoteException;

import org.apache.zookeeper.server.quorum.QuorumPeer.ServerState;

public interface ZabInfoRecorder extends Remote {
    
    public void setRole(int id, ServerState role) throws RemoteException;
    public void setLatestTxId(int id, long txId) throws RemoteException;
    public void setMaxCommittedLog(int id, long maxCommittedLog) throws RemoteException;

}
