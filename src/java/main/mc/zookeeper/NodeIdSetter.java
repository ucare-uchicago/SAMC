package mc.zookeeper;

import java.net.SocketAddress;
import java.rmi.Remote;
import java.rmi.RemoteException;

public interface NodeIdSetter extends Remote {
    
    public void setNodeId(SocketAddress address, int id) throws RemoteException;

}
