package org.apache.zookeeper.server.quorum;

import java.net.SocketAddress;
import java.rmi.RemoteException;
import java.util.HashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import mc.zookeeper.NodeIdSetter;

public class FollowerIdMap implements NodeIdSetter {
    
    private static final Logger LOG = LoggerFactory.getLogger(FollowerIdMap.class);
    
    HashMap<SocketAddress, Integer> addrIdMap;
    
    public FollowerIdMap() {
        addrIdMap = new HashMap<SocketAddress, Integer>();
    }

    @Override
    public void setNodeId(SocketAddress address, int id) 
            throws RemoteException {
        addrIdMap.put(address, id);
        LOG.info(addrIdMap.toString());
    }
    
    public int getNodeId(SocketAddress address) {
        return addrIdMap.get(address);
    }

}
