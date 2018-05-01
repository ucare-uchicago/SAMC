package org.apache.zookeeper.server.quorum;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.net.MalformedURLException;
import java.nio.ByteBuffer;
import java.rmi.Naming;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.HashMap;
import java.util.HashSet;

import org.apache.jute.OutputArchive;
import org.apache.zookeeper.server.DataNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import mc.CallbackInterceptor;
import mc.InterceptPacket;
import mc.zookeeper.ZKAspectProperties;
import mc.zookeeper.quorum.QuorumBroadcastCallback;
import mc.zookeeper.quorum.RecordPacket;
import mc.zookeeper.quorum.PrimitivePacket;

public class LeaderQuorumBroadcastCallback implements QuorumBroadcastCallback  {
    
    private static final Logger LOG = LoggerFactory.getLogger(LeaderQuorumBroadcastCallback.class);
    
    private static QuorumBroadcastCallback callbackStub = null;
    
    HashSet<FollowerHandler> followers;
    HashMap<Integer, InterceptPacket> packetMap;
    HashMap<Integer, Integer> packetNodeMap;
    HashMap<Integer, OutputArchive> outputArchiveMap;
    HashMap<Integer, BufferedOutputStream> bufferedOutputMap;
    
    public static synchronized void bindCallback(int id, 
            HashSet<FollowerHandler> followers, 
            HashMap<Integer, InterceptPacket> packetMap,
            HashMap<Integer, Integer> packetNodeMap, 
            HashMap<Integer, OutputArchive> outputArchiveMap, 
            HashMap<Integer, BufferedOutputStream> bufferedOutputMap) 
                    throws RemoteException, MalformedURLException, NotBoundException {
        LOG.info("Creating QuorumBroadcastCallback for this node, id=" + id);
        QuorumBroadcastCallback callback = new LeaderQuorumBroadcastCallback(followers, 
                packetMap, packetNodeMap, outputArchiveMap, bufferedOutputMap);
        callbackStub = (QuorumBroadcastCallback) 
                UnicastRemoteObject.exportObject(callback, 0);
        Registry r = LocateRegistry.getRegistry();
        r.rebind(ZKAspectProperties.getConfig(ZKAspectProperties.INTERCEPTOR_NAME) + 
                "QuorumCallback" + id, callbackStub);
        CallbackInterceptor callbackInceptor = (CallbackInterceptor) 
                Naming.lookup(ZKAspectProperties.getConfig(ZKAspectProperties.INTERCEPTOR_NAME));
        callbackInceptor.registerCallback(id, "QuorumCallback" + id);
    }
    
    public LeaderQuorumBroadcastCallback(HashSet<FollowerHandler> followers,
            HashMap<Integer, InterceptPacket> packetMap,
            HashMap<Integer, Integer> packetNodeMap,
            HashMap<Integer, OutputArchive> outputArchiveMap,
            HashMap<Integer, BufferedOutputStream> bufferedOutputMap) {
        this.followers = followers;
        this.packetMap = packetMap;
        this.packetNodeMap = packetNodeMap;
        this.outputArchiveMap = outputArchiveMap;
        this.bufferedOutputMap = bufferedOutputMap;
    }

    @Override
    public boolean callback(int packetId) throws RemoteException {
        InterceptPacket packet = packetMap.get(packetId);
        if (packet == null) {
            throw new RemoteException("No packet for id " + packetId);
        }
        int followerId = packetNodeMap.get(packetId);
        /*
        boolean isThereReceiverFollower = false;
        FollowerHandler thisFollower = null;
        for (FollowerHandler follower : followers) {
            if (follower.sid == packet.getToId()) {
                isThereReceiverFollower = true;
                thisFollower = follower;
                break;
            }
        }
        */
        OutputArchive outputArchive = outputArchiveMap.get(packetId);
        try {
            LOG.info("Actual sending packet " + packetId);
            if (packet instanceof RecordPacket) {
                RecordPacket recordPacket = (RecordPacket) packet;
                outputArchive.writeRecord(recordPacket.getRecord(), recordPacket.getTag());
                bufferedOutputMap.get(followerId).flush();
                packetMap.remove(packetId);
                packetNodeMap.remove(packetId);
                outputArchiveMap.remove(packetId);
                return true;
            } else if (packet instanceof PrimitivePacket) {
                PrimitivePacket qpPacket = (PrimitivePacket) packet;
                switch (qpPacket.getDataType()) {
                case PrimitivePacket.STRING:
                    outputArchive.writeString(new String(qpPacket.getData()), qpPacket.getTag());
                    break;
                case PrimitivePacket.BYTE:
                    outputArchive.writeByte(qpPacket.getData()[0], qpPacket.getTag());
                    break;
                case PrimitivePacket.BOOLEAN:
                    outputArchive.writeBool(qpPacket.getData()[0] == 1 ? true : false, qpPacket.getTag());
                    break;
                default:
                    ByteBuffer buff = ByteBuffer.wrap(qpPacket.getData());
                    switch (qpPacket.getDataType()) {
                    case PrimitivePacket.INTEGER:
                    case PrimitivePacket.VECTOR:
                    case PrimitivePacket.MAP:
                        outputArchive.writeInt(buff.getInt(), qpPacket.getTag());
                        break;
                    case PrimitivePacket.LONG:
                        outputArchive.writeLong(buff.getLong(), qpPacket.getTag());
                        break;
                    case PrimitivePacket.FLOAT:
                        outputArchive.writeFloat(buff.getFloat(), qpPacket.getTag());
                        break;
                    case PrimitivePacket.DOUBLE:
                        outputArchive.writeDouble(buff.getDouble(), qpPacket.getTag());
                        break;
                    default:
                        return false;
                    }
                }
                bufferedOutputMap.get(followerId).flush();
                packetMap.remove(packetId);
                return true;
            } else {
                return false;
            }
        } catch (IOException e) {
            throw new RemoteException(e.getMessage());
        }
    }

}
