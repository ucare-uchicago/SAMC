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

import org.apache.jute.OutputArchive;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import mc.CallbackInterceptor;
import mc.zookeeper.ZKAspectProperties;
import mc.zookeeper.quorum.InterceptQuorumPacket;
import mc.zookeeper.quorum.PrimitivePacket;
import mc.zookeeper.quorum.QuorumBroadcastCallback;
import mc.zookeeper.quorum.RecordPacket;

public class FollowerQuorumBroadcastCallback implements QuorumBroadcastCallback {
    
    private static final Logger LOG = LoggerFactory.getLogger(FollowerQuorumBroadcastCallback.class);

    private static QuorumBroadcastCallback callbackStub = null;

    HashMap<Integer, InterceptQuorumPacket> packetMap;
    OutputArchive leaderOutput;
    BufferedOutputStream bufferedOutput;
    
    private FollowerQuorumBroadcastCallback(HashMap<Integer, InterceptQuorumPacket> packetMap, 
            OutputArchive leaderOutput, BufferedOutputStream bufferedOutput) throws RemoteException {
        this.packetMap = packetMap;
        this.leaderOutput = leaderOutput;
        this.bufferedOutput = bufferedOutput;
    }
    
    public static synchronized void bindCallback(int id, 
            HashMap<Integer, InterceptQuorumPacket> packetMap, OutputArchive leaderOutput, 
            BufferedOutputStream bufferedOutput) 
                    throws RemoteException, MalformedURLException, NotBoundException {
        LOG.info("Creating QuorumBroadcastCallback for this node, id=" + id);
        QuorumBroadcastCallback callback = 
                new FollowerQuorumBroadcastCallback(packetMap, leaderOutput, bufferedOutput);
        callbackStub = (QuorumBroadcastCallback) 
                UnicastRemoteObject.exportObject(callback, 0);
        Registry r = LocateRegistry.getRegistry();
        r.rebind(ZKAspectProperties.getConfig(ZKAspectProperties.INTERCEPTOR_NAME) + 
                "QuorumCallback" + id, callbackStub);
        CallbackInterceptor callbackInceptor = (CallbackInterceptor) 
                Naming.lookup(ZKAspectProperties.getConfig(ZKAspectProperties.INTERCEPTOR_NAME));
        callbackInceptor.registerCallback(id, "QuorumCallback" + id);
    }

    @Override
    public boolean callback(int packetId) throws RemoteException {
        InterceptQuorumPacket packet = packetMap.get(packetId);
        if (packet == null) {
            throw new RemoteException("No quorum packet id " + packetId);
        }
        try {
            LOG.info("Actual sending packet " + packetId);
            if (packet instanceof RecordPacket) {
                RecordPacket recordPacket = (RecordPacket) packet;
                leaderOutput.writeRecord(recordPacket.getRecord(), recordPacket.getTag());
                bufferedOutput.flush();
                packetMap.remove(packetId);
                return true;
            } else if (packet instanceof PrimitivePacket) {
                PrimitivePacket qpPacket = (PrimitivePacket) packet;
                switch (qpPacket.getDataType()) {
                case PrimitivePacket.STRING:
                    leaderOutput.writeString(new String(qpPacket.getData()), qpPacket.getTag());
                    break;
                case PrimitivePacket.BYTE:
                    leaderOutput.writeByte(qpPacket.getData()[0], qpPacket.getTag());
                    break;
                case PrimitivePacket.BOOLEAN:
                    leaderOutput.writeBool(qpPacket.getData()[0] == 1 ? true : false, qpPacket.getTag());
                    break;
                default:
                    ByteBuffer buff = ByteBuffer.wrap(qpPacket.getData());
                    switch (qpPacket.getDataType()) {
                    case PrimitivePacket.INTEGER:
                    case PrimitivePacket.VECTOR:
                    case PrimitivePacket.MAP:
                        leaderOutput.writeInt(buff.getInt(), qpPacket.getTag());
                        break;
                    case PrimitivePacket.LONG:
                        leaderOutput.writeLong(buff.getLong(), qpPacket.getTag());
                        break;
                    case PrimitivePacket.FLOAT:
                        leaderOutput.writeFloat(buff.getFloat(), qpPacket.getTag());
                        break;
                    case PrimitivePacket.DOUBLE:
                        leaderOutput.writeDouble(buff.getDouble(), qpPacket.getTag());
                        break;
                    default:
                        return false;
                    }
                }
                bufferedOutput.flush();
                return true;
            } else {
                return false;
            }
        } catch (IOException e) {
            throw new RemoteException(e.getMessage());
        }
    }

}
