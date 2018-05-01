package org.apache.zookeeper.server.quorum;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.lang.Thread.UncaughtExceptionHandler;
import java.net.MalformedURLException;
import java.rmi.Naming;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.TreeMap;

import mc.InterceptPacket;
import mc.Interceptor;
import mc.PacketReceiveAck;
import mc.SteadyStateListener;
import mc.tools.StackTraceLogger;
import mc.zookeeper.NodeIdSetter;
import mc.zookeeper.ZKAspectProperties;
import mc.zookeeper.quorum.QuorumPacketGenerator;
import mc.zookeeper.quorum.RecordPacket;
import mc.zookeeper.quorum.PrimitivePacket;

import org.apache.jute.BinaryInputArchive;
import org.apache.jute.BinaryOutputArchive;
import org.apache.jute.InputArchive;
import org.apache.jute.OutputArchive;
import org.apache.jute.Record;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public aspect QuorumBroadcastLeaderAspect extends ZooKeeperIdAwareAspect {

    private static final Logger LOG = LoggerFactory.getLogger(QuorumBroadcastLeaderAspect.class);
    
    static {
        Thread.currentThread().setUncaughtExceptionHandler(new UncaughtExceptionHandler() {
            @Override
            public void uncaughtException(Thread t, Throwable e) {
                LOG.error(e.toString());
            }
        });
    }
    
    HashSet<FollowerHandler> followers;
    HashSet<FollowerHandler> followerWaitingResponses;
    HashMap<Integer, InterceptPacket> packetMap;
    HashMap<Integer, Integer> packetNodeMap;
    HashMap<Integer, OutputArchive> outputArchiveMap;
    HashMap<OutputArchive, Integer> outputArchiveNodeMap;
    HashMap<Integer, BufferedOutputStream> bufferedOutputMap;
    Interceptor quorumBroadcastInceptor;
    QuorumPacketGenerator packetGen;
    SteadyStateListener quorumBroadcastSteadyStateListener;
    PacketReceiveAck ack;
    NodeIdSetter followerIdMap;
    
    boolean isLastMessagePing;
    
    public QuorumBroadcastLeaderAspect() {
        try {
            packetGen = new QuorumPacketGenerator();
            packetMap = new HashMap<Integer, InterceptPacket>();
            packetNodeMap = new HashMap<Integer, Integer>();
            outputArchiveMap = new HashMap<Integer, OutputArchive>();
            bufferedOutputMap = new HashMap<Integer, BufferedOutputStream>();
            outputArchiveNodeMap = new HashMap<OutputArchive, Integer>();
            quorumBroadcastInceptor = (Interceptor) Naming.lookup(
                    ZKAspectProperties.getConfig(ZKAspectProperties.INTERCEPTOR_NAME));
            ack = (PacketReceiveAck) Naming.lookup(ZKAspectProperties.getConfig(
                    ZKAspectProperties.INTERCEPTOR_NAME) + "Ack");
            quorumBroadcastSteadyStateListener = (SteadyStateListener) Naming.lookup(
                    ZKAspectProperties.getConfig(ZKAspectProperties.INTERCEPTOR_NAME) + 
                    "SteadyState");
            followerWaitingResponses = new HashSet<FollowerHandler>();
            followerIdMap = new FollowerIdMap();
            isLastMessagePing = false;
        } catch (Exception e) {
            LOG.error(e.getMessage());
        }
    }
    
    pointcut setMyId(long id) : set(long QuorumPeer.myid) && args(id);
    
    after(long id) : setMyId(id) {
        NodeIdSetter idSetterStub;
        try {
            idSetterStub = (NodeIdSetter) 
                    UnicastRemoteObject.exportObject(followerIdMap, 0);
                Registry r = LocateRegistry.getRegistry();
                r.rebind(ZKAspectProperties.getConfig(ZKAspectProperties.INTERCEPTOR_NAME) + 
                        "QuorumFollowerIdSetter" + id, idSetterStub);
        } catch (RemoteException e) {
            LOG.error("", e);
        }
    }

    pointcut setFollowerSet(HashSet<FollowerHandler> followers) : 
        set(HashSet<FollowerHandler> Leader.followers) && args(followers);
    
    before(HashSet<FollowerHandler> followers) : setFollowerSet(followers) {
        this.followers = followers;
        try {
            LeaderQuorumBroadcastCallback.bindCallback(id, followers, packetMap, 
                    packetNodeMap, outputArchiveMap, bufferedOutputMap);
        } catch (RemoteException e) {
            LOG.error("", e);
        } catch (MalformedURLException e) {
            LOG.error("", e);
        } catch (NotBoundException e) {
            LOG.error("", e);
        }
    }
    
    pointcut setOutputArchive(FollowerHandler follower, BinaryOutputArchive oa) : 
        set(BinaryOutputArchive FollowerHandler.oa) && this(follower) && args(oa);
    
    before(FollowerHandler follower, BinaryOutputArchive oa) : setOutputArchive(follower, oa) {
        FollowerIdMap idMap = (FollowerIdMap) followerIdMap;
        int followerId = -1;
        while (true) {
            try {
                followerId = idMap.getNodeId(follower.sock.getRemoteSocketAddress());
            } catch (Exception e) {
            }
            if (followerId != -1) {
                break;
            }
            try {
                Thread.sleep(50);
            } catch (InterruptedException e1) {
                LOG.error(e1.toString());
            }
        }
        outputArchiveNodeMap.put(oa, followerId);
    }
    
    pointcut setBufferedOutput(FollowerHandler follower, BufferedOutputStream bufferedOutput) : 
        set(BufferedOutputStream FollowerHandler.bufferedOutput) &&
        this(follower) && args(bufferedOutput);
    
    before(FollowerHandler follower, BufferedOutputStream bufferedOutput) : setBufferedOutput(follower, bufferedOutput) {
        FollowerIdMap idMap = (FollowerIdMap) followerIdMap;
        int followerId = -1;
        while (true) {
            try {
                followerId = idMap.getNodeId(follower.sock.getRemoteSocketAddress());
            } catch (Exception e) {
            }
            if (followerId != -1) {
                break;
            }
            try {
                Thread.sleep(50);
            } catch (InterruptedException e1) {
                LOG.error(e1.toString());
            }
        }
        bufferedOutputMap.put(followerId, bufferedOutput);
    }
    
    pointcut writeRecordToFollower(OutputArchive output, Record data, String tag) : 
        call(public void Record.serialize(OutputArchive, String) throws IOException) && target(data) && args(output, tag) &&
        !within(FollowerQuorumBroadcastCallback) && !within(LeaderQuorumBroadcastCallback) && !within(Record+) && 
        (cflowbelow(execution(public void FollowerHandler.run())) || 
                cflowbelow(execution(private void FollowerHandler.sendPackets() throws InterruptedException)));// ||
//                cflowbelow(execution(public Proposal Leader.propose(Request))));
    
    pointcut writeByteToFollower(OutputArchive output, byte data, String tag) : 
        call(public void OutputArchive.writeByte(byte, String) throws IOException) && target(output) && args(data, tag) &&
        !within(FollowerQuorumBroadcastCallback) && !within(LeaderQuorumBroadcastCallback) && !within(Record+) && !within(OutputArchive+) &&
        (cflowbelow(execution(public void FollowerHandler.run())));
    
    pointcut writeBoolToFollower(OutputArchive output, boolean data, String tag) : 
        call(public void OutputArchive.writeLong(boolean, String) throws IOException) && target(output) && args(data, tag) &&
        !within(FollowerQuorumBroadcastCallback) && !within(LeaderQuorumBroadcastCallback) && !within(Record+) && !within(OutputArchive+) &&
        (cflowbelow(execution(public void FollowerHandler.run())));
     
    pointcut writeIntToFollower(OutputArchive output, int data, String tag) : 
        call(public void OutputArchive.writeInt(int, String) throws IOException) && target(output) && args(data, tag) &&
        !within(FollowerQuorumBroadcastCallback) && !within(LeaderQuorumBroadcastCallback) && !within(Record+) && !within(OutputArchive+) &&
        (cflowbelow(execution(public void FollowerHandler.run())));
    
    pointcut writeLongToFollower(OutputArchive output, long data, String tag) : 
        call(public void OutputArchive.writeLong(long, String) throws IOException) && target(output) && args(data, tag) &&
        !within(FollowerQuorumBroadcastCallback) && !within(LeaderQuorumBroadcastCallback) && !within(Record+) && !within(OutputArchive+) &&
        (cflowbelow(execution(public void FollowerHandler.run())));
    
    pointcut writeFloatToFollower(OutputArchive output, float data, String tag) : 
        call(public void OutputArchive.writeFloat(float, String) throws IOException) && target(output) && args(data, tag) &&
        !within(FollowerQuorumBroadcastCallback) && !within(LeaderQuorumBroadcastCallback) && !within(Record+) && !within(OutputArchive+) &&
        (cflowbelow(execution(public void FollowerHandler.run())));
    
    pointcut writeDoubleToFollower(OutputArchive output, double data, String tag) : 
        call(public void OutputArchive.writeDouble(double, String) throws IOException) && target(output) && args(data, tag) &&
        !within(FollowerQuorumBroadcastCallback) && !within(LeaderQuorumBroadcastCallback) && !within(Record+) && !within(OutputArchive+) &&
        (cflowbelow(execution(public void FollowerHandler.run())));
    
    pointcut writeStringToFollower(OutputArchive output, String data, String tag) : 
        call(public void OutputArchive.writeString(String, String) throws IOException) && target(output) && args(data, tag) &&
        !within(FollowerQuorumBroadcastCallback) && !within(LeaderQuorumBroadcastCallback) && !within(Record+) && !within(OutputArchive+) &&
        (cflowbelow(execution(public void FollowerHandler.run())));
    
    pointcut writeVectorToFollower(OutputArchive output, List data, String tag) : 
        call(public void OutputArchive.startVector(List, String) throws IOException) && target(output) && args(data, tag) &&
        !within(FollowerQuorumBroadcastCallback) && !within(LeaderQuorumBroadcastCallback) && !within(Record+) && !within(OutputArchive+) &&
        (cflowbelow(execution(public void FollowerHandler.run())));

    pointcut writeMapToFollower(OutputArchive output, TreeMap data, String tag) : 
        call(public void OutputArchive.startMap(TreeMap, String) throws IOException) && target(output) && args(data, tag) &&
        !within(FollowerQuorumBroadcastCallback) && !within(LeaderQuorumBroadcastCallback) && !within(Record+) && !within(OutputArchive+) &&
        (cflowbelow(execution(public void FollowerHandler.run())));
    
    void around(OutputArchive output, Record data, String tag) : writeRecordToFollower(output, data, tag) {
        if (data instanceof QuorumPacket) {
            QuorumPacket p = (QuorumPacket) data;
            if (p.getType() == 5) {
                proceed(output, data, tag);
                return;
            }
        }
        int toId = outputArchiveNodeMap.get(output);
        RecordPacket packetWithId = packetGen.createNewRecordPacket("QuorumCallback" + id, id, toId, data, tag);
        packetMap.put(packetWithId.getId(), packetWithId);
        outputArchiveMap.put(packetWithId.getId(), output);
        packetNodeMap.put(packetWithId.getId(), toId);
        try {
            quorumBroadcastInceptor.offerPacket(packetWithId);
        } catch (RemoteException e) {
            LOG.error("", e);
        } 
    }

    void around(OutputArchive output, byte data, String tag) : writeByteToFollower(output, data, tag) {
        int toId = outputArchiveNodeMap.get(output);
        PrimitivePacket bytePacket = packetGen.createNewQuorumPrimitivePacket("QuorumCallback" + id, id, toId, data, tag);
        packetMap.put(bytePacket.getId(), bytePacket);
        outputArchiveMap.put(bytePacket.getId(), output);
        packetNodeMap.put(bytePacket.getId(), toId);
        try {
            quorumBroadcastInceptor.offerPacket(bytePacket);
        } catch (RemoteException e) {
            LOG.error("", e);
        } 
    }
    
    void around(OutputArchive output, boolean data, String tag) : writeBoolToFollower(output, data, tag) {
        int toId = outputArchiveNodeMap.get(output);
        PrimitivePacket boolPacket = packetGen.createNewQuorumPrimitivePacket("QuorumCallback" + id, id, toId, data, tag);
        packetMap.put(boolPacket.getId(), boolPacket);
        outputArchiveMap.put(boolPacket.getId(), output);
        packetNodeMap.put(boolPacket.getId(), toId);
        try {
            quorumBroadcastInceptor.offerPacket(boolPacket);
        } catch (RemoteException e) {
            LOG.error("", e);
        } 
    } 

    void around(OutputArchive output, int data, String tag) : writeIntToFollower(output, data, tag) {
        int toId = outputArchiveNodeMap.get(output);
        PrimitivePacket intPacket = packetGen.createNewQuorumPrimitivePacket("QuorumCallback" + id, id, toId, data, tag);
        packetMap.put(intPacket.getId(), intPacket);
        outputArchiveMap.put(intPacket.getId(), output);
        packetNodeMap.put(intPacket.getId(), toId);
        try {
            quorumBroadcastInceptor.offerPacket(intPacket);
        } catch (RemoteException e) {
            LOG.error("", e);
        } 
    }
    
    void around(OutputArchive output, long data, String tag) : writeLongToFollower(output, data, tag) {
        int toId = outputArchiveNodeMap.get(output);
        PrimitivePacket longPacket = packetGen.createNewQuorumPrimitivePacket("QuorumCallback" + id, id, toId, data, tag);
        packetMap.put(longPacket.getId(), longPacket);
        outputArchiveMap.put(longPacket.getId(), output);
        packetNodeMap.put(longPacket.getId(), toId);
        try {
            quorumBroadcastInceptor.offerPacket((longPacket));
        } catch (RemoteException e) {
            LOG.error("", e);
        } 
    }

    void around(OutputArchive output, float data, String tag) : writeFloatToFollower(output, data, tag) {
        int toId = outputArchiveNodeMap.get(output);
        PrimitivePacket floatPacket = packetGen.createNewQuorumPrimitivePacket("QuorumCallback" + id, id, toId, data, tag);
        packetMap.put(floatPacket.getId(), floatPacket);
        outputArchiveMap.put(floatPacket.getId(), output);
        packetNodeMap.put(floatPacket.getId(), toId);
        try {
            quorumBroadcastInceptor.offerPacket((floatPacket));
        } catch (RemoteException e) {
            LOG.error("", e);
        } 
    }
    
    void around(OutputArchive output, double data, String tag) : writeDoubleToFollower(output, data, tag) {
        int toId = outputArchiveNodeMap.get(output);
        PrimitivePacket doublePacket = packetGen.createNewQuorumPrimitivePacket("QuorumCallback" + id, id, toId, data, tag);
        packetMap.put(doublePacket.getId(), doublePacket);
        outputArchiveMap.put(doublePacket.getId(), output);
        packetNodeMap.put(doublePacket.getId(), toId);
        try {
            quorumBroadcastInceptor.offerPacket((doublePacket));
        } catch (RemoteException e) {
            LOG.error("", e);
        } 
    }

    void around(OutputArchive output, String data, String tag) : writeStringToFollower(output, data, tag) {
        int toId = outputArchiveNodeMap.get(output);
        PrimitivePacket stringPacket = packetGen.createNewQuorumPrimitivePacket("QuorumCallback" + id, id, toId, data, tag);
        packetMap.put(stringPacket.getId(), stringPacket);
        outputArchiveMap.put(stringPacket.getId(), output);
        packetNodeMap.put(stringPacket.getId(), toId);
        try {
            quorumBroadcastInceptor.offerPacket(stringPacket);
        } catch (RemoteException e) {
            LOG.error("", e);
        } 
    }

    void around(OutputArchive output, List data, String tag) : writeVectorToFollower(output, data, tag) {
        int toId = outputArchiveNodeMap.get(output);
        PrimitivePacket stringPacket = packetGen.createNewQuorumPrimitivePacket("QuorumCallback" + id, id, toId, data, tag);
        packetMap.put(stringPacket.getId(), stringPacket);
        outputArchiveMap.put(stringPacket.getId(), output);
        packetNodeMap.put(stringPacket.getId(), toId);
        try {
            quorumBroadcastInceptor.offerPacket((stringPacket));
        } catch (RemoteException e) {
            LOG.error("", e);
        } 
    }

    void around(OutputArchive output, TreeMap data, String tag) : writeMapToFollower(output, data, tag) {
        int toId = outputArchiveNodeMap.get(output);
        PrimitivePacket stringPacket = packetGen.createNewQuorumPrimitivePacket("QuorumCallback" + id, id, toId, data, tag);
        packetMap.put(stringPacket.getId(), stringPacket);
        outputArchiveMap.put(stringPacket.getId(), output);
        packetNodeMap.put(stringPacket.getId(), toId);
        try {
            quorumBroadcastInceptor.offerPacket((stringPacket));
        } catch (RemoteException e) {
            LOG.error("", e);
        } 
    }

    pointcut readRecordFromFollower(InputArchive input, Record data, String tag) : 
        call(public void Record.deserialize(InputArchive, String) throws IOException) && target(data) && args(input, tag) &&
        !within(Record+) && (cflowbelow(execution(public void FollowerHandler.run())));
    
    pointcut readByteFromFollower(InputArchive input) : call(public byte InputArchive.readByte(String) throws IOException) && 
        !within(Record+) && !within(InputArchive+) && args(input) &&
        (cflowbelow(execution(public void FollowerHandler.run())));
    
    pointcut readBoolFromFollower(InputArchive input) : call(public boolean InputArchive.readBool(String) throws IOException) && 
        !within(Record+) && !within(InputArchive+) && args(input) &&
        (cflowbelow(execution(public void FollowerHandler.run())));
    
    pointcut readIntFromFollower(InputArchive input) : call(public int InputArchive.readInt(String) throws IOException) && 
        !within(Record+) && !within(InputArchive+) && args(input) &&
        (cflowbelow(execution(public void FollowerHandler.run())));
    
    pointcut readLongFromFollower(InputArchive input) : call(public long InputArchive.readLong(String) throws IOException) && 
        !within(Record+) && !within(InputArchive+) && args(input) &&
        (cflowbelow(execution(public void FollowerHandler.run())));
    
    pointcut readFloatFromFollower(InputArchive input) : call(public float InputArchive.readFloat(String) throws IOException) && 
        !within(Record+) && !within(InputArchive+) && args(input) &&
        (cflowbelow(execution(public void FollowerHandler.run())));
    
    pointcut readDoubleFromFollower(InputArchive input) : call(public double InputArchive.readDouble(String) throws IOException) && 
        !within(Record+) && !within(InputArchive+) && args(input) &&
        (cflowbelow(execution(public void FollowerHandler.run())));
    
    pointcut readStringFromFollower(InputArchive input) : call(public String InputArchive.readString(String) throws IOException) && 
        !within(Record+) && !within(InputArchive+) && args(input) &&
        (cflowbelow(execution(public void FollowerHandler.run())));
    
    pointcut readListFromFollower(InputArchive input) : call(public List InputArchive.startVector(String) throws IOException) && 
        !within(Record+) && !within(InputArchive+) && args(input) &&
        (cflowbelow(execution(public void FollowerHandler.run())));
    
    pointcut readMapFromFollower(InputArchive input) : call(public Map InputArchive.startMap(String) throws IOException) && 
        !within(Record+) && !within(InputArchive+) && args(input) &&
        (cflowbelow(execution(public void FollowerHandler.run())));
    
    before(InputArchive input) : readByteFromFollower(input) || readBoolFromFollower(input) || readIntFromFollower(input) || 
        readLongFromFollower(input) || readFloatFromFollower(input) || readDoubleFromFollower(input) || 
        readStringFromFollower(input) || readListFromFollower(input) || readMapFromFollower(input) {
        try {
            LOG.info("Being in steady state");
            quorumBroadcastSteadyStateListener.informSteadyState(id, 0);
         } catch (RemoteException e) {
            LOG.error(e.getMessage());
         }
    }

    after(InputArchive input) : readByteFromFollower(input) || readBoolFromFollower(input) || readIntFromFollower(input) || 
        readLongFromFollower(input) || readFloatFromFollower(input) || readDoubleFromFollower(input) || 
        readStringFromFollower(input) || readListFromFollower(input) || readMapFromFollower(input) {
        if (ack != null) {
            try {
                LOG.info("Acking back for packet " + 112);
                ack.ack(112, id);
            } catch (Exception e) {
                LOG.error(e.getMessage());
            }
        }
    }
    
    before(InputArchive input, Record data, String tag) : readRecordFromFollower(input, data, tag) {
        if (!isLastMessagePing) {
            try {
                LOG.info("Being in steady state");
                quorumBroadcastSteadyStateListener.informSteadyState(id, 0);
            } catch (RemoteException e) {
                LOG.error(e.getMessage());
            }
        }
    }

    after(InputArchive input, Record data, String tag) returning : readRecordFromFollower(input, data, tag) {
        if (data instanceof QuorumPacket) {
            QuorumPacket p = (QuorumPacket) data;
            if (p.getType() == 5) {
                isLastMessagePing = true;
                return;
            }
        }
        isLastMessagePing = false;
        if (ack != null) {
            try {
                LOG.info("Acking back for packet " + 112);
                ack.ack(112, id);
            } catch (Exception e) {
                LOG.error(e.getMessage());
            }
        }
    }
    
    pointcut afterPreCondition() : execution(public void FollowerHandler.afterPreCondition());
    
    after() : afterPreCondition() {
    }
    
}
