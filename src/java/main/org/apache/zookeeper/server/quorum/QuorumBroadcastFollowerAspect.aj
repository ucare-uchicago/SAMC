package org.apache.zookeeper.server.quorum;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.lang.Thread.UncaughtExceptionHandler;
import java.net.MalformedURLException;
import java.net.Socket;
import java.net.SocketAddress;
import java.rmi.Naming;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import mc.Interceptor;
import mc.PacketReceiveAck;
import mc.SteadyStateListener;
import mc.tools.StackTraceLogger;
import mc.zookeeper.NodeIdSetter;
import mc.zookeeper.ZKAspectProperties;
import mc.zookeeper.quorum.InterceptQuorumPacket;
import mc.zookeeper.quorum.PrimitivePacket;
import mc.zookeeper.quorum.QuorumPacketGenerator;
import mc.zookeeper.quorum.RecordPacket;

import org.apache.jute.InputArchive;
import org.apache.jute.OutputArchive;
import org.apache.jute.Record;
import org.apache.zookeeper.server.Request;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public aspect QuorumBroadcastFollowerAspect extends ZooKeeperIdAwareAspect {
    
    private static final Logger LOG = LoggerFactory.getLogger(QuorumBroadcastFollowerAspect.class);
    
    static {
        Thread.currentThread().setUncaughtExceptionHandler(new UncaughtExceptionHandler() {
            @Override
            public void uncaughtException(Thread t, Throwable e) {
                LOG.error(e.toString());
            }
        });
    }
    
    QuorumPacketGenerator packetGen;
    HashMap<Integer, InterceptQuorumPacket> packetMap;
    Interceptor quorumBroadcastInceptor;
    InputArchive leaderInput;
    OutputArchive leaderOutput;
    BufferedOutputStream bufferedOutput;
    PacketReceiveAck ack;
    SteadyStateListener quorumBroadcastSteadyStateListener;
    int leaderId;
    boolean isFollower;
    
    boolean isLastMessagePing;
    
    public QuorumBroadcastFollowerAspect() {
        try {
            quorumBroadcastInceptor = (Interceptor) Naming.lookup(
                    ZKAspectProperties.getConfig(ZKAspectProperties.INTERCEPTOR_NAME));
            packetGen = new QuorumPacketGenerator();
            packetMap = new HashMap<Integer, InterceptQuorumPacket>();
            leaderId = -1;
            isFollower = false;
            ack = (PacketReceiveAck) Naming.lookup(ZKAspectProperties.getConfig(
                    ZKAspectProperties.INTERCEPTOR_NAME) + "Ack");
            quorumBroadcastSteadyStateListener = (SteadyStateListener) Naming.lookup(
                    ZKAspectProperties.getConfig(ZKAspectProperties.INTERCEPTOR_NAME) + 
                    "SteadyState");
            isLastMessagePing = false;
        } catch (Exception e) {
            LOG.error(e.getMessage());
        } 
    }
    
    pointcut setLeader(Vote currentVote) : set(Vote QuorumPeer.currentVote) && 
        args(currentVote);
    
    before(Vote currentVote) : setLeader(currentVote) {
        leaderId = (int) currentVote.id;
    }
    
    pointcut connectToLeader(Socket socket) : 
        call(public void Socket.connect(SocketAddress, int) throws IOException) && 
        withincode(void Follower.followLeader() throws InterruptedException) && target(socket);
    
    after(Socket socket) : connectToLeader(socket) {
        try {
            NodeIdSetter idSetter = (NodeIdSetter) Naming.lookup(ZKAspectProperties.getConfig(
                    ZKAspectProperties.INTERCEPTOR_NAME) + "QuorumFollowerIdSetter" + leaderId);
            SocketAddress localAddress = socket.getLocalSocketAddress();
            if (localAddress != null) {
                idSetter.setNodeId(localAddress, id);
            }
        } catch (RemoteException e) {
            LOG.error(e.toString());
        } catch (MalformedURLException e) {
            LOG.error(e.toString());
        } catch (NotBoundException e) {
            LOG.error(e.toString());
        }
    }

    pointcut setLeaderBufferedOutput(BufferedOutputStream bufferedOutput) : 
        set(BufferedOutputStream Follower.bufferedOutput) && args(bufferedOutput);
    
    before(BufferedOutputStream bufferedOutput) : setLeaderBufferedOutput(bufferedOutput) {
        this.bufferedOutput = bufferedOutput;
    }

    pointcut setLeaderOutput(OutputArchive leaderOutput) : set(OutputArchive Follower.leaderOs) && 
        args(leaderOutput);
    
    before(OutputArchive leaderOutput) : setLeaderOutput(leaderOutput) {
        try {
            this.leaderOutput = leaderOutput;
            FollowerQuorumBroadcastCallback.bindCallback(id, packetMap, leaderOutput, bufferedOutput);
        } catch (RemoteException e) {
            LOG.error(e + "");
        } catch (MalformedURLException e) {
            LOG.error(e + "");
        } catch (NotBoundException e) {
            LOG.error(e + "");
        }
    }
    
    pointcut setLeaderInput(InputArchive leaderInput) : set(InputArchive Follower.leaderIs) && 
        args(leaderInput);
    
    before(InputArchive leaderInput) : setLeaderInput(leaderInput) {
        this.leaderInput = leaderInput;
    }

    pointcut writeRecordToLeader(OutputArchive output, Record data, String tag) : 
        call(public void Record.serialize(OutputArchive, String) throws IOException) && target(data) && args(output, tag) &&
        !within(FollowerQuorumBroadcastCallback) && !within(LeaderQuorumBroadcastCallback) && !within(Record+) && 
        (cflowbelow(execution(void Follower.followLeader() throws InterruptedException)) || 
                cflowbelow(execution(void Follower.request(Request) throws IOException)) ||
                cflowbelow(execution(public void SendAckRequestProcessor.processRequest(Request))));
    
    pointcut writeByteToLeader(OutputArchive output, byte data, String tag) : 
        call(public void OutputArchive.writeByte(byte, String) throws IOException) && target(output) && args(data, tag) &&
        !within(FollowerQuorumBroadcastCallback) && !within(LeaderQuorumBroadcastCallback) && !within(Record+) && !within(OutputArchive+) &&
        (cflowbelow(execution(void Follower.followLeader() throws InterruptedException)) || 
                cflowbelow(execution(void Follower.request(Request) throws IOException)) ||
                cflowbelow(execution(public void SendAckRequestProcessor.processRequest(Request))));
    
    pointcut writeBoolToLeader(OutputArchive output, boolean data, String tag) : 
        call(public void OutputArchive.writeLong(boolean, String) throws IOException) && target(output) && args(data, tag) &&
        !within(FollowerQuorumBroadcastCallback) && !within(LeaderQuorumBroadcastCallback) && !within(Record+) && !within(OutputArchive+) &&
        (cflowbelow(execution(void Follower.followLeader() throws InterruptedException)) || 
                cflowbelow(execution(void Follower.request(Request) throws IOException)) ||
                cflowbelow(execution(public void SendAckRequestProcessor.processRequest(Request))));
     
    pointcut writeIntToLeader(OutputArchive output, int data, String tag) : 
        call(public void OutputArchive.writeInt(int, String) throws IOException) && target(output) && args(data, tag) &&
        !within(FollowerQuorumBroadcastCallback) && !within(LeaderQuorumBroadcastCallback) && !within(Record+) && !within(OutputArchive+) &&
        (cflowbelow(execution(void Follower.followLeader() throws InterruptedException)) || 
                cflowbelow(execution(void Follower.request(Request) throws IOException)) ||
                cflowbelow(execution(public void SendAckRequestProcessor.processRequest(Request))));
    
    pointcut writeLongToLeader(OutputArchive output, long data, String tag) : 
        call(public void OutputArchive.writeLong(long, String) throws IOException) && target(output) && args(data, tag) &&
        !within(FollowerQuorumBroadcastCallback) && !within(LeaderQuorumBroadcastCallback) && !within(Record+) && !within(OutputArchive+) &&
        (cflowbelow(execution(void Follower.followLeader() throws InterruptedException)) || 
                cflowbelow(execution(void Follower.request(Request) throws IOException)) ||
                cflowbelow(execution(public void SendAckRequestProcessor.processRequest(Request))));
    
    pointcut writeFloatToLeader(OutputArchive output, float data, String tag) : 
        call(public void OutputArchive.writeFloat(float, String) throws IOException) && target(output) && args(data, tag) &&
        !within(FollowerQuorumBroadcastCallback) && !within(LeaderQuorumBroadcastCallback) && !within(Record+) && !within(OutputArchive+) &&
        (cflowbelow(execution(void Follower.followLeader() throws InterruptedException)) || 
                cflowbelow(execution(void Follower.request(Request) throws IOException)) ||
                cflowbelow(execution(public void SendAckRequestProcessor.processRequest(Request))));

    pointcut writeDoubleToLeader(OutputArchive output, double data, String tag) : 
        call(public void OutputArchive.writeDouble(double, String) throws IOException) && target(output) && args(data, tag) &&
        !within(FollowerQuorumBroadcastCallback) && !within(LeaderQuorumBroadcastCallback) && !within(Record+) && !within(OutputArchive+) &&
        (cflowbelow(execution(void Follower.followLeader() throws InterruptedException)) || 
                cflowbelow(execution(void Follower.request(Request) throws IOException)) ||
                cflowbelow(execution(public void SendAckRequestProcessor.processRequest(Request))));
    
    pointcut writeStringToLeader(OutputArchive output, String data, String tag) : 
        call(public void OutputArchive.writeString(String, String) throws IOException) && target(output) && args(data, tag) &&
        !within(FollowerQuorumBroadcastCallback) && !within(LeaderQuorumBroadcastCallback) && !within(Record+) && !within(OutputArchive+) &&
        (cflowbelow(execution(void Follower.followLeader() throws InterruptedException)) || 
                cflowbelow(execution(void Follower.request(Request) throws IOException)) ||
                cflowbelow(execution(public void SendAckRequestProcessor.processRequest(Request))));
    
    pointcut writeVectorToLeader(OutputArchive output, List data, String tag) : 
        call(public void OutputArchive.startVector(List, String) throws IOException) && target(output) && args(data, tag) &&
        !within(FollowerQuorumBroadcastCallback) && !within(LeaderQuorumBroadcastCallback) && !within(Record+) && !within(OutputArchive+) &&
        (cflowbelow(execution(void Follower.followLeader() throws InterruptedException)) || 
                cflowbelow(execution(void Follower.request(Request) throws IOException)) ||
                cflowbelow(execution(public void SendAckRequestProcessor.processRequest(Request))));

    pointcut writeMapToLeader(OutputArchive output, TreeMap data, String tag) : 
        call(public void OutputArchive.startMap(TreeMap, String) throws IOException) && target(output) && args(data, tag) &&
        !within(FollowerQuorumBroadcastCallback) && !within(LeaderQuorumBroadcastCallback) && !within(Record+) && !within(OutputArchive+) &&
        (cflowbelow(execution(void Follower.followLeader() throws InterruptedException)) || 
                cflowbelow(execution(void Follower.request(Request) throws IOException)) ||
                cflowbelow(execution(public void SendAckRequestProcessor.processRequest(Request))));
    
    void around(OutputArchive output, Record data, String tag) : writeRecordToLeader(output, data, tag) {
        if (output != leaderOutput) {
            proceed(output, data, tag);
            return;
        }
        if (data instanceof QuorumPacket) {
            QuorumPacket p = (QuorumPacket) data;
            if (p.getType() == 5) {
                proceed(output, data, tag);
                return;
            }
        }
        RecordPacket packetWithId = packetGen.createNewRecordPacket("QuorumCallback" + id, id, leaderId, data, tag);
        packetMap.put(packetWithId.getId(), packetWithId);
        try {
            quorumBroadcastInceptor.offerPacket(packetWithId);
        } catch (RemoteException e) {
            LOG.error("", e);
        }
    }

    void around(OutputArchive output, byte data, String tag) : writeByteToLeader(output, data, tag) {
        if (output != leaderOutput) {
            proceed(output, data, tag);
            return;
        }
        PrimitivePacket bytePacket = packetGen.createNewQuorumPrimitivePacket("QuorumCallback" + id, id, leaderId, data, tag);
        packetMap.put(bytePacket.getId(), bytePacket);
        try {
            quorumBroadcastInceptor.offerPacket(bytePacket);
        } catch (RemoteException e) {
            LOG.error("", e);
        }
    }
    
    void around(OutputArchive output, boolean data, String tag) : writeBoolToLeader(output, data, tag) {
        if (output != leaderOutput) {
            proceed(output, data, tag);
            return;
        }
        PrimitivePacket boolPacket = packetGen.createNewQuorumPrimitivePacket("QuorumCallback" + id, id, leaderId, data, tag);
        packetMap.put(boolPacket.getId(), boolPacket);
        try {
            quorumBroadcastInceptor.offerPacket(boolPacket);
        } catch (RemoteException e) {
            LOG.error("", e);
        }
    } 

    void around(OutputArchive output, int data, String tag) : writeIntToLeader(output, data, tag) {
        if (output != leaderOutput) {
            proceed(output, data, tag);
            return;
        }
        PrimitivePacket intPacket = packetGen.createNewQuorumPrimitivePacket("QuorumCallback" + id, id, leaderId, data, tag);
        packetMap.put(intPacket.getId(), intPacket);
        try {
            quorumBroadcastInceptor.offerPacket(intPacket);
        } catch (RemoteException e) {
            LOG.error("", e);
        }
    }
    
    void around(OutputArchive output, long data, String tag) : writeLongToLeader(output, data, tag) {
        if (output != leaderOutput) {
            proceed(output, data, tag);
            return;
        }
        PrimitivePacket longPacket = packetGen.createNewQuorumPrimitivePacket("QuorumCallback" + id, id, leaderId, data, tag);
        packetMap.put(longPacket.getId(), longPacket);
        try {
            quorumBroadcastInceptor.offerPacket(longPacket);
        } catch (RemoteException e) {
            LOG.error("", e);
        }
    }

    void around(OutputArchive output, float data, String tag) : writeFloatToLeader(output, data, tag) {
        if (output != leaderOutput) {
            proceed(output, data, tag);
            return;
        }
        PrimitivePacket floatPacket = packetGen.createNewQuorumPrimitivePacket("QuorumCallback" + id, id, leaderId, data, tag);
        packetMap.put(floatPacket.getId(), floatPacket);
        try {
            quorumBroadcastInceptor.offerPacket(floatPacket);
        } catch (RemoteException e) {
            LOG.error("", e);
        }
    }
    
    void around(OutputArchive output, double data, String tag) : writeDoubleToLeader(output, data, tag) {
        if (output != leaderOutput) {
            proceed(output, data, tag);
            return;
        }
        PrimitivePacket doublePacket = packetGen.createNewQuorumPrimitivePacket("QuorumCallback" + id, id, leaderId, data, tag);
        packetMap.put(doublePacket.getId(), doublePacket);
        try {
            quorumBroadcastInceptor.offerPacket(doublePacket);
        } catch (RemoteException e) {
            LOG.error("", e);
        }
    }

    void around(OutputArchive output, String data, String tag) : writeStringToLeader(output, data, tag) {
        if (output != leaderOutput) {
            proceed(output, data, tag);
            return;
        }
        PrimitivePacket stringPacket = packetGen.createNewQuorumPrimitivePacket("QuorumCallback" + id, id, leaderId, data, tag);
        packetMap.put(stringPacket.getId(), stringPacket);
        try {
            quorumBroadcastInceptor.offerPacket(stringPacket);
        } catch (RemoteException e) {
            LOG.error("", e);
        }
    }

    void around(OutputArchive output, List data, String tag) : writeVectorToLeader(output, data, tag) {
        if (output != leaderOutput) {
            proceed(output, data, tag);
            return;
        }
        PrimitivePacket stringPacket = packetGen.createNewQuorumPrimitivePacket("QuorumCallback" + id, id, leaderId, data, tag);
        packetMap.put(stringPacket.getId(), stringPacket);
        try {
            quorumBroadcastInceptor.offerPacket(stringPacket);
        } catch (RemoteException e) {
            LOG.error("", e);
        }
    }

    void around(OutputArchive output, TreeMap data, String tag) : writeMapToLeader(output, data, tag) {
        if (output != leaderOutput) {
            proceed(output, data, tag);
            return;
        }
        PrimitivePacket stringPacket = packetGen.createNewQuorumPrimitivePacket("QuorumCallback" + id, id, leaderId, data, tag);
        packetMap.put(stringPacket.getId(), stringPacket);
        try {
            quorumBroadcastInceptor.offerPacket(stringPacket);
        } catch (RemoteException e) {
            LOG.error("", e);
        }
    }
    
    pointcut readRecordFromLeader(InputArchive input, Record data, String tag) : 
        call(public void Record.deserialize(InputArchive, String) throws IOException) && target(data) && args(input, tag) &&
        !within(Record+) && (cflowbelow(execution(void Follower.followLeader() throws InterruptedException)));
    
    pointcut readByteFromLeader(InputArchive input) : call(public byte InputArchive.readByte(String) throws IOException) && 
        !within(Record+) && !within(InputArchive+) && target(input) &&
        cflowbelow(execution(void Follower.followLeader() throws InterruptedException));
    
    pointcut readBoolFromLeader(InputArchive input) : call(public boolean InputArchive.readBool(String) throws IOException) && 
        !within(Record+) && !within(InputArchive+) && target(input) &&
        cflowbelow(execution(void Follower.followLeader() throws InterruptedException));
    
    pointcut readIntFromLeader(InputArchive input) : call(public int InputArchive.readInt(String) throws IOException) && 
        !within(Record+) && !within(InputArchive+) && target(input) &&
        cflowbelow(execution(void Follower.followLeader() throws InterruptedException));
    
    pointcut readLongFromLeader(InputArchive input) : call(public long InputArchive.readLong(String) throws IOException) && 
        !within(Record+) && !within(InputArchive+) && target(input) &&
        cflowbelow(execution(void Follower.followLeader() throws InterruptedException));
    
    pointcut readFloatFromLeader(InputArchive input) : call(public float InputArchive.readFloat(String) throws IOException) && 
        !within(Record+) && !within(InputArchive+) && target(input) &&
        cflowbelow(execution(void Follower.followLeader() throws InterruptedException));
    
    pointcut readDoubleFromLeader(InputArchive input) : call(public double InputArchive.readDouble(String) throws IOException) && 
        !within(Record+) && !within(InputArchive+) && target(input) &&
        cflowbelow(execution(void Follower.followLeader() throws InterruptedException));
    
    pointcut readStringFromLeader(InputArchive input) : call(public String InputArchive.readString(String) throws IOException) && 
        !within(Record+) && !within(InputArchive+) && target(input) &&
        cflowbelow(execution(void Follower.followLeader() throws InterruptedException));
    
    pointcut readListFromLeader(InputArchive input) : call(public List InputArchive.startVector(String) throws IOException) && 
        !within(Record+) && !within(InputArchive+) && target(input) &&
        cflowbelow(execution(void Follower.followLeader() throws InterruptedException));
    
    pointcut readMapFromLeader(InputArchive input) : call(public Map InputArchive.startMap(String) throws IOException) && 
        !within(Record+) && !within(InputArchive+) && target(input) &&
        cflowbelow(execution(void Follower.followLeader() throws InterruptedException));
    
    before(InputArchive input) : readByteFromLeader(input) || readBoolFromLeader(input) || readIntFromLeader(input) || 
        readLongFromLeader(input) || readFloatFromLeader(input) || readDoubleFromLeader(input) || 
        readStringFromLeader(input) || readListFromLeader(input) || readMapFromLeader(input) {
        if (input != leaderInput) {
            return;
        }
        try {
            LOG.info("Being in steady state");
            quorumBroadcastSteadyStateListener.informSteadyState(id, 0);
         } catch (RemoteException e) {
            LOG.error(e.getMessage());
         }
    }

    after(InputArchive input) : readByteFromLeader(input) || readBoolFromLeader(input) || readIntFromLeader(input) || 
        readLongFromLeader(input) || readFloatFromLeader(input) || readDoubleFromLeader(input) || 
        readStringFromLeader(input) || readListFromLeader(input) || readMapFromLeader(input) {
        if (ack != null) {
            if (input != leaderInput) {
                return;
            }
            try {
                LOG.info("Acking back for packet " + 112);
                ack.ack(112, id);
            } catch (Exception e) {
                LOG.error(e.getMessage());
            }
        }
    }
    
    before(InputArchive input, Record data, String tag) : readRecordFromLeader(input, data, tag) {
        if (input != leaderInput) {
            return;
        }
        if (!isLastMessagePing) {
            try {
                LOG.info("Being in steady state");
                quorumBroadcastSteadyStateListener.informSteadyState(id, 0);
            } catch (RemoteException e) {
                LOG.error(e.getMessage());
            }
        }
    }

    after(InputArchive input, Record data, String tag) returning : readRecordFromLeader(input, data, tag) {
        if (input != leaderInput) {
            return;
        }
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
                LOG.info("korn data=" + data + " " + data.getClass());
                LOG.info("Acking back for packet " + 112);
                ack.ack(112, id);
            } catch (Exception e) {
                LOG.error(e.getMessage());
            }
        }
    }
    
    pointcut afterPreCondition() : execution(public void Follower.afterPreCondition());
    
    after() : afterPreCondition() {
    }

}
