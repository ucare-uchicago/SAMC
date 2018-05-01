package org.apache.zookeeper.server;

import java.io.IOException;
import java.net.MalformedURLException;
import java.rmi.Naming;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;

import mc.zookeeper.ZKAspectProperties;
import mc.zookeeper.quorum.ZabInfoRecorder;

import org.apache.zookeeper.server.quorum.FastLeaderElection;
import org.apache.zookeeper.server.quorum.QuorumPeer.ServerState;
import org.apache.zookeeper.server.quorum.ZooKeeperIdAwareAspect;
import org.apache.zookeeper.server.quorum.Follower;
import org.apache.zookeeper.server.quorum.Leader;
import org.apache.zookeeper.server.quorum.Vote;
import org.apache.zookeeper.server.persistence.FileTxnLog;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public aspect LocalStateTraceAspect extends ZooKeeperIdAwareAspect {
    
    private static final Logger LOG = 
            LoggerFactory.getLogger(LocalStateTraceAspect.class);
    
    ZabInfoRecorder infoRecorder;
    
    public LocalStateTraceAspect() {
        try {
            infoRecorder = (ZabInfoRecorder) Naming.lookup(ZKAspectProperties.getInterceptorName());
        } catch (MalformedURLException e) {
            LOG.info("", e);
        } catch (RemoteException e) {
            LOG.info("", e);
        } catch (NotBoundException e) {
            LOG.info("", e);
        }
    }
    
    pointcut lookForLeader() : execution(public Vote FastLeaderElection.lookForLeader() throws InterruptedException);
    
    before() : lookForLeader() {
        try {
            infoRecorder.setRole(id, ServerState.LOOKING);
        } catch (RemoteException e) {
            LOG.info("", e);
        }
    }
    
    pointcut becomeFollower() : execution(void Follower.followLeader() throws InterruptedException);
    
    before() : becomeFollower() {
        try {
            infoRecorder.setRole(id, ServerState.FOLLOWING);
        } catch (RemoteException e) {
            LOG.info("", e);
        }
    }
    
    pointcut becomeLeader() : execution(void Leader.lead()  throws IOException, InterruptedException);
    
    before() : becomeLeader() {
        try {
            infoRecorder.setRole(id, ServerState.LEADING);
        } catch (RemoteException e) {
            LOG.info("", e);
        }
    }
    
    pointcut setLatestTxId(long txId) : set(long FileTxnLog.lastZxidSeen) && args(txId);
    
    after(long txId) : setLatestTxId(txId) {
        try {
            infoRecorder.setLatestTxId(id, txId);
        } catch (RemoteException e) {
            LOG.info("", e);
        }
    }
    
    pointcut setMaxCommittedLog(long maxCommittedLog) : set(long ZooKeeperServer.maxCommittedLog) && 
        args(maxCommittedLog);
    
    after(long maxCommittedLog) : setMaxCommittedLog(maxCommittedLog) {
        try {
            infoRecorder.setMaxCommittedLog(id, maxCommittedLog);
        } catch (RemoteException e) {
            LOG.info("", e);
        }
    }

}
