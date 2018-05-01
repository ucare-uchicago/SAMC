package org.apache.zookeeper.server.persistence;

import java.io.IOException;
import java.net.MalformedURLException;
import java.rmi.Naming;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.util.LinkedList;

import org.apache.zookeeper.server.Request;
import org.apache.zookeeper.server.quorum.ZooKeeperIdAwareAspect;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import mc.DiskWrite;
import mc.DiskWriteAck;
import mc.DiskWriteGenerator;
import mc.Interceptor;
import mc.tools.StackTraceLogger;
import mc.zookeeper.ZKAspectProperties;

public aspect FileTxnSnapLogAspect extends ZooKeeperIdAwareAspect {
    
    private static final Logger LOG = LoggerFactory.getLogger(FileTxnSnapLog.class);
    
    Interceptor quorumBroadcastInceptor;
    DiskWriteAck ack;
    LinkedList<Integer> requestForCommit;
    DiskWriteGenerator writeGen;
    
    public FileTxnSnapLogAspect() {
        try {
            writeGen = new DiskWriteGenerator();
            quorumBroadcastInceptor = (Interceptor) Naming.lookup(
                    ZKAspectProperties.getConfig(ZKAspectProperties.INTERCEPTOR_NAME));
            ack = (DiskWriteAck) Naming.lookup(ZKAspectProperties.getConfig(
                    ZKAspectProperties.INTERCEPTOR_NAME) + "AckDiskWrite");
            requestForCommit = new LinkedList<Integer>();
        } catch (MalformedURLException e) {
            LOG.error(e.toString(), e);
        } catch (RemoteException e) {
            LOG.error(e.toString(), e);
        } catch (NotBoundException e) {
            LOG.error(e.toString(), e);
        }
    }
    
    public int calculateRequestId(Request si) {
        int result = 1;
        result = 37 * result + (int) (si.sessionId ^ (si.sessionId >>> 32));
        result = 37 * result + (int) (si.zxid ^ (si.zxid >>> 32));
        result = 37 * result + si.type;
        return result;
    }
    
    pointcut append(Request si) : execution(public void FileTxnSnapLog.append(Request) throws IOException) && 
        args(si);
    
    before(Request si) : append(si)  {
        LOG.info("Write this request " + si + " to buffer " + requestForCommit);
        requestForCommit.add(calculateRequestId(si));
    }
    
    pointcut commit() : execution(public void FileTxnSnapLog.commit() throws IOException);
    
    void around() : commit() {
        try {
            int dataHash = requestForCommit.hashCode();
            DiskWrite write = writeGen.createNewDiskWrite(id, dataHash);
            quorumBroadcastInceptor.requestWrite(write);
            requestForCommit.clear();
            proceed();
            LOG.info("Acking disk write " + write.getWriteId());
            ack.ack(write.getWriteId(), id);
        } catch (RemoteException e) {
            LOG.error(e.toString());
        }
    }

}
