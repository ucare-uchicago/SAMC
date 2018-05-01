package org.apache.zookeeper.server.quorum;

import mc.zookeeper.ZKAspectProperties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract aspect ZooKeeperIdAwareAspect {
    
    private static final Logger LOG = LoggerFactory.getLogger(ZooKeeperIdAwareAspect.class);

    protected boolean isLeChecked;
    protected boolean isZabChecked;
    
    protected int id;
    
    public ZooKeeperIdAwareAspect() {
        id = -1;
        isLeChecked = ZKAspectProperties.isLeChecked();
        isZabChecked = ZKAspectProperties.isZabChecked();
    }
    
    pointcut setMyId(long id) : set(long QuorumPeer.myid) && args(id);
    
    before(long id) : setMyId(id) {
        LOG.debug("Set node id, " + id);
        this.id = (int) id;
    }

}
