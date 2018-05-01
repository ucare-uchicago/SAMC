package mc.transition;

import mc.ModelChecker;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NodeStartTransition extends NodeOperationTransition {
    
    final static Logger LOG = LoggerFactory.getLogger(NodeStartTransition.class);

    public static final String ACTION = "nodestart"; 
    private static final short ACTION_HASH = (short) ACTION.hashCode();

    ModelChecker checker;
    
    public NodeStartTransition(ModelChecker checker, int id) {
        this.checker = checker;
        this.id = id;
    }
    
    @Override
    public int getTransitionId() {
        final int prime = 31;
        int hash = 1;
        hash = prime * hash + id;
        int tranId = ((int) ACTION_HASH) << 16;
        tranId = tranId | (0x0000FFFF & hash);
        return tranId;
    }
    
    public String toString() {
        return "nodestart id=" + id;
    }

    @Override
    public boolean apply() {
        LOG.info("Start node " + id);
        if (checker.runNode(id)) {
            checker.numCurrentReboot++;
            return true;
        }
        return false;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + id;
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        NodeStartTransition other = (NodeStartTransition) obj;
        if (id != other.id)
            return false;
        return true;
    }
    
}
