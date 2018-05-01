package mc.zookeeper;

import java.util.LinkedList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import mc.DiskWrite;
import mc.ModelChecker;
import mc.transition.Transition;

public class SnapLogCommitTransition extends Transition {
    
    final static Logger LOG = LoggerFactory.getLogger(SnapLogCommitTransition.class);

    public static final String ACTION = "snapcommit";
    private static final short ACTION_HASH = (short) ACTION.hashCode();
    
    protected ModelChecker checker;
    protected DiskWrite write;
    
    public SnapLogCommitTransition(ModelChecker checker, DiskWrite write) {
        this.checker = checker;
        this.write = write;
    }

    @Override
    public boolean apply() {
        try {
            boolean result = checker.writeAndWait(write);
            return result;
        } catch (InterruptedException e) {
            LOG.error(e.getMessage());
            return false;
        }
    }

    @Override
    public int getTransitionId() {
        int hash = ((int) ACTION_HASH) << 16;
        hash = hash | (0x0000FFFF & write.getWriteId());
        return hash;
    }

    public ModelChecker getChecker() {
        return checker;
    }

    public void setChecker(ModelChecker checker) {
        this.checker = checker;
    }

    public DiskWrite getWrite() {
        return write;
    }

    public void setWrite(DiskWrite write) {
        this.write = write;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((checker == null) ? 0 : checker.hashCode());
        result = prime * result + ((write == null) ? 0 : write.hashCode());
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
        SnapLogCommitTransition other = (SnapLogCommitTransition) obj;
        if (checker == null) {
            if (other.checker != null)
                return false;
        } else if (!checker.equals(other.checker))
            return false;
        if (write == null) {
            if (other.write != null)
                return false;
        } else if (!write.equals(other.write))
            return false;
        return true;
    }

    @Override
    public String toString() {
        return "SnapLogCommitTransition [checker=" + checker + ", write="
                + write + "]";
    }

    public static SnapLogCommitTransition[] buildTransitions(ModelChecker checker, 
            DiskWrite[] writes) {
        SnapLogCommitTransition[] writeTransitions = new SnapLogCommitTransition[writes.length];
        for (int i = 0; i < writes.length; ++i) {
            writeTransitions[i] = new SnapLogCommitTransition(checker, writes[i]);
        }
        return writeTransitions;
    }

    public static LinkedList<SnapLogCommitTransition> buildTransitions(ModelChecker checker, 
            List<DiskWrite> writes) {
        LinkedList<SnapLogCommitTransition> writeTransitions = 
                new LinkedList<SnapLogCommitTransition>();
        for (DiskWrite write : writes) {
            writeTransitions.add(new SnapLogCommitTransition(checker, write));
        }
        return writeTransitions;
    }
}
