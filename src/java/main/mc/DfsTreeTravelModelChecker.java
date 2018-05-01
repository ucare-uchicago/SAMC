package mc;

import java.rmi.RemoteException;
import java.util.LinkedList;
import java.util.ListIterator;

import org.apache.zookeeper.server.quorum.QuorumPeer.ServerState;

import mc.transition.Transition;
import mc.zookeeper.ZooKeeperEnsembleController;
import mc.zookeeper.quorum.ZabInfoRecorder;

public class DfsTreeTravelModelChecker extends TreeTravelModelChecker implements ZabInfoRecorder {

    public DfsTreeTravelModelChecker(String interceptorName, String ackName, int numNode,
            int numCrash, int numReboot, String globalStatePathDir, String packetRecordDir,
            ZooKeeperEnsembleController zkController, WorkloadFeeder feeder) {
        super(interceptorName, ackName, numNode, numCrash, numReboot, globalStatePathDir, 
                packetRecordDir, zkController, feeder);
    }
    
    @Override
    public Transition nextTransition(LinkedList<Transition> transitions) {
        ListIterator<Transition> iter = transitions.listIterator();
        while (iter.hasNext()) {
            Transition transition = iter.next();
            if (!exploredBranchRecorder.isSubtreeBelowChildFinished(transition.getTransitionId())) {
                iter.remove();
                return transition;
            }
        }
        return null;
    }

    @Override
    public void setRole(int id, ServerState role) throws RemoteException {
        
    }

    @Override
    public void setLatestTxId(int id, long txId) throws RemoteException {
        
    }

    @Override
    public void setMaxCommittedLog(int id, long maxCommittedLog)
            throws RemoteException {
        
    }
    
}
