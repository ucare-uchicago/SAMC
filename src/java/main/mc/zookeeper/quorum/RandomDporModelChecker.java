package mc.zookeeper.quorum;

import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Random;
import java.util.Set;

import mc.DiskWrite;
import mc.ModelChecker;
import mc.WorkloadFeeder;
import mc.transition.AbstractNodeCrashTransition;
import mc.transition.AbstractNodeOperationTransition;
import mc.transition.AbstractNodeStartTransition;
import mc.transition.DiskWriteTransition;
import mc.transition.NodeCrashTransition;
import mc.transition.NodeOperationTransition;
import mc.transition.NodeStartTransition;
import mc.transition.PacketSendTransition;
import mc.transition.Transition;
import mc.transition.TransitionTuple;
import mc.zookeeper.ZooKeeperEnsembleController;

public class RandomDporModelChecker extends PrototypeSamc {
    
    int numDporExec;
    Random random;

    public RandomDporModelChecker(String interceptorName, String ackName, int maxId,
            int numCrash, int numReboot, String globalStatePathDir, String packetRecordDir,
            ZooKeeperEnsembleController zkController, WorkloadFeeder feeder) {
        this(interceptorName, ackName, maxId, numCrash, numReboot, globalStatePathDir, packetRecordDir, "/tmp", zkController, feeder);
    }
    
    public RandomDporModelChecker(String inceptorName, String ackName, int maxId,
            int numCrash, int numReboot, String globalStatePathDir, String packetRecordDir, String cacheDir,
            ZooKeeperEnsembleController zkController, WorkloadFeeder feeder) {
        super(inceptorName, ackName, maxId, numCrash, numReboot, globalStatePathDir, packetRecordDir, cacheDir, zkController, feeder);
        random = new Random();
        numDporExec = 0;
    }
    
    protected boolean isDependent(Transition t, Transition s) {
        if (t instanceof PacketSendTransition && s instanceof PacketSendTransition) {
            return ((PacketSendTransition) t).getPacket().getToId() == ((PacketSendTransition) s).getPacket().getToId();
        }
        return true;
    }
    
    public Transition nextRandomTransition(LinkedList<Transition> transitions) {
        int i = random.nextInt(transitions.size());
        return transitions.remove(i);
    }

    @SuppressWarnings("unchecked")
    protected void calculateDPORInitialPaths() {
        TransitionTuple lastTransition;
        while ((lastTransition = currentExploringPath.pollLast()) != null) {
            boolean[] oldOnlineStatus = prevOnlineStatus.removeLast();
            LinkedList<TransitionTuple> tmpPath = (LinkedList<TransitionTuple>) currentExploringPath.clone();
            if (lastTransition.transition instanceof AbstractNodeOperationTransition) {
                LinkedList<NodeOperationTransition> transitions = ((AbstractNodeOperationTransition) lastTransition.transition).getAllRealNodeOperationTransitions(oldOnlineStatus);
                for (NodeOperationTransition t : transitions) {
                    if (t.getId() != ((AbstractNodeOperationTransition) lastTransition.transition).id) {
                        LinkedList<TransitionTuple> interestingPath = (LinkedList<TransitionTuple>) tmpPath.clone();
                        interestingPath.add(new TransitionTuple(0, t));
                        addToDporInitialPathList(interestingPath);
                    } else {
                        LinkedList<TransitionTuple> interestingPath = (LinkedList<TransitionTuple>) tmpPath.clone();
                        interestingPath.add(new TransitionTuple(0, t));
                        finishedDporInitialPaths.add(interestingPath);
                    }
                }
            }
            Iterator<TransitionTuple> reverseIter = currentExploringPath.descendingIterator();
            Iterator<boolean[]> reverseOnlineStatusIter = prevOnlineStatus.descendingIterator();
            int index = currentExploringPath.size();
            while (reverseIter.hasNext()) {
                index--;
                TransitionTuple tuple = reverseIter.next();
                oldOnlineStatus = reverseOnlineStatusIter.next();
                Set<Transition> enabledPackets = enabledPacketTable.get(tuple.state);
                if (enabledPackets.contains(lastTransition.transition)) {
                    tmpPath.pollLast();
                    if (lastTransition.transition instanceof PacketSendTransition) {
                        PacketSendTransition lastPacket = (PacketSendTransition) lastTransition.transition;
                        if (tuple.transition instanceof PacketSendTransition) {
                            PacketSendTransition tuplePacket = (PacketSendTransition) tuple.transition;
                            if (lastPacket.getPacket().isObsolete() || tuplePacket.getPacket().isObsolete()) {
                                continue;
                            }  else if (!oldOnlineStatus[lastPacket.getPacket().getFromId()]) {
                                break;
                            } else if (tuplePacket.getPacket().getToId() != lastPacket.getPacket().getToId() || tuplePacket.getPacket().getFromId() != lastPacket.getPacket().getFromId()) {
                                if (isDependent(lastPacket, tuplePacket)) {
                                    addNewDporInitialPath(tmpPath, tuple, new TransitionTuple(0, lastTransition.transition));
                                    break;
                                }
                            } else if (tuplePacket.getPacket().getToId() == lastPacket.getPacket().getToId() && tuplePacket.getPacket().getFromId() == lastPacket.getPacket().getFromId()) {
                                break;
                            }
                        } else if (tuple.transition instanceof NodeOperationTransition || tuple.transition instanceof AbstractNodeOperationTransition) {
                            if (lastPacket.getPacket().isObsolete()) {
                                if (tuple.transition instanceof NodeCrashTransition || tuple.transition instanceof AbstractNodeCrashTransition) {
                                    if (lastPacket.getPacket().getObsoleteBy() == index) {
                                        addNewDporInitialPath(tmpPath, tuple, new TransitionTuple(0, lastTransition.transition));
                                        break;
                                    }
                                }
                            } else {
                                addNewDporInitialPath(tmpPath, tuple, new TransitionTuple(0, lastTransition.transition));
                                break;
                            }
                        } else if (tuple.transition instanceof DiskWriteTransition) {
                            continue;
                        }
                    } else if (lastTransition.transition instanceof AbstractNodeOperationTransition) {
                        if (tuple.transition instanceof NodeOperationTransition) {
                            if (((NodeOperationTransition) tuple.transition).id == ((AbstractNodeOperationTransition) lastTransition.transition).id) {
                                break;
                            }
                        } else if (tuple.transition instanceof PacketSendTransition) {
                            if (((PacketSendTransition) tuple.transition).getPacket().isObsolete()) {
                                continue;
                            }
                        } else if (tuple.transition instanceof DiskWriteTransition) {
                            if (((DiskWriteTransition) tuple.transition).isObsolete()) {
                                continue;
                            } else if (((DiskWriteTransition) tuple.transition).getWrite().getNodeId() != ((AbstractNodeOperationTransition) lastTransition.transition).getId()) {
                                continue;
                            }
                        }
                        addNewDporInitialPath(tmpPath, tuple, new TransitionTuple(0, lastTransition.transition));
                        break;
                    } else if (lastTransition.transition instanceof DiskWriteTransition) {
                        DiskWriteTransition write = (DiskWriteTransition) lastTransition.transition;
                        if (tuple.transition instanceof PacketSendTransition) {
                            if (write.isObsolete()) {
                                continue;
                            } else if (((PacketSendTransition) tuple.transition).getPacket().isObsolete()) {
                                continue;
                            } else if (write.getWrite().getNodeId() == ((PacketSendTransition) tuple.transition).getPacket().getToId()) {
                                addNewDporInitialPath(tmpPath, tuple, new TransitionTuple(0, lastTransition.transition));
                                break;
                            }
                        } else if (tuple.transition instanceof NodeOperationTransition) {
                            if ((!write.isObsolete() && write.getWrite().getNodeId() == ((NodeOperationTransition) tuple.transition).getId()) || 
                                    (write.isObsolete() && index == write.getObsoleteBy())) {
                                addNewDporInitialPath(tmpPath, tuple, new TransitionTuple(0, lastTransition.transition));
                                break;
                            } else {
                                continue;
                            }
                        } else if (tuple.transition instanceof DiskWriteTransition) {
                            continue;
                        }
                    }
                } else {
                    break;
                }
            }
        }
    }
    
    protected void adjustRandomCrashReboot(LinkedList<Transition> enabledTransitions) {
        int numOnline = 0;
        for (int i = 0; i < numNode; ++i) {
            if (isNodeOnline(i)) {
                numOnline++;
            }
        }
        int numOffline = numNode - numOnline;
        int tmp = numOnline < numCrash - currentCrash ? numOnline : numCrash - currentCrash;
        for (int i = 0; i < tmp; ++i) {
            enabledTransitions.add(new AbstractRandomNodeCrashTransition(this));
            currentCrash++;
            numOffline++;
        }
        tmp = numOffline < numReboot - currentReboot ? numOffline : numReboot - currentReboot;
        for (int i = 0; i < tmp; ++i) {
            enabledTransitions.add(new AbstractRandomNodeStartTransition(this));
            currentReboot++;
        }
    }
    
    @Override
    public void resetTest() {
        super.resetTest();
        modelChecking = new PathTraversalWorker();
    }
    
    class PathTraversalWorker extends Thread {
        
        @Override
        public void run() {
            int numAppliedTranstion = 0;
            if (currentDporPath != null && !currentDporPath.isEmpty()) {
                numDporExec++;
                log.info("There is existing DPOR initial path, start with this path first");
                String tmp = "DPOR initial path\n";
                for (TransitionTuple tuple : currentDporPath) {
                    tmp += tuple.toString() + "\n";
                }
                log.info(tmp);
                for (TransitionTuple tuple : currentDporPath) {
                    getOutstandingTcpPacketTransition(currentEnabledTransitions);
                    getOutstandingDiskWrite(currentEnabledTransitions);
                    adjustCrashReboot(currentEnabledTransitions);
                    updateGlobalState2();
                    recordEnabledTransitions(globalState2, currentEnabledTransitions);
                    boolean isThereThisTuple = false;
                    for (int i = 0; i < 35; ++i) {
                        if (tuple.transition instanceof NodeCrashTransition) {
                            isThereThisTuple = currentEnabledTransitions.remove(new AbstractNodeCrashTransition(null));
                        } else if (tuple.transition instanceof NodeStartTransition) {
                            isThereThisTuple = currentEnabledTransitions.remove(new AbstractNodeStartTransition(null));
                        } else {
                            int indexOfTuple = currentEnabledTransitions.indexOf(tuple.transition);
                            isThereThisTuple = indexOfTuple != -1;
                            if (isThereThisTuple) {
                                tuple.transition = currentEnabledTransitions.remove(indexOfTuple);
                            }
                        }
                        if (isThereThisTuple) {
                            break;
                        } else {
                            try {
                                Thread.sleep(100);
                                getOutstandingTcpPacketTransition(currentEnabledTransitions);
                                getOutstandingDiskWrite(currentEnabledTransitions);
                                adjustCrashReboot(currentEnabledTransitions);
                            } catch (InterruptedException e) {
                                log.error("", e);
                            }
                        }
                    }
                    if (!isThereThisTuple) {
                        log.error("Being in wrong state, there is not transition " + 
                                tuple.transition.getTransitionId() + " to apply");
                        try {
                            pathRecordFile.write("no transition\n".getBytes());
                        } catch (IOException e) {
                            log.error("", e);
                        }
                        if (!initialPathSecondAttempt.contains(currentDporPath)) {
                            log.warn("Try this initial path one more time");
                            dporInitialPaths.addFirst(currentDporPath);
                            initialPathSecondAttempt.add(currentDporPath);
                        }
                        if (dporInitialPaths.size() == 0) {
                            exploredBranchRecorder.resetTraversal();
                            exploredBranchRecorder.markBelowSubtreeFinished();
                        } else {
                            currentDporPath = dporInitialPaths.remove();
                        }
                        resetTest();
                        return;
                    }
                    exploredBranchRecorder.createChild(tuple.transition.getTransitionId());
                    exploredBranchRecorder.traverseDownTo(tuple.transition.getTransitionId());
                    try {
                        currentExploringPath.add(new TransitionTuple(globalState2, tuple.transition));
                        prevOnlineStatus.add(isNodeOnline.clone());
//                        updateServerState();
                        saveLocalState();
                        if (tuple.transition instanceof AbstractNodeOperationTransition) {
                            AbstractNodeOperationTransition nodeOperationTransition = (AbstractNodeOperationTransition) tuple.transition;
                            tuple.transition = ((AbstractNodeOperationTransition) tuple.transition).getRealNodeOperationTransition();
                            nodeOperationTransition.id = ((NodeOperationTransition) tuple.transition).getId();
                        }
                        pathRecordFile.write((getGlobalState() + "," + globalState2 + "," + tuple.transition.getTransitionId() + " ; " + tuple.transition.toString() + "\n").getBytes());
                        numAppliedTranstion++;
                        if (tuple.transition.apply()) {
                            updateGlobalState();
                            if (tuple.transition instanceof PacketSendTransition) {
                            } else if (tuple.transition instanceof NodeCrashTransition) {
                                markPacketsObsolete(currentExploringPath.size() - 1, ((NodeCrashTransition) tuple.transition).getId(), currentEnabledTransitions);
                            } else if (tuple.transition instanceof DiskWriteTransition) {
                                if (!((DiskWriteTransition) tuple.transition).isObsolete()) {
                                    DiskWrite write = ((DiskWriteTransition) tuple.transition).getWrite();
                                }
                            }
                        }
                    } catch (IOException e) {
                        log.error("", e);
                    }
                    if (tuple.transition instanceof NodeCrashTransition) {
                        markPacketsObsolete(currentExploringPath.size() - 1, ((NodeCrashTransition) tuple.transition).getId(), currentEnabledTransitions);
                    } 
                }
                log.info("Try to find new path/Continue from DPOR initial path");
                int numWaitTime = 0;
                while (true) {
                    getOutstandingTcpPacketTransition(currentEnabledTransitions);
                    getOutstandingDiskWrite(currentEnabledTransitions);
                    adjustCrashReboot(currentEnabledTransitions);
                    updateGlobalState2();
                    recordEnabledTransitions(globalState2, currentEnabledTransitions);
                    if ((currentEnabledTransitions.isEmpty() && feeder.areAllWorkDone()) || numWaitTime >= 6 || numAppliedTranstion > 200) {
                        boolean verifiedResult = verifier.verify(isNodeOnline);
                        String[] data = verifier.getValues(isNodeOnline);
                        String result = verifiedResult + "";
                        for (String d : data) {
                            result += result + " " + d;
                        }
                        saveResult(result + "\n");
                        String mainPath = "";
                        for (TransitionTuple tuple : currentExploringPath) {
                            mainPath += tuple.toString() + "\n";
                        }
                        log.info("Main path\n" + mainPath);
                        exploredBranchRecorder.markBelowSubtreeFinished();
                        if (dporInitialPaths.size() == 0 || numDporExec >= 100) {
//                            exploredBranchRecorder.resetTraversal();
//                            exploredBranchRecorder.markBelowSubtreeFinished();
//                            log.warn("Finished exploring all states");
//                            zkController.stopEnsemble();
//                            System.exit(0);
                            currentDporPath = null;
                            numDporExec = 0;
                        } else {
                            findDPORInitialPaths();
                            currentDporPath = dporInitialPaths.remove();
                        }
                        resetTest();
                        break;
                    } else if (currentEnabledTransitions.isEmpty()) {
                        try {
                            numWaitTime++;
                            Thread.sleep(500);
                        } catch (InterruptedException e) {
                        }
                        continue;
                    }
                    numWaitTime = 0;
                    Transition transition = nextTransition(currentEnabledTransitions);
                    if (transition != null) {
                        exploredBranchRecorder.createChild(transition.getTransitionId());
                        exploredBranchRecorder.traverseDownTo(transition.getTransitionId());
                        try {
                            currentExploringPath.add(new TransitionTuple(globalState2, transition));
                            prevOnlineStatus.add(isNodeOnline.clone());
//                            updateServerState();
                            saveLocalState();
                            if (transition instanceof AbstractNodeOperationTransition) {
                                AbstractNodeOperationTransition nodeOperationTransition = (AbstractNodeOperationTransition) transition;
                                transition = ((AbstractNodeOperationTransition) transition).getRealNodeOperationTransition();
                                nodeOperationTransition.id = ((NodeOperationTransition) transition).getId();
                            }
                            pathRecordFile.write((getGlobalState() + "," + globalState2 + "," + transition.getTransitionId() + " ; " + transition.toString() + "\n").getBytes());
                            numAppliedTranstion++;
                            if (transition.apply()) {
                                updateGlobalState();
                                if (transition instanceof PacketSendTransition) {
                                } else if (transition instanceof NodeCrashTransition) {
                                    markPacketsObsolete(currentExploringPath.size() - 1, ((NodeCrashTransition) transition).getId(), currentEnabledTransitions);
                                } else if (transition instanceof DiskWriteTransition) {
                                    if (!((DiskWriteTransition) transition).isObsolete()) {
                                        DiskWrite write = ((DiskWriteTransition) transition).getWrite();
                                    }
                                }
                            }
                        } catch (IOException e) {
                            log.error("", e);
                        }
                    } else if (exploredBranchRecorder.getCurrentDepth() == 0) {
                        log.warn("Finished exploring all states");
                    } else {
                        if (dporInitialPaths.size() == 0) {
                            exploredBranchRecorder.resetTraversal();
                            exploredBranchRecorder.markBelowSubtreeFinished();
                            System.exit(0);
                        } else {
                            currentDporPath = dporInitialPaths.remove();
                        }
                        try {
                            pathRecordFile.write("duplicated\n".getBytes());
                        } catch (IOException e) {
                            log.error("", e);
                        }
                        resetTest();
                        break;
                    }
                }
            } else {
                numAppliedTranstion = 0;
                int numWaitTime = 0;
                log.info("Try random path");
                while (true) {
                    getOutstandingTcpPacketTransition(currentEnabledTransitions);
                    getOutstandingDiskWrite(currentEnabledTransitions);
                    adjustRandomCrashReboot(currentEnabledTransitions);
                    updateGlobalState2();
                    recordEnabledTransitions(globalState2, currentEnabledTransitions);
                    if ((currentEnabledTransitions.isEmpty() && feeder.areAllWorkDone()) || numWaitTime >= 6 || numAppliedTranstion > 200) {
                        boolean verifiedResult = verifier.verify(isNodeOnline);
                        String[] data = verifier.getValues(isNodeOnline);
                        String result = verifiedResult + "";
                        for (String d : data) {
                            result += result + " " + d;
                        }
                        saveResult(result + "\n");
//                        recordTestId();
                        String mainPath = "";
                        for (TransitionTuple tuple : currentExploringPath) {
                            mainPath += tuple.toString() + "\n";
                        }
                        log.info("Main path\n" + mainPath);
                        exploredBranchRecorder.markBelowSubtreeFinished();
                        if (numAppliedTranstion <= 50) {
                            findDPORInitialPaths();
                        }
                        if (dporInitialPaths.size() == 0) {
                            exploredBranchRecorder.resetTraversal();
                            exploredBranchRecorder.markBelowSubtreeFinished();
                            log.warn("Finished exploring all states");
                            zkController.stopEnsemble();
                            System.exit(0);
                        } else {
                            currentDporPath = dporInitialPaths.remove();
                        }
                        exploredBranchRecorder.markBelowSubtreeFinished();
                        resetTest();
                        break;
                    } else if (currentEnabledTransitions.isEmpty()) {
                        try {
                            numWaitTime++;
                            Thread.sleep(100);
                        } catch (InterruptedException e) {
                        }
                        continue;
                    }
                    numWaitTime = 0;
                    Transition transition = nextRandomTransition(currentEnabledTransitions);
                    if (transition != null) {
                        exploredBranchRecorder.createChild(transition.getTransitionId());
                        exploredBranchRecorder.traverseDownTo(transition.getTransitionId());
//                        exploredBranchRecorder.noteThisNode(".packets", transition.toString(), false);
                        try {
                            if (transition instanceof AbstractRandomNodeOperationTransition) {
//                                AbstractRandomNodeOperationTransition nodeOperationTransition = (AbstractRandomNodeOperationTransition) transition;
                                Transition oldTransition = transition;
                                transition = ((AbstractRandomNodeOperationTransition) transition).getRealNodeOperationTransition();
                                if (transition == null) {
                                    currentEnabledTransitions.add(oldTransition);
                                    continue;
                                }
//                                nodeOperationTransition.id = ((NodeOperationTransition) transition).getId();
                            }
                            if (transition instanceof NodeCrashTransition) {
                                AbstractNodeCrashTransition c = new AbstractNodeCrashTransition(RandomDporModelChecker.this);
                                c.id = ((NodeCrashTransition) transition).id;
                                currentExploringPath.add(new TransitionTuple(globalState2, c));
                            } else if (transition instanceof NodeStartTransition) {
                                AbstractNodeStartTransition s = new AbstractNodeStartTransition(RandomDporModelChecker.this);
                                s.id = ((NodeStartTransition) transition).id;
                                currentExploringPath.add(new TransitionTuple(globalState2, s));
                            } else {
                                currentExploringPath.add(new TransitionTuple(globalState2, transition));
                            }
                            prevOnlineStatus.add(isNodeOnline.clone());
//                            updateServerState();
                            saveLocalState();
                            pathRecordFile.write((getGlobalState() + "," + transition.getTransitionId() + " ; " + transition.toString() + "\n").getBytes());
                            numAppliedTranstion++;
                            if (transition.apply()) {
                                updateGlobalState();
                                if (transition instanceof PacketSendTransition) {
                                }
                            }
                        } catch (IOException e) {
                            log.error("", e);
                        }
                    } else if (exploredBranchRecorder.getCurrentDepth() == 0) {
                        log.warn("Finished exploring all states");
                    } else {
                        try {
                            pathRecordFile.write("duplicated\n".getBytes());
                        } catch (IOException e) {
                            log.error("", e);
                        }
                        resetTest();
                        break;
                    }
                }
            }
        }

    }
    
    static abstract class AbstractRandomNodeOperationTransition extends NodeOperationTransition {
        
        static final Random RANDOM = new Random(System.currentTimeMillis());
        
        protected ModelChecker checker;
        
        public AbstractRandomNodeOperationTransition(ModelChecker checker) {
            id = -1;
            this.checker = checker;
        }

        public abstract NodeOperationTransition getRealNodeOperationTransition();
        public abstract LinkedList<NodeOperationTransition> getAllRealNodeOperationTransitions(boolean[] onlineStatus);
        
    }

    static class AbstractRandomNodeCrashTransition extends AbstractRandomNodeOperationTransition {
        
        public AbstractRandomNodeCrashTransition(ModelChecker checker) {
            super(checker);
        }

        @Override
        public boolean apply() {
            NodeCrashTransition t = getRealNodeOperationTransition();
            if (t == null) {
                return false;
            }
            id = t.getId();
            return t.apply();
        }

        @Override
        public int getTransitionId() {
            return 101;
        }
        
        @Override
        public boolean equals(Object o) {
            return o instanceof AbstractNodeCrashTransition;
        }
        
        @Override 
        public int hashCode() {
            return 101;
        }
        
        public NodeCrashTransition getRealNodeOperationTransition() {
            LinkedList<NodeOperationTransition> allPossible = getAllRealNodeOperationTransitions(checker.isNodeOnline);
            int i = RANDOM.nextInt(allPossible.size());
            return (NodeCrashTransition) allPossible.get(i);
        }
        
        @Override
        public LinkedList<NodeOperationTransition> getAllRealNodeOperationTransitions(boolean[] onlineStatus) {
            LinkedList<NodeOperationTransition> result = new LinkedList<NodeOperationTransition>();
            for (int i = 0; i < onlineStatus.length; ++i) {
                if (onlineStatus[i]) {
                    result.add(new NodeCrashTransition(checker, i));
                }
            }
            return result;
        }

        public String toString() {
            return "abstract_random_node_crash";
        }
        
    }
    
    static class AbstractRandomNodeStartTransition extends AbstractRandomNodeOperationTransition {
        
        public AbstractRandomNodeStartTransition(ModelChecker checker) {
            super(checker);
        }

        @Override
        public boolean apply() {
            NodeOperationTransition t = getRealNodeOperationTransition();
            if (t == null) {
                return false;
            }
            id = t.getId();
            return t.apply();
        }

        @Override
        public int getTransitionId() {
            return 112;
        }
        
        @Override
        public boolean equals(Object o) {
            return o instanceof AbstractNodeStartTransition;
        }
        
        @Override 
        public int hashCode() {
            return 112;
        }
        
        @Override
        public NodeStartTransition getRealNodeOperationTransition() {
            LinkedList<NodeOperationTransition> allPossible = getAllRealNodeOperationTransitions(checker.isNodeOnline);
            if (allPossible.isEmpty()) {
                return null;
            }
            int i = RANDOM.nextInt(allPossible.size());
            return (NodeStartTransition) allPossible.get(i);
        }
        
        @Override
        public LinkedList<NodeOperationTransition> getAllRealNodeOperationTransitions(boolean[] onlineStatus) {
            LinkedList<NodeOperationTransition> result = new LinkedList<NodeOperationTransition>();
            for (int i = 0; i < onlineStatus.length; ++i) {
                if (!onlineStatus[i]) {
                    result.add(new NodeStartTransition(checker, i));
                }
            }
            return result;
        }

        public String toString() {
            return "abstract_random_node_start";
        }
        
    }

    
}
