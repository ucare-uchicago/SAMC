package mc.zookeeper.quorum;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.LinkedList;
import java.util.ListIterator;
import java.util.Set;

import org.apache.zookeeper.server.quorum.QuorumPeer.ServerState;

import com.almworks.sqlite4java.SQLiteException;

import mc.DiskWrite;
import mc.ExploredBranchRecorder;
import mc.SqliteExploredBranchRecorder;
import mc.SteadyStateInformedModelChecker;
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
import mc.zookeeper.ZooKeeperLogExtractor;

public abstract class PrototypeSamc extends SteadyStateInformedModelChecker implements ZabInfoRecorder {
    
    ZooKeeperLogExtractor logExtractor;
    ExploredBranchRecorder exploredBranchRecorder;
    Hashtable<Integer, Set<Transition>> enabledPacketTable;
    LinkedList<LinkedList<TransitionTuple>> dporInitialPaths;
    HashSet<LinkedList<TransitionTuple>> finishedDporInitialPaths;
    HashSet<LinkedList<TransitionTuple>> initialPathSecondAttempt;
    LinkedList<TransitionTuple> currentDporPath;
    LinkedList<TransitionTuple> currentExploringPath = new LinkedList<TransitionTuple>();
    String cacheDir;
    String stateDir;
    int numCrash;
    int numReboot;
    int currentCrash;
    int currentReboot;
    
    int globalState2;
    LinkedList<boolean[]> prevOnlineStatus;
    ConsistentVerifier verifier;
    int[] onDiskLog;
    LinkedList<int[]> prevOnDiskLog;
    ServerState[] serverState;
    LinkedList<ServerState[]> prevServerState;
    long latestTxId[];
    LinkedList<long[]> prevLatestTxId;
    long maxCommittedLog[];
    LinkedList<long[]> prevMaxCommittedLog;
    
    public PrototypeSamc(String interceptorName, String ackName, int maxId,
            int numCrash, int numReboot, String globalStatePathDir, String packetRecordDir,
            ZooKeeperEnsembleController zkController, WorkloadFeeder feeder) {
        this(interceptorName, ackName, maxId, numCrash, numReboot, globalStatePathDir, packetRecordDir, "/tmp", zkController, feeder);
    }
    
    @SuppressWarnings("unchecked")
    public PrototypeSamc(String inceptorName, String ackName, int maxId,
            int numCrash, int numReboot, String globalStatePathDir, String packetRecordDir, String cacheDir,
            ZooKeeperEnsembleController zkController, WorkloadFeeder feeder) {
        super(inceptorName, ackName, maxId, globalStatePathDir, zkController, feeder);
        logExtractor = new ZooKeeperLogExtractor(packetRecordDir + "/..");
        dporInitialPaths = new LinkedList<LinkedList<TransitionTuple>>();
        finishedDporInitialPaths = new HashSet<LinkedList<TransitionTuple>>();
        initialPathSecondAttempt = new HashSet<LinkedList<TransitionTuple>>();
        this.numCrash = numCrash;
        this.numReboot = numReboot;
        onDiskLog = new int[numNode];
        serverState = new ServerState[numNode];
        latestTxId = new long[numNode];
        maxCommittedLog = new long[numNode];
        verifier = (ConsistentVerifier) feeder.allVerifiers.peek();
        dporInitialPaths = new LinkedList<LinkedList<TransitionTuple>>();
        finishedDporInitialPaths = new HashSet<LinkedList<TransitionTuple>>();
        try {
            File initialPathFile = new File(cacheDir + "/initialPaths");
            if (initialPathFile.exists()) {
                ObjectInputStream ois = new ObjectInputStream(new FileInputStream(initialPathFile));
                LinkedList<LinkedList<TransitionTuple>> dumbDporInitialPaths = (LinkedList<LinkedList<TransitionTuple>>) ois.readObject();
                for (LinkedList<TransitionTuple> dumbInitPath : dumbDporInitialPaths) {
                    LinkedList<TransitionTuple> initPath = new LinkedList<TransitionTuple>();
                    for (TransitionTuple dumbTuple : dumbInitPath) {
                        initPath.add(TransitionTuple.getRealTransitionTuple(this, dumbTuple));
                    }
                    dporInitialPaths.add(initPath);
                }
                ois.close();
                currentDporPath = dporInitialPaths.poll();
            }
            File finishedInitialPathFile = new File(cacheDir + "/finishedInitialPaths");
            if (finishedInitialPathFile.exists()) {
                ObjectInputStream ois = new ObjectInputStream(new FileInputStream(finishedInitialPathFile));
                HashSet<LinkedList<TransitionTuple>> dumbFinishedDporInitialPaths = (HashSet<LinkedList<TransitionTuple>>) ois.readObject();
                for (LinkedList<TransitionTuple> dumbFinishedPath : dumbFinishedDporInitialPaths) {
                    LinkedList<TransitionTuple> finishedPath = new LinkedList<TransitionTuple>();
                    for (TransitionTuple dumbTuple : dumbFinishedPath) {
                        finishedPath.add(TransitionTuple.getRealTransitionTuple(this, dumbTuple));
                    }
                    finishedDporInitialPaths.add(finishedPath);
                }
                ois.close();
            } else {
                finishedDporInitialPaths = new HashSet<LinkedList<TransitionTuple>>();
            }
        } catch (FileNotFoundException e1) {
            log.warn("", e1);
        } catch (IOException e1) {
            log.warn("", e1);
        } catch (ClassNotFoundException e) {
            log.warn("", e);
        }
        stateDir = packetRecordDir;
        try {
            exploredBranchRecorder = new SqliteExploredBranchRecorder(packetRecordDir);
        } catch (SQLiteException e) {
            log.error("", e);
        }
        this.cacheDir = cacheDir;
        resetTest();
    }
    
    public int numOnline() {
        int numOnline = 0;
        for (boolean online : isNodeOnline) {
            if (online) {
                numOnline++;
            }
        }
        return numOnline;
    }
    
    public boolean isQuorumOnline(int numOnline) {
        return (numNode / 2) + 1 >= numOnline;
    }
    
    @Override
    public void setRole(int id, ServerState state) {
        serverState[id] = state;
    }
    
    @Override
    public void setLatestTxId(int id, long txId) {
//        log.info("korn node " + id + " has latest tx id " + txId);
        latestTxId[id] = txId;
    }
    
    @Override
    public void setMaxCommittedLog(int id, long maxCommittedLog) {
//        log.info("korn node " + id + " has max committed log " + maxCommittedLog);
        this.maxCommittedLog[id] = maxCommittedLog;
    }
    
    public int getCurrentFollowerCount() {
        int count = 0;
        for (int i = 0; i < numNode; ++i) {
            if (isNodeOnline[i] && serverState[i] == ServerState.FOLLOWING) {
                count++;
            }
        }
        return count;
    }
    
    public int getFollowerCount(boolean[] isNodeOnline, ServerState[] serverState) {
        int count = 0;
        for (int i = 0; i < numNode; ++i) {
            if (isNodeOnline[i] && serverState[i] == ServerState.FOLLOWING) {
                count++;
            }
        }
        return count;
    }
    
    @Override
    public void resetTest() {
        if (exploredBranchRecorder == null) {
            return;
        }
        super.resetTest();
        currentCrash = 0;
        currentReboot = 0;
        modelChecking = new PathTraversalWorker();
        currentEnabledTransitions = new LinkedList<Transition>();
        currentExploringPath = new LinkedList<TransitionTuple>();
        enabledPacketTable = new Hashtable<Integer, Set<Transition>>();
        exploredBranchRecorder.resetTraversal();
        prevOnlineStatus = new LinkedList<boolean[]>();
        Arrays.fill(onDiskLog, 0);
        prevOnDiskLog = new LinkedList<int[]>();
        Arrays.fill(serverState, ServerState.LOOKING);
        prevServerState = new LinkedList<ServerState[]>();
        Arrays.fill(latestTxId, 0);
        prevLatestTxId = new LinkedList<long[]>();
        Arrays.fill(maxCommittedLog, 0);
        prevMaxCommittedLog = new LinkedList<long[]>();
        File waiting = new File(stateDir + "/.waiting");
        try {
            waiting.createNewFile();
        } catch (IOException e) {
            log.error("", e);
        }
    }
    
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
    
    public void recordEnabledTransitions(int globalState, LinkedList<Transition> currentEnabledTransitions) {
        if (enabledPacketTable.containsKey(globalState)) {
            ((Set<Transition>) enabledPacketTable.get(globalState)).addAll(currentEnabledTransitions);
        } else {
            enabledPacketTable.put(globalState, new HashSet<Transition>(currentEnabledTransitions));
        }
    }

    protected void adjustCrashReboot(LinkedList<Transition> enabledTransitions) {
        int numOnline = 0;
        for (int i = 0; i < numNode; ++i) {
            if (isNodeOnline(i)) {
                numOnline++;
            }
        }
        int numOffline = numNode - numOnline;
        int tmp = numOnline < numCrash - currentCrash ? numOnline : numCrash - currentCrash;
        for (int i = 0; i < tmp; ++i) {
            enabledTransitions.add(new AbstractNodeCrashTransition(this));
            currentCrash++;
            numOffline++;
        }
        tmp = numOffline < numReboot - currentReboot ? numOffline : numReboot - currentReboot;
        for (int i = 0; i < tmp; ++i) {
            enabledTransitions.add(new AbstractNodeStartTransition(this));
            currentReboot++;
        }
    }
    

    protected void markPacketsObsolete(int obsoleteBy, int crashingNode, LinkedList<Transition> enabledTransitions) {
        ListIterator<Transition> iter = enabledTransitions.listIterator();
        while (iter.hasNext()) {
            Transition t = iter.next();
            if (t instanceof PacketSendTransition) {
                PacketSendTransition p = (PacketSendTransition) t;
                if (p.getPacket().getFromId() == crashingNode || p.getPacket().getToId() == crashingNode) {
                    p.getPacket().setObsolete(true);
                    p.getPacket().setObsoleteBy(obsoleteBy);
                }
            } else if (t instanceof DiskWriteTransition) {
                DiskWriteTransition w = (DiskWriteTransition) t;
                if (w.getWrite().getNodeId() == crashingNode) {
                    w.setObsolete(true);
                    w.setObsoleteBy(obsoleteBy);
                }
            }
        }
    }
    
    protected void removeCrashedSenderPackets(int crashedSender, LinkedList<Transition> enabledTransitions) {
        ListIterator<Transition> iter = enabledTransitions.listIterator();
        while (iter.hasNext()) {
            Transition t = iter.next();
            if (t instanceof PacketSendTransition) {
                if (((PacketSendTransition) t).getPacket().getFromId() == crashedSender) {
                    iter.remove();
                }
            }
        }
    }
    
    public void updateGlobalState2() {
        int prime = 31;
        globalState2 = getGlobalState();
        globalState2 = prime * globalState2 + currentEnabledTransitions.hashCode();
        for (int i = 0; i < numNode; ++i) {
            for (int j = 0; j < numNode; ++j) {
                globalState2 = prime * globalState2 + Arrays.hashCode(senderReceiverQueues[i][j].toArray());
            }
        }
    }
    
    protected int getGlobalState2() {
        return globalState2;
    }
    
    protected void convertExecutedAbstractTransitionToReal(LinkedList<TransitionTuple> executedPath) {
        ListIterator<TransitionTuple> iter = executedPath.listIterator();
        while (iter.hasNext()) {
            TransitionTuple iterItem = iter.next();
            if (iterItem.transition instanceof AbstractNodeCrashTransition) {
                AbstractNodeCrashTransition crash = (AbstractNodeCrashTransition) iterItem.transition;
                iter.set(new TransitionTuple(iterItem.state, new NodeCrashTransition(PrototypeSamc.this, crash.id)));
            } else if (iterItem.transition instanceof AbstractNodeStartTransition) {
                AbstractNodeStartTransition start = (AbstractNodeStartTransition) iterItem.transition;
                iter.set(new TransitionTuple(iterItem.state, new NodeStartTransition(PrototypeSamc.this, start.id)));
            }
        }
    }
    
    protected void addToDporInitialPathList(LinkedList<TransitionTuple> dporInitialPath) {
        convertExecutedAbstractTransitionToReal(dporInitialPath);
        if (!finishedDporInitialPaths.contains(dporInitialPath)) {
            dporInitialPaths.add(dporInitialPath);
            finishedDporInitialPaths.add(dporInitialPath);
        } else {
        }
    }
    
    @SuppressWarnings("unchecked")
    protected void addNewDporInitialPath(LinkedList<TransitionTuple> initialPath, 
            TransitionTuple oldTransition, TransitionTuple newTransition) {
        LinkedList<TransitionTuple> oldPath = (LinkedList<TransitionTuple>) initialPath.clone();
        convertExecutedAbstractTransitionToReal(oldPath);
        oldPath.add(new TransitionTuple(0, oldTransition.transition));
        finishedDporInitialPaths.add(oldPath);
        LinkedList<TransitionTuple> newDporInitialPath = (LinkedList<TransitionTuple>) initialPath.clone();
        convertExecutedAbstractTransitionToReal(newDporInitialPath);
        newDporInitialPath.add(newTransition);
        if (!finishedDporInitialPaths.contains(newDporInitialPath)) {
            log.info("Transition " + newTransition.transition + " is dependent with " + oldTransition.transition + " at state " + oldTransition.state);
            dporInitialPaths.add(newDporInitialPath);
            finishedDporInitialPaths.add(newDporInitialPath);
        }
    }
    
    protected void saveDPORInitialPaths() {
        try {
            ObjectOutputStream oos = new ObjectOutputStream(new FileOutputStream(cacheDir + "/initialPaths"));
            LinkedList<LinkedList<TransitionTuple>> dumbDporInitialPaths = new LinkedList<LinkedList<TransitionTuple>>();
            for (LinkedList<TransitionTuple> initPath : dporInitialPaths) {
                LinkedList<TransitionTuple> dumbPath = new LinkedList<TransitionTuple>();
                for (TransitionTuple realTuple : initPath) {
                    dumbPath.add(realTuple.getSerializable());
                }
                dumbDporInitialPaths.add(dumbPath);
            }
            oos.writeObject(dumbDporInitialPaths);
            oos.close();
            HashSet<LinkedList<TransitionTuple>> dumbFinishedDporInitialPaths = new HashSet<LinkedList<TransitionTuple>>();
            for (LinkedList<TransitionTuple> finishedPath : finishedDporInitialPaths) {
                LinkedList<TransitionTuple> dumbPath = new LinkedList<TransitionTuple>();
                for (TransitionTuple realTuple : finishedPath) {
                    dumbPath.add(realTuple.getSerializable());
                }
                dumbFinishedDporInitialPaths.add(dumbPath);
            }
            oos = new ObjectOutputStream(new FileOutputStream(cacheDir + "/finishedInitialPaths"));
            oos.writeObject(dumbFinishedDporInitialPaths);
            oos.close();
        } catch (FileNotFoundException e) {
            log.error("", e);
        } catch (IOException e) {
            log.error("", e);
        }
    }
    
    protected void findDPORInitialPaths() {
        calculateDPORInitialPaths();
        log.info("There are " + dporInitialPaths.size() + " initial path of DPOR");
        int i = 1;
        if (log.isDebugEnabled()) {
            for (LinkedList<TransitionTuple> path : dporInitialPaths) {
                String tmp = "DPOR path no. " + i++ + "\n";
                for (TransitionTuple tuple : path) {
                    tmp += tuple.toString() + "\n";
                }
                log.debug(tmp);
            }
        }
        saveDPORInitialPaths();
    }
    
    protected void addOnDiskLog(int[] onDiskLog, DiskWrite write) {
        int oldLog = onDiskLog[write.getNodeId()];
        int newLog = oldLog * 37 + write.getDataHash();
        onDiskLog[write.getNodeId()] = newLog;
    }

    protected abstract void calculateDPORInitialPaths();
    
    class PathTraversalWorker extends Thread {
        
        @Override
        public void run() {
            int numAppliedTransition = 0;
            while (currentEnabledTransitions.isEmpty()) {
                getOutstandingTcpPacketTransition(currentEnabledTransitions);
                getOutstandingDiskWrite(currentEnabledTransitions);
                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                }
            }
            if (currentDporPath != null) {
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
                    for (int i = 0; i < 120; ++i) {
                        if (tuple.transition instanceof NodeCrashTransition || tuple.transition instanceof AbstractNodeCrashTransition) {
                            isThereThisTuple = currentEnabledTransitions.remove(new AbstractNodeCrashTransition(null));
//                            if (!isThereThisTuple) {
//                                isThereThisTuple = true;
//                                currentCrash++;
//                            }
//                            isThereThisTuple = true;
//                            currentCrash++;
                        } else if (tuple.transition instanceof NodeStartTransition || tuple.transition instanceof AbstractNodeStartTransition) {
                            isThereThisTuple = currentEnabledTransitions.remove(new AbstractNodeStartTransition(null));
//                            if (!isThereThisTuple) {
//                                isThereThisTuple = true;
//                                currentReboot++;
//                            }
//                            isThereThisTuple = true;
//                            currentReboot++;
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
                        prevOnDiskLog.add(onDiskLog.clone());
                        prevServerState.add(serverState.clone());
                        if (tuple.transition instanceof AbstractNodeOperationTransition) {
                            AbstractNodeOperationTransition nodeOperationTransition = (AbstractNodeOperationTransition) tuple.transition;
                            tuple.transition = ((AbstractNodeOperationTransition) tuple.transition).getRealNodeOperationTransition();
                            nodeOperationTransition.id = ((NodeOperationTransition) tuple.transition).getId();
                        }
                        pathRecordFile.write((getGlobalState() + "," + globalState2 + "," + tuple.transition.getTransitionId() + " ; " + tuple.transition.toString() + "\n").getBytes());
                        numAppliedTransition++;
                        if (tuple.transition.apply()) {
                            updateGlobalState();
                            if (tuple.transition instanceof PacketSendTransition) {
                            } else if (tuple.transition instanceof NodeCrashTransition) {
                                    markPacketsObsolete(currentExploringPath.size() - 1, ((NodeCrashTransition) tuple.transition).getId(), currentEnabledTransitions);
                            } else if (tuple.transition instanceof DiskWriteTransition) {
                                if (!((DiskWriteTransition) tuple.transition).isObsolete()) {
                                    addOnDiskLog(onDiskLog, ((DiskWriteTransition) tuple.transition).getWrite());
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
            }
            log.info("Try to find new path/Continue from DPOR initial path");
            int numWaitTime = 0;
            while (true) {
                getOutstandingTcpPacketTransition(currentEnabledTransitions);
                getOutstandingDiskWrite(currentEnabledTransitions);
                adjustCrashReboot(currentEnabledTransitions);
                updateGlobalState2();
                recordEnabledTransitions(globalState2, currentEnabledTransitions);
                if ((currentEnabledTransitions.isEmpty() && feeder.areAllWorkDone()) || numWaitTime >= 20 || numAppliedTransition > 200) {
                    log.info("korn " + currentEnabledTransitions.isEmpty() + " " + feeder.numFinished + " " + (numWaitTime >= 20) + " " + (numAppliedTransition > 200));
                    boolean verifiedResult = verifier.verify(isNodeOnline);
                    String[] data = verifier.getValues(isNodeOnline);
                    String result = verifiedResult + "";
                    for (String d : data) {
                        result += " " + d;
                    }
                    saveResult(result + "\n");
                    String mainPath = "";
                    for (TransitionTuple tuple : currentExploringPath) {
                        mainPath += tuple.toString() + "\n";
                    }
                    log.info("Main path\n" + mainPath);
                    exploredBranchRecorder.markBelowSubtreeFinished();
                    findDPORInitialPaths();
                    if (dporInitialPaths.size() == 0) {
                        exploredBranchRecorder.resetTraversal();
                        exploredBranchRecorder.markBelowSubtreeFinished();
                        log.warn("Finished exploring all states");
                        zkController.stopEnsemble();
                        System.exit(0);
                    } else {
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
                        prevOnDiskLog.add(onDiskLog.clone());
                        prevServerState.add(serverState.clone());
//                        updateServerState();
                        if (transition instanceof AbstractNodeOperationTransition) {
                            AbstractNodeOperationTransition nodeOperationTransition = (AbstractNodeOperationTransition) transition;
                            transition = ((AbstractNodeOperationTransition) transition).getRealNodeOperationTransition();
                            nodeOperationTransition.id = ((NodeOperationTransition) transition).getId();
                        }
                        pathRecordFile.write((getGlobalState() + "," + globalState2 + "," + transition.getTransitionId() + " ; " + transition.toString() + "\n").getBytes());
                        numAppliedTransition++;
                        if (transition.apply()) {
                            updateGlobalState();
                            if (transition instanceof PacketSendTransition) {
                            } else if (transition instanceof NodeCrashTransition) {
                                markPacketsObsolete(currentExploringPath.size() - 1, ((NodeCrashTransition) transition).getId(), currentEnabledTransitions);
                            } else if (transition instanceof DiskWriteTransition) {
                                if (!((DiskWriteTransition) transition).isObsolete()) {
                                    addOnDiskLog(onDiskLog, ((DiskWriteTransition) transition).getWrite());
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
                        log.info("korn all enabled transition " + currentEnabledTransitions);
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
