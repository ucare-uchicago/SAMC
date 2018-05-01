package mc.zookeeper.quorum;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.ListIterator;
import java.util.Set;

import org.apache.jute.Record;
import org.apache.zookeeper.server.quorum.QuorumPacket;
import org.apache.zookeeper.server.quorum.QuorumPeer.ServerState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import mc.AbstractGlobalState;
import mc.AbstractLocalState;
import mc.DiskWrite;
import mc.InterceptPacket;
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

// Semantic aware of packet content and symmetry by role of node and dependency between packet and crash
public class SemanticAwareModelChecker extends PrototypeSamc {
    
    Hashtable<LinkedList<TransitionTuple>, LinkedList<TransitionTuple>> dependentTransitionTable;
    int crashRelatedDporIndex;
    int obsoleteRelatedDporIndex;
    int otherDporIndex;
    
    Hashtable<Integer, HashSet<Integer>> crashHistoryTable;
    Hashtable<Integer, HashSet<Integer>> rebootHistoryTable;

    public SemanticAwareModelChecker(String interceptorName, String ackName, int maxId,
            int numCrash, int numReboot, String globalStatePathDir, String packetRecordDir,
            ZooKeeperEnsembleController zkController, WorkloadFeeder feeder) {
        this(interceptorName, ackName, maxId, numCrash, numReboot, globalStatePathDir, packetRecordDir, "/tmp", zkController, feeder);
    }
    
    @SuppressWarnings("unchecked")
    public SemanticAwareModelChecker(String inceptorName, String ackName, int maxId,
            int numCrash, int numReboot, String globalStatePathDir, String packetRecordDir, String cacheDir,
            ZooKeeperEnsembleController zkController, WorkloadFeeder feeder) {
        super(inceptorName, ackName, maxId, numCrash, numReboot, globalStatePathDir, packetRecordDir, cacheDir, zkController, feeder);
        dependentTransitionTable = new Hashtable<LinkedList<TransitionTuple>, LinkedList<TransitionTuple>>();
        crashRelatedDporIndex = 0;
        obsoleteRelatedDporIndex = 0;
        otherDporIndex = 0;
        crashHistoryTable = new Hashtable<Integer, HashSet<Integer>>();
        rebootHistoryTable = new Hashtable<Integer, HashSet<Integer>>();
        try {
            File crashHistoryTableFile = new File(cacheDir + "/crashHistoryTable");
            if (crashHistoryTableFile.exists()) {
                ObjectInputStream ois = new ObjectInputStream(new FileInputStream(crashHistoryTableFile));
                crashHistoryTable = (Hashtable<Integer, HashSet<Integer>>) ois.readObject();
                ois.close();
            }
            File rebootHistoryTableFile = new File(cacheDir + "/rebootHistoryTable");
            if (rebootHistoryTableFile.exists()) {
                ObjectInputStream ois = new ObjectInputStream(new FileInputStream(rebootHistoryTableFile));
                rebootHistoryTable = (Hashtable<Integer, HashSet<Integer>>) ois.readObject();
                ois.close();
            }
        } catch (FileNotFoundException e) {
            log.error("", e);
        } catch (IOException e) {
            log.error("", e);
        } catch (ClassNotFoundException e) {
            log.error("", e);
        }
    }
    
    @Override
    protected void saveDPORInitialPaths() {
        super.saveDPORInitialPaths();
        try {
            ObjectOutputStream oos = new ObjectOutputStream(new FileOutputStream(cacheDir + "/crashHistoryTable"));
            oos.writeObject(crashHistoryTable);
            oos.close();
            oos = new ObjectOutputStream(new FileOutputStream(cacheDir + "/rebootHistoryTable"));
            oos.writeObject(rebootHistoryTable);
            oos.close();
        } catch (FileNotFoundException e) {
            log.error("", e);
        } catch (IOException e) {
            log.error("", e);
        }
    }
    
    protected boolean isQuorumFine(int numFollower) {
        return numFollower >= numNode / 2;
    }
    
    protected AbstractGlobalState crashPredicate(boolean[] isNodeOnline, int crashNodeId, 
            ServerState[] serverState, long[] latestTxId, int[] onDiskLog) {
        AbstractGlobalState ags = new AbstractGlobalState();
        boolean shouldCrash = false;
        int online = 0;
        for (int i = 0; i < numNode; ++i) {
            if (isNodeOnline[i]) {
                online++;
            }
        }
        boolean isQuorum = online >= numNode / 2 + 1;
        if (!isQuorum) {
            for (int i = 0; i < numNode; ++i) {
                if (i != crashNodeId && isNodeOnline[i]) {
                    int followerCount = 0;
                    // This might be a non-sense predicate because it always falls either first or second condition
                    shouldCrash = true;
                    AbstractLocalState1 als = new AbstractLocalState1(ServerState.LOOKING, followerCount, latestTxId[i], onDiskLog[i]);
                    ags.addAbstractLocalState(als);
                }
            }
            return ags;
        }
        for (int i = 0; i < numNode; ++i) {
            if (i != crashNodeId && isNodeOnline[i]) {
                int followerCount = getFollowerCount(isNodeOnline, serverState) - 1;
                // This might be a non-sense predicate because it always falls either first or second condition
                if (serverState[i] == ServerState.LEADING && serverState[crashNodeId] == ServerState.FOLLOWING) {
                    shouldCrash = true;
                    AbstractLocalState1 als = new AbstractLocalState1(serverState[i], followerCount, latestTxId[i], onDiskLog[i]);
                    ags.addAbstractLocalState(als);
                } else if (serverState[i] == ServerState.FOLLOWING && serverState[crashNodeId] == ServerState.LEADING) {
                    shouldCrash = true;
                    AbstractLocalState1 als = new AbstractLocalState1(serverState[i], followerCount, latestTxId[i], onDiskLog[i]);
                    ags.addAbstractLocalState(als);
                }
            }
        }
        return shouldCrash ? ags : null;
    }
    
    protected AbstractGlobalState getCrashAbstractState(boolean[] isNodeOnline, int crashNodeId, ServerState[] serverState, long[] latestTxId, int[] onDiskLog) {
        AbstractGlobalState ags = new AbstractGlobalState();
        for (int i = 0; i < numNode; ++i) {
            if (i != crashNodeId && isNodeOnline[i]) {
                int followerCount = getFollowerCount(isNodeOnline, serverState) - 1;
                // This might be a non-sense predicate because it always falls either first or second condition
                if (serverState[i] == ServerState.LEADING && serverState[crashNodeId] == ServerState.FOLLOWING) {
                    AbstractLocalState1 als = new AbstractLocalState1(serverState[i], followerCount, latestTxId[i], onDiskLog[i]);
                    ags.addAbstractLocalState(als);
                } else if (serverState[i] == ServerState.FOLLOWING && serverState[crashNodeId] == ServerState.LEADING) {
                    AbstractLocalState1 als = new AbstractLocalState1(serverState[i], followerCount, latestTxId[i], onDiskLog[i]);
                    ags.addAbstractLocalState(als);
                }
            }
        }
        return ags;
    }
    
    protected AbstractGlobalState rebootPredicate(boolean[] isNodeOnline, int rebootNodeId, ServerState[] serverState, int[] onDiskLog) {
        AbstractGlobalState ags = new AbstractGlobalState();
        boolean shouldReboot = false;
        int numOnline = 0;
        for (int i = 0; i < numNode; ++i) {
            if (i != rebootNodeId && isNodeOnline[i]) {
                numOnline++;
                if (serverState[i] == ServerState.LEADING && onDiskLog[i] != onDiskLog[rebootNodeId]) {
                    shouldReboot = true;
                    AbstractLocalState2 als = new AbstractLocalState2(serverState[i], onDiskLog[i]);
                    ags.addAbstractLocalState(als);
                }
            }
        }
        if (numOnline < numNode / 2 + 1) {
            return ags;
        }
        return shouldReboot ? ags : null;
    }
    
    protected AbstractGlobalState getRebootAbstractState(boolean[] isNodeOnline, int rebootNodeId, ServerState[] serverState, long[] latestTxId) {
        AbstractGlobalState ags = new AbstractGlobalState();
        int numOnline = 0;
        for (int i = 0; i < numNode; ++i) {
            if (i != rebootNodeId && isNodeOnline[i]) {
                numOnline++;
                if (serverState[i] == ServerState.LEADING && latestTxId[i] != latestTxId[rebootNodeId]) {
                    AbstractLocalState2 als = new AbstractLocalState2(serverState[i], latestTxId[i]);
                    ags.addAbstractLocalState(als);
                }
            }
        }
        if (numOnline < numNode / 2 + 1) {
            return ags;
        }
        return ags;
    }
    
    public Transition nextInterestingTransition(LinkedList<Transition> transitions) {
        ListIterator<Transition> iter = transitions.listIterator();
        while (iter.hasNext()) {
            Transition transition = iter.next();
            if (!exploredBranchRecorder.isSubtreeBelowChildFinished(transition.getTransitionId())) {
                if (transition instanceof AbstractNodeCrashTransition) {
                    AbstractNodeCrashTransition abstractCrash = (AbstractNodeCrashTransition) transition;
                    LinkedList<NodeOperationTransition> possibleCrash = abstractCrash.getAllRealNodeOperationTransitions(isNodeOnline);
                    for (NodeOperationTransition c : possibleCrash) {
                        AbstractGlobalState ags = crashPredicate(isNodeOnline, c.getId(), serverState, latestTxId, onDiskLog);
                        if (ags != null) {
                            AbstractCrashState acs = new AbstractCrashState(serverState[c.id], latestTxId[c.id], onDiskLog[c.id]);
                            int agsValue = ags.getStateValue();
                            HashSet<Integer> crashRecord = crashHistoryTable.get(agsValue);
                            if (crashRecord == null || !crashRecord.contains(acs.getStateValue())) {
                                iter.remove();
                                return c;
                            }
                        }
                    }
                } else if (transition instanceof AbstractNodeStartTransition) {
                    AbstractNodeStartTransition abstractStart = (AbstractNodeStartTransition) transition;
                    LinkedList<NodeOperationTransition> possibleReboot = abstractStart.getAllRealNodeOperationTransitions(isNodeOnline);
                    for (NodeOperationTransition c : possibleReboot) {
                        AbstractGlobalState ags = rebootPredicate(isNodeOnline, c.getId(), serverState, onDiskLog);
                        if (ags != null) {
                            AbstractRebootState acs = new AbstractRebootState(onDiskLog[c.id]);
                            int agsValue = ags.getStateValue();
                            HashSet<Integer> rebootRecord = rebootHistoryTable.get(agsValue);
                            if (rebootRecord == null || !rebootRecord.contains(acs.getStateValue())) {
                                iter.remove();
                                return c;
                            }
                        }
                    }
                } else {
                    iter.remove();
                    return transition;
                }
            }
        }
        return null;
    }
    
    @Override
    protected void addToDporInitialPathList(LinkedList<TransitionTuple> dporInitialPath) {
        convertExecutedAbstractTransitionToReal(dporInitialPath);
        if (!finishedDporInitialPaths.contains(dporInitialPath)) {
            TransitionTuple t = dporInitialPath.peekLast();
            if (t.transition instanceof NodeCrashTransition) {
                dporInitialPaths.add(crashRelatedDporIndex, dporInitialPath);
                ++crashRelatedDporIndex;
            } else {
                dporInitialPaths.add(crashRelatedDporIndex + obsoleteRelatedDporIndex + otherDporIndex, dporInitialPath);
                ++otherDporIndex;
            }
            finishedDporInitialPaths.add(dporInitialPath);
        } else {
//            log.info("This dependent is duplicated so we will drop it");
        }
    }
    
    @SuppressWarnings("unchecked")
    protected void addNewDporInitialPath(LinkedList<TransitionTuple> initialPath, 
            TransitionTuple oldTransition, TransitionTuple newTransition) {
        LinkedList<TransitionTuple> cloneInitialPath = (LinkedList<TransitionTuple>) initialPath.clone();
        LinkedList<TransitionTuple> oldPath = (LinkedList<TransitionTuple>) initialPath.clone();
        convertExecutedAbstractTransitionToReal(oldPath);
        oldPath.add(new TransitionTuple(0, oldTransition.transition));
        finishedDporInitialPaths.add(oldPath);
        LinkedList<TransitionTuple> newDporInitialPath = (LinkedList<TransitionTuple>) initialPath.clone();
        convertExecutedAbstractTransitionToReal(newDporInitialPath);
        newDporInitialPath.add(newTransition);
        if (!finishedDporInitialPaths.contains(newDporInitialPath)) {
            finishedDporInitialPaths.add(newDporInitialPath);
            if (!dependentTransitionTable.containsKey(cloneInitialPath)) {
                dependentTransitionTable.put(cloneInitialPath, new LinkedList<TransitionTuple>());
            }
            dependentTransitionTable.get(cloneInitialPath).add(newTransition);
        }
    }
    
    protected void finalizeDporInitialPath() {
        for (LinkedList<TransitionTuple> path : dependentTransitionTable.keySet()) {
            LinkedList<TransitionTuple> nextTransitionList = dependentTransitionTable.get(path);
            path.addAll(nextTransitionList);
            boolean isCrash = false;
            boolean isObsolete = false;
            for (TransitionTuple t : nextTransitionList) {
                if (t.transition instanceof AbstractNodeCrashTransition || t.transition instanceof NodeCrashTransition) {
                    isCrash = true;
                } else if (t.transition instanceof PacketSendTransition) {
                    if (((PacketSendTransition) t.transition).getPacket().isObsolete()) {
                        isObsolete = true;
                    }
                } else if (t.transition instanceof DiskWriteTransition) {
                    if (((DiskWriteTransition) t.transition).isObsolete()) {
                        isObsolete = true;
                    }
                } 
            }
            if (isCrash) {
                dporInitialPaths.add(crashRelatedDporIndex, path);
                ++crashRelatedDporIndex;
            } else if (isObsolete) {
                dporInitialPaths.add(crashRelatedDporIndex + obsoleteRelatedDporIndex, path);
                ++obsoleteRelatedDporIndex;
            } else {
                dporInitialPaths.add(crashRelatedDporIndex + obsoleteRelatedDporIndex + otherDporIndex, path);
                ++otherDporIndex;
            }
        }
    }
    
    protected int appendLog(int oldLog, int append) {
        return oldLog * 37 + append;
    }
    
    @SuppressWarnings("unchecked")
    protected void calculateDPORInitialPaths() {
        log.debug("Crash history table = " + crashHistoryTable);
        log.debug("Reboot history table = " + rebootHistoryTable);
        TransitionTuple lastTransition;
        dependentTransitionTable = new Hashtable<LinkedList<TransitionTuple>, LinkedList<TransitionTuple>>();
        while ((lastTransition = currentExploringPath.pollLast()) != null) {
            boolean[] oldOnlineStatus = prevOnlineStatus.removeLast();
            ServerState[] oldServerState = prevServerState.removeLast();
            long[] oldLatestTxId = prevLatestTxId.removeLast();
            int[] oldOnDiskLog = prevOnDiskLog.removeLast();
            LinkedList<TransitionTuple> tmpPath = (LinkedList<TransitionTuple>) currentExploringPath.clone();
            Iterator<TransitionTuple> reverseIter = currentExploringPath.descendingIterator();
            Iterator<boolean[]> reverseOnlineStatusIter = prevOnlineStatus.descendingIterator();
            Iterator<ServerState[]> reverseServerStateIter = prevServerState.descendingIterator();
            Iterator<long[]> reverseOldLatestTxId = prevLatestTxId.descendingIterator();
            Iterator<int[]> reverseOldOnDiskLog = prevOnDiskLog.descendingIterator();
            int index = currentExploringPath.size();
            if (lastTransition.transition instanceof AbstractNodeCrashTransition) {
                AbstractNodeCrashTransition abstractNodeCrashTransition = (AbstractNodeCrashTransition) lastTransition.transition;
                LinkedList<NodeOperationTransition> transitions = abstractNodeCrashTransition.getAllRealNodeOperationTransitions(oldOnlineStatus);
                for (NodeOperationTransition t : transitions) {
                    AbstractGlobalState ags = crashPredicate(oldOnlineStatus, t.id, oldServerState, oldLatestTxId, oldOnDiskLog);
                    if (ags != null) {
                        int agsValue = ags.getStateValue();
//                        log.info("korn ags " + agsValue + " " + ags.localState);
                        AbstractCrashState acs = new AbstractCrashState(oldServerState[t.id], oldLatestTxId[t.id], oldOnDiskLog[t.id]);
                        HashSet<Integer> crashRecord = crashHistoryTable.get(agsValue);
                        if (crashRecord == null) {
                            crashHistoryTable.put(agsValue, new HashSet<Integer>());
                        }
                        if (!crashHistoryTable.get(agsValue).contains(acs.getStateValue())) {
                            log.debug("Adding crash " + t.id + " (" + acs.getStateValue() + ") to state " + agsValue + "(" + crashHistoryTable.get(agsValue) + ")");
                            crashHistoryTable.get(agsValue).add(acs.getStateValue());
                            LinkedList<TransitionTuple> interestingPath = (LinkedList<TransitionTuple>) tmpPath.clone();
                            interestingPath.add(new TransitionTuple(0, t));
                            addToDporInitialPathList(interestingPath);
                        }
                    }
                }
            } else if (lastTransition.transition instanceof NodeCrashTransition) {
                for (int i = 0; i < numNode; ++i) {
                    if (oldOnlineStatus[i]) {
                        AbstractGlobalState ags = crashPredicate(oldOnlineStatus, i, oldServerState, oldLatestTxId, oldOnDiskLog);
                        if (ags != null) {
                            int agsValue = ags.getStateValue();
//                            log.info("korn ags " + agsValue + " " + ags.localState);
                            AbstractCrashState acs = new AbstractCrashState(oldServerState[i], oldLatestTxId[i], oldOnDiskLog[i]);
                            HashSet<Integer> crashRecord = crashHistoryTable.get(agsValue);
                            if (crashRecord == null) {
                                crashHistoryTable.put(agsValue, new HashSet<Integer>());
                            }
                            if (!crashHistoryTable.get(agsValue).contains(acs.getStateValue())) {
                                log.debug("Adding crash " + i + " (" + acs.getStateValue() + ") to state " + agsValue + "(" + crashHistoryTable.get(agsValue) + ")");
//                                log.debug("korn " + )
                                crashHistoryTable.get(agsValue).add(acs.getStateValue());
                                LinkedList<TransitionTuple> interestingPath = (LinkedList<TransitionTuple>) tmpPath.clone();
                                interestingPath.add(new TransitionTuple(0, new NodeCrashTransition(SemanticAwareModelChecker.this, i)));
                                addToDporInitialPathList(interestingPath);
                            }
                        }
                    }
                }
            } else if (lastTransition.transition instanceof AbstractNodeStartTransition) {
                AbstractNodeStartTransition abstractNodeStartTransition = (AbstractNodeStartTransition) lastTransition.transition;
                LinkedList<NodeOperationTransition> transitions = abstractNodeStartTransition.getAllRealNodeOperationTransitions(oldOnlineStatus);
                for (NodeOperationTransition t : transitions) {
                    AbstractGlobalState ags = rebootPredicate(oldOnlineStatus, t.id, oldServerState, oldOnDiskLog);
                    if (ags != null) {
                        int agsValue = ags.getStateValue();
                        AbstractRebootState acs = new AbstractRebootState(oldOnDiskLog[t.id]);
                        HashSet<Integer> rebootRecord = rebootHistoryTable.get(agsValue);
                        if (rebootRecord == null) {
                            rebootHistoryTable.put(agsValue, new HashSet<Integer>());
                        }
                        if (!rebootHistoryTable.get(agsValue).contains(acs.getStateValue())) {
                            log.debug("Adding reboot " + acs.getStateValue() + " to state " + agsValue + "(" + rebootHistoryTable.get(agsValue) + ")");
                            rebootHistoryTable.get(agsValue).add(acs.getStateValue());
                            LinkedList<TransitionTuple> interestingPath = (LinkedList<TransitionTuple>) tmpPath.clone();
                            interestingPath.add(new TransitionTuple(0, t));
                            addToDporInitialPathList(interestingPath);
                        }
                    }
                }
            } else if (lastTransition.transition instanceof NodeStartTransition) {
                for (int i = 0; i < numNode; ++i) {
                    if (!oldOnlineStatus[i]) {
                        AbstractGlobalState ags = rebootPredicate(oldOnlineStatus, i, oldServerState, oldOnDiskLog);
                        if (ags != null) {
                            int agsValue = ags.getStateValue();
                            AbstractRebootState acs = new AbstractRebootState(oldOnDiskLog[i]);
                            HashSet<Integer> rebootRecord = rebootHistoryTable.get(agsValue);
                            if (rebootRecord == null) {
                                rebootHistoryTable.put(agsValue, new HashSet<Integer>());
                            }
                            if (!rebootHistoryTable.get(agsValue).contains(acs.getStateValue())) {
                                log.debug("Adding reboot " + acs.getStateValue() + " to state " + agsValue + "(" + rebootHistoryTable.get(agsValue) + ")");
                                rebootHistoryTable.get(agsValue).add(acs.getStateValue());
                                LinkedList<TransitionTuple> interestingPath = (LinkedList<TransitionTuple>) tmpPath.clone();
                                interestingPath.add(new TransitionTuple(0, new NodeStartTransition(SemanticAwareModelChecker.this, i)));
                                addToDporInitialPathList(interestingPath);
                            }
                        }
                    }
                }
            }
            while (reverseIter.hasNext()) {
                index--;
                TransitionTuple tuple = reverseIter.next();
                oldOnlineStatus = reverseOnlineStatusIter.next();
                oldServerState = reverseServerStateIter.next();
                oldLatestTxId = reverseOldLatestTxId.next();
                oldOnDiskLog = reverseOldOnDiskLog.next();
                Set<Transition> enabledPackets = enabledPacketTable.get(tuple.state);
                if (enabledPackets.contains(lastTransition.transition)) {
                    tmpPath.pollLast();
                    if (lastTransition.transition instanceof PacketSendTransition) {
                        InterceptPacket lastPacket = ((PacketSendTransition) lastTransition.transition).getPacket();
                        if (tuple.transition instanceof PacketSendTransition) {
                            InterceptPacket tuplePacket = ((PacketSendTransition) tuple.transition).getPacket();
                            if (lastPacket.isObsolete() || tuplePacket.isObsolete()) {
                                continue;
                            } else if (!oldOnlineStatus[lastPacket.getFromId()]) {
                                break;
                            } else if (tuplePacket.getToId() != lastPacket.getToId() || tuplePacket.getFromId() != lastPacket.getFromId()) {
                                if (tuplePacket.getToId() == lastPacket.getToId()) {
                                    if (lastPacket instanceof RecordPacket && tuplePacket instanceof RecordPacket) {
                                        Record lastRecord = ((RecordPacket) lastPacket).getRecord();
                                        Record tupleRecord = ((RecordPacket) tuplePacket).getRecord();
                                        if (lastRecord instanceof QuorumPacket && tupleRecord instanceof QuorumPacket) {
                                            QuorumPacket lastQuorumPacket = (QuorumPacket) lastRecord;
                                            QuorumPacket tupleQuorumPacket = (QuorumPacket) tupleRecord;
                                            if (lastQuorumPacket.getType() == 1 && tupleQuorumPacket.getType() == 1) {
                                                addNewDporInitialPath(tmpPath, tuple, new TransitionTuple(0, lastTransition.transition));
                                                break;
                                            }
                                        }
                                    }
                                }
                            } else if (tuplePacket.getToId() == lastPacket.getToId() && tuplePacket.getFromId() == lastPacket.getFromId()) {
                                break;
                            }
                        } else if (tuple.transition instanceof NodeCrashTransition || tuple.transition instanceof AbstractNodeCrashTransition) {
                            if (lastPacket.isObsolete()) {
                                if (lastPacket.getObsoleteBy() >= index) {
                                    addNewDporInitialPath(tmpPath, tuple, new TransitionTuple(0, lastTransition.transition));
                                    break;
                                }
                            } else {
//                                for (int i = 0; i < numNode; ++i) {
//                                    if (oldOnlineStatus[i]) {
//                                        AbstractGlobalState ags = crashPredicate(oldOnlineStatus, i, oldServerState, oldLatestTxId);
//                                        if (ags != null) {
//                                            int agsValue = ags.getStateValue();
//                                            AbstractCrashState acs = new AbstractCrashState(oldServerState[i], oldLatestTxId[i]);
//                                            HashSet<Integer> crashRecord = crashHistoryTable.get(agsValue);
//                                            if (crashRecord == null) {
//                                                crashHistoryTable.put(agsValue, new HashSet<Integer>());
//                                            }
//                                            if (!crashHistoryTable.get(agsValue).contains(acs)) {
//                                                crashHistoryTable.get(agsValue).add(acs.getStateValue());
//                                                addNewDporInitialPath(tmpPath, tuple, new TransitionTuple(0, new NodeCrashTransition(SemanticAwareModelChecker.this, i)));
//                                                break;
//                                            }
//                                        }
//                                    }
//                                }
                            }
                        } else if (tuple.transition instanceof NodeStartTransition || tuple.transition instanceof AbstractNodeStartTransition) {
                            if (lastPacket.isObsolete()) {
                                continue;
                            } else {
                                addNewDporInitialPath(tmpPath, tuple, new TransitionTuple(0, lastTransition.transition));
                                break;
//                                for (int i = 0; i < numNode; ++i) {
//                                    if (!oldOnlineStatus[i]) {
//                                        AbstractGlobalState ags = rebootPredicate(oldOnlineStatus, i, oldServerState, oldLatestTxId);
//                                        if (ags != null) {
//                                            int agsValue = ags.getStateValue();
//                                            AbstractRebootState acs = new AbstractRebootState(oldLatestTxId[i]);
//                                            HashSet<Integer> rebootRecord = rebootHistoryTable.get(agsValue);
//                                            if (rebootRecord == null) {
//                                                rebootHistoryTable.put(agsValue, new HashSet<Integer>());
//                                            }
//                                            if (!rebootHistoryTable.get(agsValue).contains(acs)) {
//                                                rebootHistoryTable.get(agsValue).add(acs.getStateValue());
//                                                addNewDporInitialPath(tmpPath, tuple, new TransitionTuple(0, new NodeStartTransition(SemanticAwareModelChecker.this, i)));
//                                                break;
//                                            }
//                                        }
//                                    }
//                                }
                            }
                        } else if (tuple.transition instanceof DiskWriteTransition) {
                            DiskWriteTransition writeTransition = (DiskWriteTransition) tuple.transition;
                            if (!lastPacket.isObsolete() && !writeTransition.isObsolete()) {
                                DiskWrite write = writeTransition.getWrite();
                                if (write.getNodeId() == lastPacket.getFromId() || write.getNodeId() == lastPacket.getToId()) {
                                    addNewDporInitialPath(tmpPath, tuple, new TransitionTuple(0, lastTransition.transition));
                                    break;
                                }
                            }
                        }
                    } else if (lastTransition.transition instanceof AbstractNodeCrashTransition || lastTransition.transition instanceof NodeCrashTransition) {
                        if (tuple.transition instanceof NodeOperationTransition) {
                            if (((NodeOperationTransition) tuple.transition).id == ((NodeOperationTransition) lastTransition.transition).id) {
                                break;
                            } else if (tuple.transition instanceof AbstractNodeCrashTransition || tuple.transition instanceof NodeCrashTransition) {
                                break;
                            }
                        } else if (tuple.transition instanceof PacketSendTransition) {
                            InterceptPacket packet = ((PacketSendTransition) tuple.transition).getPacket();
                            if (packet.isObsolete()) {
                                continue;
                            } else {
                                for (int i = 0; i < numNode; ++i) {
                                    if (oldOnlineStatus[i]) {
                                        AbstractGlobalState ags = crashPredicate(oldOnlineStatus, i, oldServerState, oldLatestTxId, oldOnDiskLog);
                                        if (ags != null) {
                                            int agsValue = ags.getStateValue();
//                                            log.info("korn ags " + agsValue + " " + ags.localState);
                                            AbstractCrashState acs = new AbstractCrashState(oldServerState[i], oldLatestTxId[i], oldOnDiskLog[i]);
                                            HashSet<Integer> crashRecord = crashHistoryTable.get(agsValue);
                                            if (crashRecord == null) {
                                                crashHistoryTable.put(agsValue, new HashSet<Integer>());
                                            }
                                            if (!crashHistoryTable.get(agsValue).contains(acs.getStateValue())) {
                                                log.debug("Adding crash " + i + " (" + acs.getStateValue() + ") to state " + agsValue + "(" + crashHistoryTable.get(agsValue) + ")");
                                                crashHistoryTable.get(agsValue).add(acs.getStateValue());
                                                addNewDporInitialPath(tmpPath, tuple, new TransitionTuple(0, new NodeCrashTransition(SemanticAwareModelChecker.this, i)));
                                                break;
                                            }
                                        }
                                    }
                                }
                            }
                        } else if (tuple.transition instanceof DiskWriteTransition) {
                            if (((DiskWriteTransition) tuple.transition).isObsolete()) {
                                continue;
                            } else {
                                addNewDporInitialPath(tmpPath, tuple, new TransitionTuple(0, 
                                        new NodeCrashTransition(SemanticAwareModelChecker.this, 
                                                ((DiskWriteTransition) tuple.transition).getWrite().getNodeId())));
                                break;
                            }
                        }
                    } else if (lastTransition.transition instanceof AbstractNodeStartTransition || lastTransition.transition instanceof NodeStartTransition) {
                        if (tuple.transition instanceof NodeOperationTransition) {
                            if (((NodeOperationTransition) tuple.transition).id == ((NodeOperationTransition) lastTransition.transition).id) {
                                break;
                            } else if (tuple.transition instanceof AbstractNodeStartTransition || tuple.transition instanceof NodeStartTransition) {
                                break;
                            }
                        } else if (tuple.transition instanceof PacketSendTransition) {
                            if (((PacketSendTransition) tuple.transition).getPacket().isObsolete()) {
                                continue;
                            } else {
                                for (int i = 0; i < numNode; ++i) {
                                    if (!oldOnlineStatus[i]) {
                                        AbstractGlobalState ags = rebootPredicate(oldOnlineStatus, i, oldServerState, oldOnDiskLog);
                                        if (ags != null) {
                                            int agsValue = ags.getStateValue();
                                            AbstractRebootState acs = new AbstractRebootState(oldOnDiskLog[i]);
                                            HashSet<Integer> rebootRecord = rebootHistoryTable.get(agsValue);
                                            if (rebootRecord == null) {
                                                rebootHistoryTable.put(agsValue, new HashSet<Integer>());
                                            }
                                            if (!rebootHistoryTable.get(agsValue).contains(acs.getStateValue())) {
                                                log.debug("Adding reboot " + acs.getStateValue() + " to state " + agsValue + "(" + rebootHistoryTable.get(agsValue) + ")");
                                                rebootHistoryTable.get(agsValue).add(acs.getStateValue());
                                                addNewDporInitialPath(tmpPath, tuple, new TransitionTuple(0, new NodeStartTransition(SemanticAwareModelChecker.this, i)));
                                                break;
                                            }
                                        }
                                    }
                                }
                            }
                        } else if (tuple.transition instanceof NodeStartTransition || tuple.transition instanceof AbstractNodeStartTransition) {
                            break;
                        } else if (tuple.transition instanceof DiskWriteTransition) {
                            if (((DiskWriteTransition) tuple.transition).isObsolete()) {
                                continue;
                            } else {
                                for (int i = 0; i < numNode; ++i) {
                                    if (!oldOnlineStatus[i]) {
                                        AbstractGlobalState ags = rebootPredicate(oldOnlineStatus, i, oldServerState, oldOnDiskLog);
                                        if (ags != null) {
                                            int agsValue = ags.getStateValue();
                                            AbstractRebootState acs = new AbstractRebootState(oldOnDiskLog[i]);
                                            HashSet<Integer> rebootRecord = rebootHistoryTable.get(agsValue);
                                            if (rebootRecord == null) {
                                                rebootHistoryTable.put(agsValue, new HashSet<Integer>());
                                            }
                                            if (!rebootHistoryTable.get(agsValue).contains(acs.getStateValue())) {
                                                log.debug("Adding reboot " + acs.getStateValue() + " to state " + agsValue + "(" + rebootHistoryTable.get(agsValue) + ")");
                                                rebootHistoryTable.get(agsValue).add(acs.getStateValue());
                                                addNewDporInitialPath(tmpPath, tuple, 
                                                        new TransitionTuple(0, new NodeStartTransition(SemanticAwareModelChecker.this, i)));
                                                break;
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    } else if (lastTransition.transition instanceof DiskWriteTransition) {
                        DiskWriteTransition write = (DiskWriteTransition) lastTransition.transition;
                        if (tuple.transition instanceof PacketSendTransition) {
                            InterceptPacket packet = ((PacketSendTransition) tuple.transition).getPacket();
                            if (!write.isObsolete() && !packet.isObsolete()) {
                                if (write.getWrite().getNodeId() == packet.getFromId() || write.getWrite().getNodeId() == packet.getToId()) {
                                    addNewDporInitialPath(tmpPath, tuple, new TransitionTuple(0, lastTransition.transition));
                                    break;
                                }
                            }
                        } else if (tuple.transition instanceof NodeOperationTransition) {
                            if ((!write.isObsolete() && write.getWrite().getNodeId() == ((NodeOperationTransition) tuple.transition).getId()) || 
                                    (write.isObsolete() && index == write.getObsoleteBy()) || tuple.transition instanceof NodeStartTransition || 
                                    tuple.transition instanceof AbstractNodeStartTransition) {
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
        finalizeDporInitialPath();
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
                            if (obsoleteRelatedDporIndex != 0) {
                                obsoleteRelatedDporIndex--;
                            } else if (crashRelatedDporIndex != 0) {
                                crashRelatedDporIndex--;
                            } else if (otherDporIndex != 0) {
                                otherDporIndex--;
                            }
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
//                        updateServerState();
                        prevServerState.add(serverState.clone());
                        prevLatestTxId.add(latestTxId.clone());
                        prevMaxCommittedLog.add(maxCommittedLog.clone());
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
                                    onDiskLog[write.getNodeId()] = appendLog(onDiskLog[write.getNodeId()], write.getDataHash());
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
                if ((currentEnabledTransitions.isEmpty() && feeder.areAllWorkDone()) || numWaitTime >= 20 || numAppliedTranstion > 200) {
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
                        if (obsoleteRelatedDporIndex != 0) {
                            obsoleteRelatedDporIndex--;
                        } else if (crashRelatedDporIndex != 0) {
                            crashRelatedDporIndex--;
                        } else if (otherDporIndex != 0) {
                            otherDporIndex--;
                        }
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
//                Transition transition = nextTransition(currentEnabledTransitions);
                Transition transition = nextInterestingTransition(currentEnabledTransitions);
                if (transition == null) {
                    transition = nextTransition(currentEnabledTransitions);
                }
                if (transition != null) {
                    exploredBranchRecorder.createChild(transition.getTransitionId());
                    exploredBranchRecorder.traverseDownTo(transition.getTransitionId());
                    try {
                        currentExploringPath.add(new TransitionTuple(globalState2, transition));
                        prevOnlineStatus.add(isNodeOnline.clone());
                        prevOnDiskLog.add(onDiskLog.clone());
//                        updateServerState();
                        prevServerState.add(serverState.clone());
                        prevLatestTxId.add(latestTxId.clone());
                        prevMaxCommittedLog.add(maxCommittedLog.clone());
                        saveLocalState();
                        if (transition instanceof AbstractNodeOperationTransition) {
                            AbstractNodeOperationTransition nodeOperationTransition = (AbstractNodeOperationTransition) transition;
                            transition = ((AbstractNodeOperationTransition) transition).getRealNodeOperationTransition();
                            nodeOperationTransition.id = ((NodeOperationTransition) transition).getId();
                        }
                        pathRecordFile.write((getGlobalState() + "," + globalState2 + "," + transition.getTransitionId() + " ; " + transition.toString() + "\n").getBytes());
                        numAppliedTranstion++;
                        if (transition instanceof NodeCrashTransition) {
                            NodeCrashTransition crashTransition = (NodeCrashTransition) transition;
                            AbstractGlobalState ags = getCrashAbstractState(isNodeOnline, crashTransition.getId(), serverState, latestTxId, onDiskLog);
                            AbstractCrashState acs = new AbstractCrashState(serverState[crashTransition.getId()], latestTxId[crashTransition.getId()], onDiskLog[crashTransition.getId()]);
                            int agsValue = ags.getStateValue();
//                            log.info("korn ags " + agsValue + " " + ags.localState);
                            HashSet<Integer> crashRecord = crashHistoryTable.get(agsValue);
                            if (crashRecord == null) {
                                crashHistoryTable.put(agsValue, new HashSet<Integer>());
                            }
                            log.debug("Adding crash " + acs.getStateValue() + " to state " + agsValue + "(" + crashHistoryTable.get(agsValue) + ")");
                            crashHistoryTable.get(agsValue).add(acs.getStateValue());
                        } else if (transition instanceof NodeStartTransition) {
                            NodeStartTransition startTransition = (NodeStartTransition) transition;
                            AbstractGlobalState ags = getRebootAbstractState(isNodeOnline, startTransition.getId(), serverState, latestTxId);
                            AbstractRebootState ars = new AbstractRebootState(onDiskLog[startTransition.getId()]);
                            int agsValue = ags.getStateValue();
                            HashSet<Integer> crashRecord = rebootHistoryTable.get(agsValue);
                            if (crashRecord == null) {
                                rebootHistoryTable.put(agsValue, new HashSet<Integer>());
                            }
                            log.debug("Adding reboot " + ars.getStateValue() + " to state " + agsValue + "(" + rebootHistoryTable.get(agsValue) + ")");
                            rebootHistoryTable.get(agsValue).add(ars.getStateValue());
                        }
                        if (transition.apply()) {
                            updateGlobalState();
                            if (transition instanceof PacketSendTransition) {
                            } else if (transition instanceof NodeCrashTransition) {
                                NodeCrashTransition crashTransition = (NodeCrashTransition) transition;
                                markPacketsObsolete(currentExploringPath.size() - 1, crashTransition.getId(), currentEnabledTransitions);
                            } else if (transition instanceof NodeStartTransition) {
                            } else if (transition instanceof DiskWriteTransition) {
                                if (!((DiskWriteTransition) transition).isObsolete()) {
                                    DiskWrite write = ((DiskWriteTransition) transition).getWrite();
                                    onDiskLog[write.getNodeId()] = appendLog(onDiskLog[write.getNodeId()], write.getDataHash());
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
                        if (obsoleteRelatedDporIndex != 0) {
                            obsoleteRelatedDporIndex--;
                        } else if (crashRelatedDporIndex != 0) {
                            crashRelatedDporIndex--;
                        } else if (otherDporIndex != 0) {
                            otherDporIndex--;
                        }
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
        }

    }
    
    public static class AbstractLocalState1 extends AbstractLocalState {
        
        
        protected final Logger log = LoggerFactory.getLogger(AbstractLocalState1.class);
        
        public ServerState electionStatus;
        public int followerCount;
        public long latestTxId;
        public int onDiskTx;

        public AbstractLocalState1() {
        }

        public AbstractLocalState1(ServerState electionStatus,
                int followerCount, long latestTxId, int onDiskTx) {
            this.electionStatus = electionStatus;
            this.followerCount = followerCount;
            this.latestTxId = latestTxId;
            this.onDiskTx = onDiskTx;
        }

        @Override
        public int hashCode() {
//            log.info("korn als1 " + (electionStatus.toString()) + " " + followerCount + " " + latestTxId + " " + onDiskTx);
            final int prime = 31;
            int result = 1;
            result = prime
                    * result
                    + ((electionStatus == null) ? 0 : electionStatus.ordinal() + 10);
            result = prime * result + followerCount;
            result = prime * result + (int) (latestTxId ^ (latestTxId >>> 32));
            result = prime * result + onDiskTx;
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
            AbstractLocalState1 other = (AbstractLocalState1) obj;
            if (electionStatus != other.electionStatus)
                return false;
            if (followerCount != other.followerCount)
                return false;
            if (latestTxId != other.latestTxId)
                return false;
            if (onDiskTx != other.onDiskTx)
                return false;
            return true;
        }

    }
    
    public static class AbstractLocalState2 extends AbstractLocalState {
        
        public ServerState electionState;
        public long latestTxId;

        public AbstractLocalState2() {
        }

        public AbstractLocalState2(ServerState electionState, long latestTxId) {
            this.electionState = electionState;
            this.latestTxId = latestTxId;
        }

        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result
                    + ((electionState == null) ? 0 : electionState.ordinal() + 10);
            result = prime * result + (int) (latestTxId ^ (latestTxId >>> 32));
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
            AbstractLocalState2 other = (AbstractLocalState2) obj;
            if (electionState != other.electionState)
                return false;
            if (latestTxId != other.latestTxId)
                return false;
            return true;
        }

    }
    
    public static class AbstractCrashState extends AbstractLocalState {
        
        public ServerState electionState;
        public long latestTxId;
        public int onDiskLog;
        
        public AbstractCrashState() {
            
        }

        public AbstractCrashState(ServerState electionState, long latestTxId, int onDiskLog) {
            this.electionState = electionState;
            this.latestTxId = latestTxId;
            this.onDiskLog = onDiskLog;
        }

        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result
                    + ((electionState == null) ? 0 : electionState.ordinal() + 10);
            result = prime * result + (int) (latestTxId ^ (latestTxId >>> 32));
            result = prime * result + onDiskLog;
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
            AbstractCrashState other = (AbstractCrashState) obj;
            if (electionState != other.electionState)
                return false;
            if (latestTxId != other.latestTxId)
                return false;
            if (onDiskLog != other.onDiskLog)
                return false;
            return true;
        }
        
    }
    
    public static class AbstractRebootState extends AbstractLocalState {
        
        int onDiskLog;

        public AbstractRebootState() {
        }

        public AbstractRebootState(int onDiskLog) {
            this.onDiskLog = onDiskLog;
        }

        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result + onDiskLog;
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
            AbstractRebootState other = (AbstractRebootState) obj;
            if (onDiskLog != other.onDiskLog)
                return false;
            return true;
        }

    }
    
}
