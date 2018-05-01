package mc;

import java.io.File;
import java.io.IOException;
import java.util.LinkedList;
import java.util.ListIterator;
import java.util.concurrent.ConcurrentLinkedQueue;

import com.almworks.sqlite4java.SQLiteException;

import mc.transition.DiskWriteTransition;
import mc.transition.NodeCrashTransition;
import mc.transition.NodeStartTransition;
import mc.transition.PacketSendTransition;
import mc.transition.Transition;
import mc.zookeeper.ZooKeeperEnsembleController;
import mc.zookeeper.quorum.ConsistentVerifier;

public abstract class TreeTravelModelChecker extends SteadyStateInformedModelChecker {
    
    protected String stateDir;
    protected ExploredBranchRecorder exploredBranchRecorder;
    protected LinkedList<InterceptPacket> enabledPacketList;
    protected int numCrash;
    protected int numReboot;
    
    ConsistentVerifier verifier;
    
    public TreeTravelModelChecker(String interceptorName, String ackName, int numNode,
            int numCrash, int numReboot, String globalStatePathDir, String packetRecordDir, 
            ZooKeeperEnsembleController zkController, WorkloadFeeder feeder) {
        super(interceptorName, ackName, numNode, globalStatePathDir, zkController, feeder);
        try {
            this.numCrash = numCrash;
            this.numReboot = numReboot;
            this.stateDir = packetRecordDir;
            verifier = (ConsistentVerifier) feeder.allVerifiers.peek();
            exploredBranchRecorder = new SqliteExploredBranchRecorder(packetRecordDir);
        } catch (SQLiteException e) {
            log.error("", e);
        }
        resetTest();
    }
    
    abstract public Transition nextTransition(LinkedList<Transition> transitions);
    
    @Override
    public void resetTest() {
        if (exploredBranchRecorder == null) {
            return;
        }
        super.resetTest();
        modelChecking = new PacketExplorer();
        enabledPacketList = new LinkedList<InterceptPacket>();
        currentEnabledTransitions = new LinkedList<Transition>();
        exploredBranchRecorder.resetTraversal();
        File waiting = new File(stateDir + "/.waiting");
        try {
            waiting.createNewFile();
        } catch (IOException e) {
            log.error("", e);
        }
        numCurrentCrash = 0;
        numCurrentReboot = 0;
    }
    
    protected void adjustCrashAndReboot(LinkedList<Transition> transitions) {
        if (numCurrentCrash < numCrash) {
            for (int i = 0; i < isNodeOnline.length; ++i) {
                if (isNodeOnline(i)) {
                    NodeCrashTransition c = new NodeCrashTransition(this, i);
                    if (!transitions.contains(c)) {
                        transitions.add(c);
                    }
                }
            }
        } else {
            ListIterator<Transition> iter = transitions.listIterator();
            while (iter.hasNext()) {
                if (iter.next() instanceof NodeCrashTransition) {
                    iter.remove();
                }
            }
        }
        if (numCurrentReboot < numReboot) {
            for (int i = 0; i < isNodeOnline.length; ++i) {
                if (!isNodeOnline(i)) {
                    NodeStartTransition s = new NodeStartTransition(this, i);
                    if (!transitions.contains(s)) {
                        transitions.add(s);
                    }
                }
            }
        } else {
            ListIterator<Transition> iter = transitions.listIterator();
            while (iter.hasNext()) {
                if (iter.next() instanceof NodeStartTransition) {
                    iter.remove();
                }
            }
        }
    }
    
    protected void recordTestId() {
        exploredBranchRecorder.noteThisNode(".test_id", testId + "");
    }
    
    class PacketExplorer extends Thread {

        @Override
        @SuppressWarnings("unchecked")
        public void run() {
            LinkedList<LinkedList<Transition>> pastEnabledTransitionList = 
                    new LinkedList<LinkedList<Transition>>();
            int numWaitTime = 0;
            int numAppliedTransition = 0;
            getOutstandingTcpPacketTransition(currentEnabledTransitions);
            getOutstandingDiskWrite(currentEnabledTransitions);
            adjustCrashAndReboot(currentEnabledTransitions);
            while (true) {
                getOutstandingTcpPacketTransition(currentEnabledTransitions);
                getOutstandingDiskWrite(currentEnabledTransitions);
                if ((currentEnabledTransitions.isEmpty() && feeder.areAllWorkDone()) || numWaitTime >= 20 || numAppliedTransition > 200) {
                    log.info("korn " + currentEnabledTransitions.isEmpty() + " " + feeder.numFinished + " " + (numWaitTime >= 20) + " " + (numAppliedTransition > 200));
                    boolean verifiedResult = verifier.verify(isNodeOnline);
                    String[] data = verifier.getValues(isNodeOnline);
                    String result = verifiedResult + "";
                    for (String d : data) {
                        result += " " + d;
                    }
                    saveResult(result + "\n");
                    exploredBranchRecorder.markBelowSubtreeFinished();
                    for (LinkedList<Transition> pastTransitions : pastEnabledTransitionList) {
                        exploredBranchRecorder.traverseUpward(1);
                        Transition nextTransition = nextTransition(pastTransitions);
                        if (nextTransition == null) {
                            exploredBranchRecorder.markBelowSubtreeFinished();
                        } else {
                            break;
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
                pastEnabledTransitionList.addFirst((LinkedList<Transition>) currentEnabledTransitions.clone());
                Transition nextTransition = nextTransition(currentEnabledTransitions);
                if (nextTransition != null) {
                    exploredBranchRecorder.createChild(nextTransition.getTransitionId());
                    exploredBranchRecorder.traverseDownTo(nextTransition.getTransitionId());
//                    exploredBranchRecorder.noteThisNode(".packet", nextTransition.toString());
                    try {
                        if (nextTransition.apply()) {
                            pathRecordFile.write((getGlobalState() + "," + nextTransition.getTransitionId() + 
                                    " ; " + nextTransition.toString() + "\n").getBytes());
                            updateGlobalState();
                            numAppliedTransition++;
                            if (nextTransition instanceof PacketSendTransition) {
                            } else if (nextTransition instanceof NodeCrashTransition) {
                                NodeCrashTransition crash = (NodeCrashTransition) nextTransition;
                                ListIterator<Transition> iter = currentEnabledTransitions.listIterator();
                                while (iter.hasNext()) {
                                    Transition t = iter.next();
                                    if (t instanceof PacketSendTransition) {
                                        PacketSendTransition p = (PacketSendTransition) t;
                                        if (p.getPacket().getFromId() == crash.getId()) {
                                            iter.remove();
                                        }
                                    } else if (t instanceof DiskWriteTransition) {
                                        DiskWriteTransition w = (DiskWriteTransition) t;
                                        if (w.getWrite().getNodeId() == crash.getId()) {
                                            iter.remove();
                                        }
                                    }
                                }
                                for (ConcurrentLinkedQueue<InterceptPacket> queue : senderReceiverQueues[crash.getId()]) {
                                    queue.clear();
                                }
                                adjustCrashAndReboot(currentEnabledTransitions);
                            } else if (nextTransition instanceof NodeStartTransition) {
                                adjustCrashAndReboot(currentEnabledTransitions);
                            }
                        }
                    } catch (Exception e) {
                        log.error("", e);
                    }
                } else if (exploredBranchRecorder.getCurrentDepth() == 0) {
                    log.warn("Finished exploring all states");
                    zkController.stopEnsemble();
                    System.exit(0);
                } else {
                    log.error("There might be some errors");
                    zkController.stopEnsemble();
                    System.exit(1);
                }
            }
        }
    }
    
}
