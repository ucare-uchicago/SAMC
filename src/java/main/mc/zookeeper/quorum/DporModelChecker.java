package mc.zookeeper.quorum;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.Set;

import mc.WorkloadFeeder;
import mc.transition.AbstractNodeCrashTransition;
import mc.transition.AbstractNodeOperationTransition;
import mc.transition.AbstractNodeStartTransition;
import mc.transition.DiskWriteTransition;
import mc.transition.NodeCrashTransition;
import mc.transition.NodeOperationTransition;
import mc.transition.PacketSendTransition;
import mc.transition.Transition;
import mc.transition.TransitionTuple;
import mc.zookeeper.ZooKeeperEnsembleController;

public class DporModelChecker extends PrototypeSamc {
    
    public DporModelChecker(String interceptorName, String ackName, int maxId,
            int numCrash, int numReboot, String globalStatePathDir, String packetRecordDir,
            ZooKeeperEnsembleController zkController, WorkloadFeeder feeder) {
        this(interceptorName, ackName, maxId, numCrash, numReboot, globalStatePathDir, packetRecordDir, "/tmp", zkController, feeder);
    }
    
    public DporModelChecker(String inceptorName, String ackName, int maxId,
            int numCrash, int numReboot, String globalStatePathDir, String packetRecordDir, String cacheDir,
            ZooKeeperEnsembleController zkController, WorkloadFeeder feeder) {
        super(inceptorName, ackName, maxId, numCrash, numReboot, globalStatePathDir, packetRecordDir, cacheDir, zkController, feeder);
    }
    
    protected boolean isDependent(Transition t, Transition s) {
        if (t instanceof PacketSendTransition && s instanceof PacketSendTransition) {
            return ((PacketSendTransition) t).getPacket().getToId() == ((PacketSendTransition) s).getPacket().getToId();
        }
        return true;
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
                if (enabledPackets.contains(lastTransition.transition) || lastTransition.transition instanceof NodeOperationTransition) {
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
                            continue;
//                            if (lastPacket.getPacket().isObsolete()) {
//                                if (tuple.transition instanceof NodeCrashTransition || tuple.transition instanceof AbstractNodeCrashTransition) {
//                                    if (lastPacket.getPacket().getObsoleteBy() == index) {
//                                        addNewDporInitialPath(tmpPath, tuple, new TransitionTuple(0, lastTransition.transition));
//                                        break;
//                                    }
//                                }
//                            } else {
//                                addNewDporInitialPath(tmpPath, tuple, new TransitionTuple(0, lastTransition.transition));
//                                break;
//                            }
                        } else if (tuple.transition instanceof DiskWriteTransition) {
                            DiskWriteTransition diskWrite = (DiskWriteTransition) tuple.transition;
                            if (diskWrite.getWrite().getNodeId() == lastPacket.getPacket().getToId()) {
                                addNewDporInitialPath(tmpPath, tuple, new TransitionTuple(0, lastTransition.transition));
                                break;
                            }
                            continue;
                        }
                    } else if (lastTransition.transition instanceof AbstractNodeCrashTransition) {
                        if (tuple.transition instanceof NodeOperationTransition) {
                            if (((NodeOperationTransition) tuple.transition).id == ((AbstractNodeOperationTransition) lastTransition.transition).id) {
                                break;
                            }
                        } else if (tuple.transition instanceof PacketSendTransition) {
                            if (((PacketSendTransition) tuple.transition).getPacket().isObsolete()) {
                                continue;
                            } else {
                                addNewDporInitialPath(tmpPath, tuple, new TransitionTuple(0, lastTransition.transition));
                                break;
                            }
                        } else if (tuple.transition instanceof DiskWriteTransition) {
                            if (((DiskWriteTransition) tuple.transition).isObsolete()) {
                                continue;
                            } else {
                                LinkedList<TransitionTuple> interestingPath = (LinkedList<TransitionTuple>) tmpPath.clone();
                                interestingPath.add(new TransitionTuple(0, 
                                        new NodeCrashTransition(DporModelChecker.this, 
                                                ((DiskWriteTransition) tuple.transition).getWrite().getNodeId())));
                                addToDporInitialPathList(interestingPath);
                                // use coninue instead of break because this abstract transition so there 
                                // might be write from other node dependent with this abstract crash
//                                break;
                                continue;
                            }
                        }
                    } else if (lastTransition.transition instanceof AbstractNodeStartTransition) {
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
//                            } else if (((DiskWriteTransition) tuple.transition).getWrite().getNodeId() != ((AbstractNodeOperationTransition) lastTransition.transition).getId()) {
//                                continue;
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
                            continue;
//                            if ((!write.isObsolete() && write.getWrite().getNodeId() == ((NodeOperationTransition) tuple.transition).getId()) || 
//                                    (write.isObsolete() && index == write.getObsoleteBy())) {
//                                addNewDporInitialPath(tmpPath, tuple, new TransitionTuple(0, lastTransition.transition));
//                                break;
//                            } else {
//                                continue;
//                            }
                        } else if (tuple.transition instanceof DiskWriteTransition) {
                            if (write.isObsolete()) {
                                continue;
                            } else if (((DiskWriteTransition) tuple.transition).isObsolete()) {
                                continue;
                            } else if (write.getWrite().getNodeId() == ((DiskWriteTransition) tuple.transition).getWrite().getNodeId()) {
                                addNewDporInitialPath(tmpPath, tuple, new TransitionTuple(0, lastTransition.transition));
                                break;
                            }
                        }
                    }
                } else {
                    break;
                }
            }
        }
    }
    
}
