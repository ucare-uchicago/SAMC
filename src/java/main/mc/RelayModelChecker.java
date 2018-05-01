package mc;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.LinkedList;

import mc.transition.PacketSendTransition;
import mc.zookeeper.ZooKeeperEnsembleController;

public class RelayModelChecker extends ProgrammableModelChecker {
    
    protected LinkedList<PacketSendTransition> currentLevelPackets;
    
    public RelayModelChecker(String interceptorName, String ackName,
            int numNode, String globalStatePathDir,
            ZooKeeperEnsembleController zkController, WorkloadFeeder feeder)
            throws FileNotFoundException {
        super(interceptorName, ackName, numNode, globalStatePathDir, null,
                zkController, feeder);
        currentLevelPackets = new LinkedList<PacketSendTransition>();
        resetTest();
    }

    public RelayModelChecker(String interceptorName, String ackName,
            int numNode, String globalStatePathDir, File program,
            ZooKeeperEnsembleController zkController, WorkloadFeeder feeder)
            throws FileNotFoundException {
        super(interceptorName, ackName, numNode, globalStatePathDir, program,
                zkController, feeder);
        currentLevelPackets = new LinkedList<PacketSendTransition>();
        resetTest();
    }
    
    @Override
    public void resetTest() {
        if (currentLevelPackets == null) {
            return;
        }
        super.resetTest();
        afterProgramModelChecker = new RelayWorker(this);
        currentLevelPackets.clear();
    }
    
    protected class RelayWorker extends SteadyStateInformedModelChecker.Explorer {

        public RelayWorker(SteadyStateInformedModelChecker checker) {
            super(checker);
        }
        
        public void run() {
            currentLevelPackets = PacketSendTransition.buildTransitions(checker, enabledPackets); 
            LinkedList<InterceptPacket> thisLevelPackets = new LinkedList<InterceptPacket>();
            while (true) {
                while (!writeQueue.isEmpty()) {
                    DiskWrite write = writeQueue.remove();
                    try {
                        writeAndWait(write);
                    } catch (InterruptedException e) {
                        log.error(e.getMessage());
                    }
                }
                thisLevelPackets.clear();
                getOutstandingTcpPacket(thisLevelPackets);
                currentLevelPackets.addAll(PacketSendTransition.buildTransitions(checker, thisLevelPackets));
                for (PacketSendTransition packet : currentLevelPackets) {
                    if (packet.apply()) {
                        updateGlobalState();
                    }
                }
                currentLevelPackets.clear();
            }
        }
        
    }

}
