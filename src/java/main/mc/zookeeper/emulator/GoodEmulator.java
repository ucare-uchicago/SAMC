package mc.zookeeper.emulator;

import java.util.LinkedList;

import mc.DiskWrite;
import mc.EnsembleController;
import mc.InterceptPacket;
import mc.SteadyStateInformedModelChecker;
import mc.WorkloadFeeder;
import mc.transition.Transition;

public class GoodEmulator extends SteadyStateInformedModelChecker {
    
    protected LinkedList<Transition> currentLevelPackets;

    public GoodEmulator(String interceptorName, String ackName,
            int numNode, String globalStatePathDir,
            EnsembleController zkController, WorkloadFeeder feeder) {
        super(interceptorName, ackName, numNode, globalStatePathDir, zkController, feeder);
    }
    
    @Override
    public void resetTest() {
        super.resetTest();
        modelChecking = new Worker(this);
        currentLevelPackets = new LinkedList<Transition>();
    }
    
    protected class Worker extends SteadyStateInformedModelChecker.Explorer {
        
        public Worker(SteadyStateInformedModelChecker checker) {
            super(checker);
        }
        
        @Override
        public void run() {
            LinkedList<InterceptPacket> thisLevelPackets = new LinkedList<InterceptPacket>();
            while (true) {
                while (!writeQueue.isEmpty()) {
                    DiskWrite write = writeQueue.peek();
                    try {
                        writeAndWait(write);
                    } catch (InterruptedException e) {
                        log.error(e.getMessage());
                    }
                }
                thisLevelPackets.clear();
                getOutstandingTcpPacketTransition(currentLevelPackets);
                for (Transition packet : currentLevelPackets) {
                    if (packet.apply()) {
                        updateGlobalState();
                    }
                }
                currentLevelPackets.clear();
            }
        }
    }
    
}