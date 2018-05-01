package mc.zookeeper.emulator;

import mc.EnsembleController;
import mc.SteadyStateInformedModelChecker;
import mc.WorkloadFeeder;

public class AllPacketLossEmulator extends SteadyStateInformedModelChecker {

    public AllPacketLossEmulator(String interceptorName, String ackName,
            int numNode, String globalStatePathDir,
            EnsembleController zkController, WorkloadFeeder feeder) {
        super(interceptorName, ackName, numNode, globalStatePathDir, zkController,
                feeder);
    }

}
