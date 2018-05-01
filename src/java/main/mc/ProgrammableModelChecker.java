package mc;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.rmi.RemoteException;
import java.util.LinkedList;

import org.apache.zookeeper.server.quorum.QuorumPeer.ServerState;

import mc.transition.DiskWriteTransition;
import mc.transition.NodeCrashTransition;
import mc.transition.NodeStartTransition;
import mc.transition.PacketSendTransition;
import mc.transition.Transition;
import mc.zookeeper.ZooKeeperEnsembleController;
import mc.zookeeper.quorum.ZabInfoRecorder;

public class ProgrammableModelChecker extends SteadyStateInformedModelChecker implements ZabInfoRecorder {
    
    protected ProgramParser parser;
    protected LinkedList<InterceptPacket> enabledPackets;
    protected Thread afterProgramModelChecker;
    protected File program;
    
    public ProgrammableModelChecker(String interceptorName, String ackName, 
            int numNode, String globalStatePathDir, File program, 
            ZooKeeperEnsembleController zkController, WorkloadFeeder feeder) throws FileNotFoundException {
        super(interceptorName, ackName, numNode, globalStatePathDir, zkController, feeder);
        this.program = program;
        afterProgramModelChecker = null;
        resetTest();
    }
    
    @Override
    public void resetTest() {
        super.resetTest();
        try {
            if (program != null) {
                parser = new ProgramParser(this, program);
            }
        } catch (FileNotFoundException e) {
            throw new RuntimeException(e.getMessage());
        }
        modelChecking = new ProgramExecutor(this);
        enabledPackets = new LinkedList<InterceptPacket>();
    }
    
    class ProgramExecutor extends SteadyStateInformedModelChecker.Explorer {

        public ProgramExecutor(SteadyStateInformedModelChecker checker) {
            super(checker);
        }
        
        @Override
        public void run() {
            InstructionTransition instruction;
            while (parser != null && (instruction = parser.readNextInstruction()) != null) {
                getOutstandingTcpPacketTransition(currentEnabledTransitions);
                getOutstandingDiskWrite(currentEnabledTransitions);
//                log.info("korn " + enabledPackets.toString());
//                log.info("korn " + currentEnabledTransitions.toString());
                Transition transition = instruction.getRealTransition(checker);
                if (transition == null) {
                    break;
                }
                if (transition.apply()) {
                    updateGlobalState();
                } else {
                    
                }
                if (transition instanceof PacketSendTransition || transition instanceof DiskWriteTransition) {
                    currentEnabledTransitions.remove(transition);
                }
            }
            if (afterProgramModelChecker != null) {
                afterProgramModelChecker.start();
            }
        }
        
    }
    
    class ProgramParser {
        
        BufferedReader programReader;
        
        public ProgramParser(ModelChecker checker, File program) throws FileNotFoundException {
            this.programReader = new BufferedReader(new FileReader(program));
        }
        
        public InstructionTransition readNextInstruction() {
            try {
                String transitionString = programReader.readLine();
                if (transitionString == null) {
                    return null;
                }
                String[] tokens = transitionString.split(" ");
                if (tokens[0].equals("packetsend")) {
                    String packetIdString = tokens[1].split("=")[1];
                    if (packetIdString.equals("*")) {
                        return new PacketSendInstuctionTransition(0);
                    } else {
                        long packetId = Long.parseLong(packetIdString);
                        return new PacketSendInstuctionTransition(packetId);
                    }
                } else if (tokens[0].equals("diskwrite")) {
                    String writeIdString = tokens[1].split("=")[1];
                    if (writeIdString.equals("*")) {
                        return new PacketSendInstuctionTransition(0);
                    } else {
                        int writeId = Integer.parseInt(writeIdString);
                        return new DiskWriteInstructionTransition(writeId);
                    }
                } else if (tokens[0].equals("nodecrash")) {
                    int id = Integer.parseInt(tokens[1].split("=")[1]);
                    return new NodeCrashInstructionTransition(id);
                } else if (tokens[0].equals("nodestart")) {
                    int id = Integer.parseInt(tokens[1].split("=")[1]);
                    return new NodeStartInstructionTransition(id);
                } else if (tokens[0].equals("sleep")) {
                    long sleep = Long.parseLong(tokens[1].split("=")[1]);
                    return new SleepInstructionTransition(sleep);
                } else if (tokens[0].equals("workload")) {
                    return new WorkloadInstructionTransition();
                } else if (tokens[0].equals("stop")) {
                    return new ExitInstructionTransaction();
                }
            } catch (IOException e) {
                return null;
            }
            return null;
        }
        
    }
    
    abstract class InstructionTransition {

        abstract Transition getRealTransition(ModelChecker checker);

    }
    
    class PacketSendInstuctionTransition extends InstructionTransition {
        
        long packetId;
        
        public PacketSendInstuctionTransition(long packetId) {
            this.packetId = packetId;
        }
        
        @Override
        Transition getRealTransition(ModelChecker checker) {
            for (int i = 0; i < 15; ++i) {
                for (Transition t : currentEnabledTransitions) {
                    if (t instanceof PacketSendTransition) {
                        PacketSendTransition p = (PacketSendTransition) t;
                        if (packetId == 0) {
                            return p;
                        } else if (p.getPacket().getId() == packetId) {
                            return p;
                        }
                    }
                }
                try {
                    log.info("korn wait for new packet");
                    Thread.sleep(500);
                } catch (InterruptedException e) {
                    log.error("", e);
                }
                getOutstandingTcpPacketTransition(currentEnabledTransitions);
                log.info("korn " + currentEnabledTransitions);
            }
            throw new RuntimeException("No expected enabled packet for " + packetId);
        }
        
    }
    
    class NodeCrashInstructionTransition extends InstructionTransition {
        
        int id;

        private NodeCrashInstructionTransition(int id) {
            this.id = id;
        }

        @Override
        Transition getRealTransition(ModelChecker checker) {
            return new NodeCrashTransition(checker, id);
        }
        
    }
    
    class NodeStartInstructionTransition extends InstructionTransition {
        
        int id;

        private NodeStartInstructionTransition(int id) {
            this.id = id;
        }

        @Override
        Transition getRealTransition(ModelChecker checker) {
            return new NodeStartTransition(checker, id);
        }
        
    }
    
    class DiskWriteInstructionTransition extends InstructionTransition {
        
        int writeId;

        private DiskWriteInstructionTransition(int writeId) {
            this.writeId = writeId;
        }

        @Override
        Transition getRealTransition(ModelChecker checker) {
            for (int i = 0; i < 15; ++i) {
                for (Transition t : currentEnabledTransitions) {
                    if (t instanceof DiskWriteTransition) {
                        DiskWriteTransition w = (DiskWriteTransition) t;
                        if (writeId == 0) {
                            return w;
                        } else if (w.getWrite().getWriteId() == writeId) {
                            return w;
                        }
                    }
                }
                try {
                    log.info("korn wait for new diskwrite");
                    Thread.sleep(500);
                } catch (InterruptedException e) {
                    log.error("", e);
                }
                getOutstandingDiskWrite(currentEnabledTransitions);
            }
            throw new RuntimeException("No expected enabled diskwrite for " + writeId);
        }
        
    }
    
    class SleepInstructionTransition extends InstructionTransition {
        
        long sleep;
        
        private SleepInstructionTransition(long sleep) {
            this.sleep = sleep;
        }

        @Override
        Transition getRealTransition(ModelChecker checker) {
            return new Transition() {

                @Override
                public boolean apply() {
                    try {
                        Thread.sleep(sleep);
                    } catch (InterruptedException e) {
                        return false;
                    }
                    return true;
                }

                @Override
                public int getTransitionId() {
                    return 0;
                }
                
            };
        }
    }
    
    class WorkloadInstructionTransition extends InstructionTransition {
        
        ModelChecker checker;

        @Override
        Transition getRealTransition(final ModelChecker checker) {
            return new Transition() {
                @Override
                public int getTransitionId() {
                    return 0;
                }
                
                @Override
                public boolean apply() {
                    log.info("korn stupid");
                    checker.feeder.runAll();
                    return true;
                }
            };
        }
        
    }
    
    class ExitInstructionTransaction extends InstructionTransition {

        @Override
        Transition getRealTransition(ModelChecker checker) {
            return new Transition() {
                
                @Override
                public int getTransitionId() {
                    return 0;
                }
                
                @Override
                public boolean apply() {
                    System.exit(0);
                    return true;
                }
            };
        }
        
    }

    @Override
    public void setRole(int id, ServerState role) throws RemoteException {
        
    }

    @Override
    public void setLatestTxId(int id, long txId) throws RemoteException {
        
    }

    @Override
    public void setMaxCommittedLog(int id, long maxCommitedLog)
            throws RemoteException {
        
    }

}