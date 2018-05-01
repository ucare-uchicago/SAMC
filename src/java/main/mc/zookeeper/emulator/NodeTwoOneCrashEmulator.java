package mc.zookeeper.emulator;

import java.io.IOException;
import java.util.LinkedList;

import org.apache.log4j.Logger;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.server.quorum.QuorumPacket;

import mc.DiskWrite;
import mc.EnsembleController;
import mc.InterceptPacket;
import mc.SpecVerifier;
import mc.SteadyStateInformedModelChecker;
import mc.Workload;
import mc.WorkloadFeeder;
import mc.transition.PacketSendTransition;
import mc.transition.Transition;
import mc.zookeeper.quorum.RecordPacket;

public class NodeTwoOneCrashEmulator extends SteadyStateInformedModelChecker {
    
    protected LinkedList<InterceptPacket> enabledPackets;

    public NodeTwoOneCrashEmulator(String interceptorName, String ackName,
            int numNode, String globalStatePathDir,
            EnsembleController zkController, WorkloadFeeder feeder) {
        super(interceptorName, ackName, numNode, globalStatePathDir, zkController, feeder);
    }
    
    @Override
    public void resetTest() {
        super.resetTest();
        modelChecking = new Worker(this);
        enabledPackets = new LinkedList<InterceptPacket>();
    }
    
    protected class Worker extends SteadyStateInformedModelChecker.Explorer {
        
        public Worker(SteadyStateInformedModelChecker checker) {
            super(checker);
        }
        
        @Override
        public void run() {
            int type2Num = 0;
            int type4Num = 0;
            while (type4Num < 2) {
                getOutstandingTcpPacket(enabledPackets);
                for (InterceptPacket packet : enabledPackets) {
                    if (packet instanceof RecordPacket) {
                        RecordPacket record = (RecordPacket) packet;
                        if (record.getRecord() instanceof QuorumPacket) {
                            QuorumPacket quorumPacket = (QuorumPacket) record.getRecord();
                            if (quorumPacket.getType() == 1) {
                                continue;
                            } else if (quorumPacket.getType() == 2) {
                                type2Num++;
                            } else if (quorumPacket.getType() == 4) {
                                type4Num++;
                            }
                        }
                    }
                    PacketSendTransition packetTranstion = new PacketSendTransition(checker, packet);
                    packetTranstion.apply();
                    if (type2Num == 2) {
                        while (writeQueue.size() != 3) {
                            try {
                                Thread.sleep(100);
                            } catch (InterruptedException e) {
                                e.printStackTrace();
                            }
                        }
                        DiskWrite write = writeQueue.peek();
                        write(write);
                        write = writeQueue.peek();
                        write(write);
                        write = writeQueue.peek();
                        write(write);
                        type2Num = 0;
                    }
                }
                enabledPackets.clear();
            }
            outer:
            while (true) {
                getOutstandingTcpPacket(enabledPackets);
                for (InterceptPacket packet : enabledPackets) {
                    if (packet instanceof RecordPacket) {
                        RecordPacket record = (RecordPacket) packet;
                        if (record.getRecord() instanceof QuorumPacket) {
                            QuorumPacket quorumPacket = (QuorumPacket) record.getRecord();
                            if (quorumPacket.getType() == 2) {
                                while (writeQueue.size() != 1) {
                                    try {
                                        Thread.sleep(000);
                                    } catch (InterruptedException e) {
                                        e.printStackTrace();
                                    }
                                }
                                DiskWrite write = writeQueue.peek();
                                write(write);
                                log.info("Start kill node");
                                checker.killNode(2);
                                checker.killNode(1);
                                break outer;
                            }
                        }
                    }
                    PacketSendTransition packetTranstion = new PacketSendTransition(checker, packet);
                    packetTranstion.apply();
                }
                enabledPackets.clear();
            }
            enabledPackets.clear();
            checker.runNode(1);
            type2Num = 0;
            type4Num = 0;
            while (type4Num < 2) {
                getOutstandingTcpPacket(enabledPackets);
                for (InterceptPacket packet : enabledPackets) {
                    if (packet instanceof RecordPacket) {
                        RecordPacket record = (RecordPacket) packet;
                        if (record.getRecord() instanceof QuorumPacket) {
                            QuorumPacket quorumPacket = (QuorumPacket) record.getRecord();
                            if (quorumPacket.getType() == 2) {
                                type2Num++;
                            } else if (quorumPacket.getType() == 4) {
                                type4Num++;
                            }
                        }
                    }
                    PacketSendTransition packetTranstion = new PacketSendTransition(checker, packet);
                    packetTranstion.apply();
                    if (type2Num == 2) {
                        while (writeQueue.size() != 2) {
                            try {
                                Thread.sleep(1000);
                            } catch (InterruptedException e) {
                                e.printStackTrace();
                            }
                        }
                        DiskWrite write = writeQueue.peek();
                        write(write);
                        write = writeQueue.peek();
                        write(write);
                        type2Num = 0;
                    }
                }
                enabledPackets.clear();
            }
            checker.runNode(2);
            while (true) {
                while (!writeQueue.isEmpty()) {
                    DiskWrite write = writeQueue.peek();
                    write(write);
                }
                getOutstandingTcpPacket(enabledPackets);
                for (InterceptPacket packet : enabledPackets) {
                    Transition transition = new PacketSendTransition(checker, packet);
                    if (transition.apply()) {
                        updateGlobalState();
                    }
                }
                enabledPackets.clear();
            }
        }
    }
    
    public static class FirstClient extends Workload {
        
        private static final Logger LOG = Logger.getLogger(FirstClient.class);
        
        ZooKeeper zk;
        boolean isConnected;
        boolean hasCreated;
        
        public FirstClient() {
            isConnected = false;
            hasCreated = false;
        }
        
        @Override
        public void reset() {
            if (zk != null) {
                try {
                    zk.close();
                } catch (InterruptedException e) {
                }
            }
            zk = null;
            isConnected = false;
            hasCreated = false;
        }

        @Override
        public void run() {
            Thread t = new Thread(new Runnable() {

                @Override
                public void run() {
                    try {
                        zk = new ZooKeeper("localhost:4002", 15000, new Watcher() {
                            @Override
                            public void process(WatchedEvent event) {
                                if (!hasCreated && event.getState() == KeeperState.SyncConnected) {
                                    isConnected = true;
                                    LOG.info("Second connected");
                                } else if (event.getState() == KeeperState.Disconnected) {
                                    isConnected = false;
                                }
                            }
                        });
                        while (!isConnected) {
                            try {
                                Thread.sleep(100);
                            } catch (InterruptedException e) {
                                e.printStackTrace();
                            }
                        }
                        LOG.info("Try to write first");
                        zk.create("/data", "first".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                        hasCreated = true;
                        zk.close();
                        finish();
                    } catch (IOException e) {
                        e.printStackTrace();
                    } catch (KeeperException e) {
                        try {
                            zk.close();
                            finish();
                        } catch (InterruptedException e1) {
                            e1.printStackTrace();
                        }
                    } catch (InterruptedException e) {
                        try {
                            zk.close();
                            finish();
                        } catch (InterruptedException e1) {
                            e1.printStackTrace();
                        }
                    }
                }
                
            });
            t.start();
        }

        @Override
        public void stop() {
            
        }
        
    }
    
    public static class SecondClient extends Workload {
        
        private static final Logger LOG = Logger.getLogger(SecondClient.class);
        
        ZooKeeper zk;
        boolean isConnected;
        boolean hasCreated;
        
        public SecondClient() {
            isConnected = false;
            hasCreated = false;
        }
        
        @Override
        public void reset() {
            if (zk != null) {
                try {
                    zk.close();
                } catch (InterruptedException e) {
                }
            }
            zk = null;
            isConnected = false;
            hasCreated = false;
        }

        @Override
        public void run() {
            Thread t = new Thread(new Runnable() {

                @Override
                public void run() {
                    try {
                        zk = new ZooKeeper("localhost:4001", 15000, new Watcher() {
                            @Override
                            public void process(WatchedEvent event) {
                                if (!hasCreated && event.getState() == KeeperState.SyncConnected) {
                                    LOG.info("Second connected");
                                    isConnected = true;
                                } else if (event.getState() == KeeperState.Disconnected) {
                                    isConnected = false;
                                }
                            }
                        });
                        while (!isConnected) {
                            try {
                                Thread.sleep(100);
                            } catch (InterruptedException e) {
                                e.printStackTrace();
                            }
                        }
                        while (!hasCreated) {
                            try {
                                LOG.info("Try to write second");
                                zk.create("/data", "second".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                            } catch (KeeperException e) {
                                e.printStackTrace();
                                continue;
                            } catch (InterruptedException e) {
                                e.printStackTrace();
                                continue;
                            }
                            hasCreated = true;
                        }    
                        try {
                            zk.close();
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                        finish();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
                
            });
            t.start();
        }

        @Override
        public void stop() {
            
        }
        
    }
    
    public static WorkloadFeeder getIssue335WorkloadFeeder() {
        LinkedList<Workload> allWorkloads = new LinkedList<Workload>();
        allWorkloads.add(new FirstClient());
        allWorkloads.add(new SecondClient());
        return new WorkloadFeeder(allWorkloads, new LinkedList<SpecVerifier>());
    }

}