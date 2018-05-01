package mc.zookeeper;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.LinkedList;
import java.util.Properties;

import mc.CallbackInterceptor;
import mc.ModelChecker;
import mc.ProgrammableModelChecker;
import mc.RelayModelChecker;
import mc.SpecVerifier;
import mc.Workload;
import mc.WorkloadFeeder;
import mc.zookeeper.emulator.AllPacketLossEmulator;
import mc.zookeeper.emulator.GoodEmulator;
import mc.zookeeper.emulator.Issue335Emulator;
import mc.zookeeper.emulator.NodeTwoOneCrashEmulator;
import mc.zookeeper.quorum.ConsistentVerifier;
import mc.zookeeper.quorum.DfsTreeTravelModelChecker;
import mc.zookeeper.quorum.DporModelChecker;
import mc.zookeeper.quorum.RandomDporModelChecker;
import mc.zookeeper.quorum.RandomModelChecker;
import mc.zookeeper.quorum.SemanticAwareModelChecker;
import mc.zookeeper.quorum.SemanticAwareModelChecker2;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ZooKeeperTestRunner {
    
    final static Logger LOG = LoggerFactory.getLogger(ZooKeeperTestRunner.class);
    
    static WorkloadFeeder feeder;
    
    public static void main(String[] argv) throws IOException {
        String testRunnerConf = null;
        if (argv.length == 0) {
            System.err.println("Please specify test config file");
            System.exit(1);
        }
        boolean isPasuedEveryTest = false;
        for (String param : argv) {
            if (param.equals("-p")) {
                isPasuedEveryTest = true;
            } else {
                testRunnerConf = param;
            }
        }
        Properties testRunnerProp = new Properties();
        FileInputStream configInputStream = new FileInputStream(testRunnerConf);
        testRunnerProp.load(configInputStream);
        configInputStream.close();
        String workingDir = testRunnerProp.getProperty("working_dir");
        int numNode = Integer.parseInt(testRunnerProp.getProperty("num_node"));
        ZooKeeperEnsembleController zkController = 
                new ZooKeeperEnsembleController(numNode, workingDir);
        ModelChecker checker = createLeaderElectionModelCheckerFromConf(workingDir + "/mc.conf", workingDir, zkController);
        startExploreTesting(checker, numNode, workingDir, zkController, isPasuedEveryTest);
    }
    
    protected static ModelChecker createLeaderElectionModelCheckerFromConf(String confFile, 
            String workingDir, ZooKeeperEnsembleController zkController) {
        CallbackInterceptor modelChecker = null;
        try {
            Properties prop = new Properties();
            FileInputStream configInputStream = new FileInputStream(confFile);
            prop.load(configInputStream);
            configInputStream.close();
            String interceptorName = prop.getProperty("mc_name");
            int numNode = Integer.parseInt(prop.getProperty("num_node"));
            String testRecordDir = prop.getProperty("test_record_dir");
            String traversalRecordDir = prop.getProperty("traversal_record_dir");
            String strategy = prop.getProperty("exploring_strategy", "dfs");
            int numCrash = Integer.parseInt(prop.getProperty("num_crash"));
            int numReboot = Integer.parseInt(prop.getProperty("num_reboot"));
            String programFile = prop.getProperty("program");
            File program = programFile == null ? null : new File(programFile);
            String ackName = "Ack";
            LinkedList<SpecVerifier> specVerifiers = new LinkedList<SpecVerifier>();
            specVerifiers.add(new ConsistentVerifier(workingDir, numNode, "/data"));
            LinkedList<Workload> workload = new LinkedList<Workload>();
            feeder = new WorkloadFeeder(workload, specVerifiers);
            workload.add(new Issue335Emulator.OneClient(feeder));
            LOG.info("State exploration strategy is " + strategy);
            if (strategy.equals("level_dfs")) {

            } else if (strategy.equals("packet_dfs")) {
                modelChecker = new DfsTreeTravelModelChecker(interceptorName, ackName, numNode, numCrash, numReboot, 
                        testRecordDir, traversalRecordDir, zkController, feeder);
            } else if (strategy.equals("random")) {
                modelChecker = new RandomModelChecker(interceptorName, ackName, numNode, numCrash, numReboot, 
                        testRecordDir, traversalRecordDir, zkController, feeder);
            } else if (strategy.equals("program")) {
                modelChecker = new ProgrammableModelChecker(interceptorName, ackName, numNode, 
                        testRecordDir, program, zkController, feeder);
            } else if (strategy.equals("relay")) {
                modelChecker = new RelayModelChecker(interceptorName, ackName, numNode, testRecordDir, program, zkController, feeder);
            } else if (strategy.equals("335")) {
                feeder = Issue335Emulator.getIssue335WorkloadFeeder();
                modelChecker = new Issue335Emulator(interceptorName, ackName, numNode, testRecordDir, zkController, feeder);
            } else if (strategy.equals("good")) {
                feeder = Issue335Emulator.getIssue335WorkloadFeeder();
                modelChecker = new GoodEmulator(interceptorName, ackName, numNode, testRecordDir, zkController, feeder);
            } else if (strategy.equals("n21c")) {
                feeder = NodeTwoOneCrashEmulator.getIssue335WorkloadFeeder();
                modelChecker = new NodeTwoOneCrashEmulator(interceptorName, ackName, numNode, testRecordDir, zkController, feeder);
            } else if (strategy.equals("loss")) {
                modelChecker = new AllPacketLossEmulator(interceptorName, ackName, numNode, testRecordDir, zkController, feeder);
            } else if (strategy.equals("packet_dpor")) {
                modelChecker = new DporModelChecker(interceptorName, ackName, numNode, numCrash, numReboot,
                        testRecordDir, traversalRecordDir, traversalRecordDir, zkController, feeder);
            } else if (strategy.equals("random_dpor")) {
                modelChecker = new RandomDporModelChecker(interceptorName, ackName, numNode, numCrash, numReboot,
                        testRecordDir, traversalRecordDir, traversalRecordDir, zkController, feeder);
            } else if (strategy.equals("packet_semantic")) {
                modelChecker = new SemanticAwareModelChecker(interceptorName, ackName, numNode, numCrash, numReboot,
                        testRecordDir, traversalRecordDir, traversalRecordDir, zkController, feeder);
            } else if (strategy.equals("packet_semantic2")) {
                modelChecker = new SemanticAwareModelChecker2(interceptorName, ackName, numNode, numCrash, numReboot,
                        testRecordDir, traversalRecordDir, traversalRecordDir, zkController, feeder);
            } else {
                LOG.error("Sorry you need to do hard code for this strategy");
                System.exit(1);
            }
            CallbackInterceptor interceptorStub = (CallbackInterceptor) 
                    UnicastRemoteObject.exportObject(modelChecker, 0);
            Registry r = LocateRegistry.getRegistry();
            r.rebind(interceptorName, interceptorStub);
            r.rebind(interceptorName + "SteadyState", interceptorStub);
            r.rebind(interceptorName + "LeaderElectGlobalStateRecorder", interceptorStub);
            r.rebind(interceptorName + "LeaderElectTestIdRecorder", interceptorStub);
            
        } catch (RemoteException e) {
            e.printStackTrace();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return (ModelChecker) modelChecker;
    }
    
    protected static void startExploreTesting(ModelChecker checker, int numNode, String workingDir, 
            ZooKeeperEnsembleController zkController, boolean isPausedEveryTest) throws IOException {
        File gspathDir = new File(workingDir + "/record");
        int testNum = gspathDir.list().length + 1;
        File finishedFlag = new File(workingDir + "/state/.finished");
        File waitingFlag = new File(workingDir + "/state/.waiting");
        try {
            for (; !finishedFlag.exists(); ++testNum) {
                waitingFlag.delete();
                checker.setTestId(testNum);
                Process reset = Runtime.getRuntime().exec("resettest " + numNode + 
                        " " + workingDir);
                reset.waitFor();
                /*
                Process setTestId = Runtime.getRuntime().exec("setzkmc_testid " + 
                        testNum + " " + workingDir);
                setTestId.waitFor();
                */
                zkController.resetTest();
//                zkController.startEnsemble();
                checker.runEnsemble();
//                feeder.runAll();
                while (!waitingFlag.exists()) {
                    Thread.sleep(100);
                }
//                zkController.stopEnsemble();
                checker.stopEnsemble();
//                feeder.stopAll();
//                feeder.resetAll();
                if (isPausedEveryTest) {
                    System.out.print("enter to continue");
                    System.in.read();
                }
            }
            System.exit(0);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

}
