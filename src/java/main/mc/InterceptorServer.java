package mc;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.LinkedList;
import java.util.Properties;

import mc.zookeeper.ZKAspectProperties;
import mc.zookeeper.ZooKeeperEnsembleController;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class InterceptorServer {
    
    private static final String DEFAULT_CONFIG_FILE_PATH = "inceptor.conf";
    final static Logger LOG = LoggerFactory.getLogger(InterceptorServer.class);

    public static void main(String[] args) {
        try {
            Registry r = LocateRegistry.getRegistry();
            String configFilePath = DEFAULT_CONFIG_FILE_PATH;
            if (args.length != 0) {
                configFilePath = args[0];
            }
            Properties prop = new Properties();
            FileInputStream configInputStream = new FileInputStream(configFilePath);
            prop.load(configInputStream);
            configInputStream.close();
            String inceptorName = prop.getProperty("mc_name");
            int numNode = Integer.parseInt(prop.getProperty("num_node"));
            String globalStatePathDir = prop.getProperty("global_state_path");
            String traversalRecordDir = prop.getProperty("traversal_record_dir");
            String strategy = prop.getProperty("exploring_strategy", "dfs");
            String workingDir = prop.getProperty("working_dir");
            ZooKeeperEnsembleController zkController = new ZooKeeperEnsembleController(numNode, workingDir);
            CallbackInterceptor inceptor = null;
            WorkloadFeeder feeder = new WorkloadFeeder(new LinkedList<Workload>(), 
                    new LinkedList<SpecVerifier>());
            LOG.info("State exploration strategy is " + strategy);
            String ackName = "LeaderElection";
            if (strategy.equals("level_dfs")) {
            } else if (strategy.equals("level_dpor")) {
            } else if (strategy.equals("level_semantic")) {
            } else if (strategy.equals("packet_dfs")) {
            } else if (strategy.equals("modist_dpor")) {
            } else if (strategy.equals("level_semantic")) {
            } else if (strategy.equals("level_dfs2")) {
                inceptor = new DfsLevelModelChecker(inceptorName, ackName, numNode, 1, 1, globalStatePathDir,
                        traversalRecordDir, zkController, feeder);
            } else if (strategy.equals("node0crash")) {
                inceptor = new FirstNodeCrashModelChecker(inceptorName, ackName, numNode, globalStatePathDir,
                        zkController, feeder);
            } else {
                LOG.error("Sorry you need to do hard code for this strategy");
                System.exit(1);
            }
            CallbackInterceptor inceptorStub = (CallbackInterceptor) 
                    UnicastRemoteObject.exportObject(inceptor, 0);
            r.rebind(prop.getProperty(ZKAspectProperties.INTERCEPTOR_NAME), inceptorStub);
            r.rebind(prop.getProperty(ZKAspectProperties.INTERCEPTOR_NAME) + "TestIdRecorder", inceptorStub);
            r.rebind(prop.getProperty(ZKAspectProperties.INTERCEPTOR_NAME) + "SteadyState", inceptorStub);
            r.rebind(prop.getProperty(ZKAspectProperties.INTERCEPTOR_NAME) + "GlobalStateRecorder", inceptorStub);
        } catch (RemoteException e) {
            e.printStackTrace();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        
    }
    
}
