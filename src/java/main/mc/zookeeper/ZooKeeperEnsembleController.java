package mc.zookeeper;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import mc.EnsembleController;

public class ZooKeeperEnsembleController implements EnsembleController {
    
    private final static Logger LOG = LoggerFactory.getLogger(ZooKeeperEnsembleController.class);
    
    static final String[] CMD = { "java", "-cp", System.getenv("CLASSPATH"), "-Xmx1G",
        "-Dzookeeper.log.dir=%s/log/%d", "-Dapple.awt.UIElement=true", 
        "-Dlog4j.configuration=%s", "org.apache.zookeeper.server.quorum.QuorumPeerMain", 
        "conf/%d" };
    
    int numNode;
    Process[] zookeeper;
    Thread consoleWriter;
    FileOutputStream[] consoleLog;
    String workingDir;
    
    public ZooKeeperEnsembleController(int numNode, String workingDir) {
        this.numNode = numNode;
        this.workingDir = workingDir;
        zookeeper = new Process[numNode];
        consoleLog = new FileOutputStream[numNode];
        consoleWriter = new Thread(new LogWriter());
        consoleWriter.start();
    }
    
    public void resetTest() {
        for (int i = 0; i < numNode; ++i) {
            if (consoleLog[i] != null) {
                try {
                    consoleLog[i].close();
                } catch (IOException e) {
                    LOG.error("", e);
                }
            }
            try {
                consoleLog[i] = new FileOutputStream(workingDir + "/console/" + i);
            } catch (FileNotFoundException e) {
                LOG.error("", e);
            }
        }
    }
    
    public void startEnsemble() {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Starting ensemble");
        }
        ProcessBuilder builder = new ProcessBuilder();
        builder.environment().put("MC_CONFIG", workingDir + "/zkmc.conf");
        builder.directory(new File(workingDir));
        for (int i = numNode - 1; i >= 0; --i) {
            String[] cmd = Arrays.copyOf(CMD, CMD.length);
            cmd[4] = String.format(cmd[4], workingDir, i);
            cmd[6] = String.format(cmd[6], "zk_log.properties");
            cmd[8] = String.format(cmd[8], i);
            try {
                LOG.debug("Starting node " + i);
                zookeeper[i] = builder.command(cmd).start();
                Thread.sleep(300);
            } catch (InterruptedException e) {
                LOG.error("", e);
                throw new RuntimeException(e);
            } catch (IOException e) {
                LOG.error("", e);
                throw new RuntimeException(e);
            }
        }
    }
    
    public void stopEnsemble() {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Stopping ensemble");
        }
        for (Process node : zookeeper) {
            node.destroy();
        }
        for (Process node : zookeeper) {
            try {
                node.waitFor();
            } catch (InterruptedException e) {
                LOG.error("", e);
            }
        }
    }
    
    public void stopNode(int id) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Stopping node " + id);
        }
        zookeeper[id].destroy();
        try {
            zookeeper[id].waitFor();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
    
    public void startNode(int id) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Starting node" + id);
        }
        ProcessBuilder builder = new ProcessBuilder();
        builder.environment().put("MC_CONFIG", workingDir + "/zkmc.conf");
        builder.directory(new File(workingDir));
        String[] cmd = Arrays.copyOf(CMD, CMD.length);
        cmd[4] = String.format(cmd[4], workingDir, id);
        cmd[6] = String.format(cmd[6], "zk_log.properties");
        cmd[8] = String.format(cmd[8], id);
        try {
            zookeeper[id] = builder.command(cmd).start();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
    
    class LogWriter implements Runnable {

        @Override
        public void run() {
            byte[] buff = new byte[256];
            while (true) {
                for (int i = 0; i < numNode; ++i) {
                    if (zookeeper[i] != null) {
                        int r = 0;
                        InputStream stdout = zookeeper[i].getInputStream();
                        InputStream stderr = zookeeper[i].getErrorStream();
                        try {
                            while((r = stdout.read(buff)) != -1) {
                                consoleLog[i].write(buff, 0, r);
                            }
                            while((r = stderr.read(buff)) != -1) {
                                consoleLog[i].write(buff, 0, r);
                            }
                        } catch (IOException e) {
//                            LOG.debug("", e);
                        }
                    }
                }
                try {
                    Thread.sleep(300);
                } catch (InterruptedException e) {
                    LOG.warn("", e);
                }
            }
        }
        
    }

}
