package mc.zookeeper.quorum;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;

import org.apache.zookeeper.server.DataNode;
import org.apache.zookeeper.server.DataTree;
import org.apache.zookeeper.server.persistence.FileTxnSnapLog;

import mc.SpecVerifier;

public class ConsistentVerifier extends SpecVerifier {
    
    String workingDir;
    File[] dataDir;
    int numNode;
    String path;
    
    public ConsistentVerifier(String workingDir, int numNode, String path) {
        this.workingDir = workingDir;
        this.numNode = numNode;
        dataDir = new File[numNode];
        for (int i = 0; i < numNode; ++i) {
            dataDir[i] = new File(workingDir + "/data/" + i);
        }
        this.path = path;
    }

    @Override
    public boolean verify() {
        HashMap<Long, Integer> sessions = new HashMap<Long, Integer>();
        HashSet<String> dataSet = new HashSet<String>();
        for (File data : dataDir) {
            FileTxnSnapLog txnLog = new FileTxnSnapLog(data, data);
            DataTree dt = new DataTree();
            try {
                txnLog.restore(dt, sessions, null);
            } catch (IOException e) {
                return false;
            }
            DataNode node = dt.getNode(path);
            if (node != null) {
                dataSet.add(new String(node.data));
            }
        }
        return dataSet.size() == 1;
    }

    public boolean verify(boolean[] isOnline) {
        HashMap<Long, Integer> sessions = new HashMap<Long, Integer>();
        HashSet<String> dataSet = new HashSet<String>();
        for (int i = 0; i < numNode; ++i) {
            if (isOnline[i]) {
                FileTxnSnapLog txnLog = new FileTxnSnapLog(dataDir[i], dataDir[i]);
                DataTree dt = new DataTree();
                try {
                    txnLog.restore(dt, sessions, null);
                } catch (IOException e) {
                    return false;
                }
                DataNode node = dt.getNode(path);
                if (node != null) {
                    dataSet.add(new String(node.data));
                }
            }
        }
        return dataSet.size() == 1;
    }
    
    public String[] getValues() {
        String[] values = new String[numNode];
        HashMap<Long, Integer> sessions = new HashMap<Long, Integer>();
        for (int i = 0; i < numNode; ++i) {
            FileTxnSnapLog txnLog = new FileTxnSnapLog(dataDir[i], dataDir[i]);
            DataTree dt = new DataTree();
            try {
                txnLog.restore(dt, sessions, null);
            } catch (IOException e) {
                values[i] = "-exception-";
                continue;
            }
            DataNode node = dt.getNode(path);
            if (node != null) {
                values[i] = new String(node.data);
            } else {
                values[i] = "-null-";
            }
        }
        return values;
    }
    
    public String[] getValues(boolean[] isOnline) {
        String[] values = new String[numNode];
        HashMap<Long, Integer> sessions = new HashMap<Long, Integer>();
        for (int i = 0; i < numNode; ++i) {
            if (isOnline[i]) {
                FileTxnSnapLog txnLog = new FileTxnSnapLog(dataDir[i], dataDir[i]);
                DataTree dt = new DataTree();
                try {
                    txnLog.restore(dt, sessions, null);
                } catch (IOException e) {
                    values[i] = "-exception-";
                    continue;
                }
                DataNode node = dt.getNode(path);
                if (node != null) {
                    values[i] = new String(node.data);
                } else {
                    values[i] = "-null-";
                }
            } else {
                values[i] = "-offline-";
            }
        }
        return values;
    }

}
