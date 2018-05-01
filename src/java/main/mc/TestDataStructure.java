package mc;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;

import org.apache.zookeeper.server.DataNode;
import org.apache.zookeeper.server.DataTree;
import org.apache.zookeeper.server.persistence.FileTxnSnapLog;

public class TestDataStructure {

    public static void main(String[] args) throws IOException {
        FileTxnSnapLog txnLog = new FileTxnSnapLog(new File("/tmp/zkchecking/data/1"), new File("/tmp/zkchecking/data/2"));
        DataTree dt = new DataTree();
        HashMap<Long, Integer> sessions = new HashMap<Long, Integer>();
        txnLog.restore(dt, sessions, null);
        DataNode node = dt.getNode("/data");
        System.out.println(new String(node.data));
    }

}
