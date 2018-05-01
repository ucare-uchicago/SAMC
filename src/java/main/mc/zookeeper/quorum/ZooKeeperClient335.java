package mc.zookeeper.quorum;

import java.io.IOException;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;

public class ZooKeeperClient335 extends Thread {
    
    ZooKeeper zk;
    String path;
    String value;
    boolean isConnected;
    boolean isSuccess;
    
    public ZooKeeperClient335(int nodeId, String path, String value) throws IOException {
        this.path = path;
        this.value = value;
        isConnected = false;
        isSuccess = false;
        zk = new ZooKeeper("localhost:" + (4000 + nodeId), 9000, new Watcher() {
            
            @Override
            public void process(WatchedEvent event) {
                if (event.getState() == KeeperState.SyncConnected) {
                    isConnected = true;
                }
            }
        });
    }
    
    @Override
    public void run() {
        while (!isConnected) {
            try {
                Thread.sleep(200);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        try {
            zk.create(path, value.getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            zk.close();
            isSuccess = true;
        } catch (KeeperException e) {
            e.printStackTrace();
            try {
                zk.close();
            } catch (InterruptedException e1) {
                e1.printStackTrace();
            }
            isSuccess = false;
        } catch (InterruptedException e) {
            e.printStackTrace();
            try {
                zk.close();
            } catch (InterruptedException e1) {
                e1.printStackTrace();
            }
            isSuccess = false;
        }
    }
    
}
