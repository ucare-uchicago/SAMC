package mc.zookeeper;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

import org.apache.zookeeper.server.quorum.QuorumPeer.ServerState;

public class ZooKeeperLogExtractor {
    
    String workingDir;
    
    public ZooKeeperLogExtractor(String workingDir) {
        this.workingDir = workingDir;
    }
    
    public ServerState getServerState(int nodeId) {
        String logFileName = workingDir + "/log/" + nodeId + "/zookeeper.log";
        BufferedReader reader;
        try {
            reader = new BufferedReader(new FileReader(logFileName));
            String line;
            ServerState state = ServerState.LOOKING;
            try {
                while ((line = reader.readLine()) != null) {
                    String[] token = line.split("-");
                    if (token.length > 3) {
                        if (token[4].trim().equals("LEADING")) {
                            state = ServerState.LEADING;
                        } else if (token[4].trim().equals("FOLLOWING")) {
                            state = ServerState.FOLLOWING;
                        } else if (token[4].trim().equals("LOOKING")) {
                            state = ServerState.LOOKING;
                        }
                    }
                }
                reader.close();
                return state;
            } catch (IOException e) {
                e.printStackTrace();
            }
        } catch (FileNotFoundException e1) {
            e1.printStackTrace();
        }
        return ServerState.LOOKING;
    }

}
