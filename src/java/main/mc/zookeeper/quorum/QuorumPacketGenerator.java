package mc.zookeeper.quorum;

import java.util.HashMap;
import java.util.List;
import java.util.TreeMap;

import org.apache.jute.Record;

public class QuorumPacketGenerator {
    
    private HashMap<Integer, Integer> packetCount;
    
    public QuorumPacketGenerator() {
        packetCount = new HashMap<Integer, Integer>();
    }

    public RecordPacket createNewRecordPacket(String callbackName, int fromId, int toId, Record record, String tag) {
        int hash = recordHashCodeWithoutId(fromId, toId, record, tag);
        Integer count = packetCount.get(hash);
        if (count == null) {
            count = 0;
        }
        ++count;
        int id = 31 * hash + count;
        packetCount.put(hash, count);
        return new RecordPacket(id, callbackName, fromId, toId, record, tag);
    }
    
    private static int recordHashCodeWithoutId(int fromId, int toId, Record record, String tag) {
        final int prime = 31;
        int result = 1;
        result = prime * result + fromId;
        result = prime * result + toId;
        result = prime * result + (record == null ? 0 : record.hashCode());
        result = prime * result + (tag == null ? 0 : tag.hashCode());
        return result;
    }
    
    public PrimitivePacket createNewQuorumPrimitivePacket(String callbackName, int fromId, int toId, String data, String tag) {
        int hash = quorumPrimitiveHashCodeWithoutId(fromId, toId, data, tag);
        Integer count = packetCount.get(hash);
        if (count == null) {
            count = 0;
        }
        ++count;
        int id = 31 * hash + count;
        packetCount.put(hash, count);
        return new PrimitivePacket(id, callbackName, fromId, toId, data, tag);
    }
    public PrimitivePacket createNewQuorumPrimitivePacket(String callbackName, int fromId, int toId, byte data, String tag) {
        int hash = quorumPrimitiveHashCodeWithoutId(fromId, toId, data, tag);
        Integer count = packetCount.get(hash);
        if (count == null) {
            count = 0;
        }
        ++count;
        int id = 31 * hash + count;
        packetCount.put(hash, count);
        return new PrimitivePacket(id, callbackName, fromId, toId, data, tag);
    }
     
    public PrimitivePacket createNewQuorumPrimitivePacket(String callbackName, int fromId, int toId, boolean data, String tag) {
        int hash = quorumPrimitiveHashCodeWithoutId(fromId, toId, data, tag);
        Integer count = packetCount.get(hash);
        if (count == null) {
            count = 0;
        }
        ++count;
        int id = 31 * hash + count;
        packetCount.put(hash, count);
        return new PrimitivePacket(id, callbackName, fromId, toId, data, tag);
    }
     
    public PrimitivePacket createNewQuorumPrimitivePacket(String callbackName, int fromId, int toId, int data, String tag) {
        int hash = quorumPrimitiveHashCodeWithoutId(fromId, toId, data, tag);
        Integer count = packetCount.get(hash);
        if (count == null) {
            count = 0;
        }
        ++count;
        int id = 31 * hash + count;
        packetCount.put(hash, count);
        return new PrimitivePacket(id, callbackName, fromId, toId, data, tag);
    }
     
    public PrimitivePacket createNewQuorumPrimitivePacket(String callbackName, int fromId, int toId, long data, String tag) {
        int hash = quorumPrimitiveHashCodeWithoutId(fromId, toId, data, tag);
        Integer count = packetCount.get(hash);
        if (count == null) {
            count = 0;
        }
        ++count;
        int id = 31 * hash + count;
        packetCount.put(hash, count);
        return new PrimitivePacket(id, callbackName, fromId, toId, data, tag);
    }
     
    public PrimitivePacket createNewQuorumPrimitivePacket(String callbackName, int fromId, int toId, float data, String tag) {
        int hash = quorumPrimitiveHashCodeWithoutId(fromId, toId, data, tag);
        Integer count = packetCount.get(hash);
        if (count == null) {
            count = 0;
        }
        ++count;
        int id = 31 * hash + count;
        packetCount.put(hash, count);
        return new PrimitivePacket(id, callbackName, fromId, toId, data, tag);
    }
     
    public PrimitivePacket createNewQuorumPrimitivePacket(String callbackName, int fromId, int toId, double data, String tag) {
        int hash = quorumPrimitiveHashCodeWithoutId(fromId, toId, data, tag);
        Integer count = packetCount.get(hash);
        if (count == null) {
            count = 0;
        }
        ++count;
        int id = 31 * hash + count;
        packetCount.put(hash, count);
        return new PrimitivePacket(id, callbackName, fromId, toId, data, tag);
    }

    public PrimitivePacket createNewQuorumPrimitivePacket(String callbackName, int fromId, int toId, List data, String tag) {
        int hash = quorumPrimitiveHashCodeWithoutId(fromId, toId, data, tag);
        Integer count = packetCount.get(hash);
        if (count == null) {
            count = 0;
        }
        ++count;
        int id = 31 * hash + count;
        packetCount.put(hash, count);
        return new PrimitivePacket(id, callbackName, fromId, toId, data, tag);
    } 

    public PrimitivePacket createNewQuorumPrimitivePacket(String callbackName, int fromId, int toId, TreeMap data, String tag) {
        int hash = quorumPrimitiveHashCodeWithoutId(fromId, toId, data, tag);
        Integer count = packetCount.get(hash);
        if (count == null) {
            count = 0;
        }
        ++count;
        int id = 31 * hash + count;
        packetCount.put(hash, count);
        return new PrimitivePacket(id, callbackName, fromId, toId, data, tag);
    }

    private static int quorumPrimitiveHashCodeWithoutId(int fromId, int toId, Object data, String tag) {
        final int prime = 31;
        int result = 1;
        result = prime * result + (data == null ? 0 : data.hashCode());
        result = prime * result + fromId;
        result = prime * result + toId;
        result = prime * result + (tag == null ? 0 : tag.hashCode());
        return result;
    }

}
