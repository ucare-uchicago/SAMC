package mc.zookeeper.quorum;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.TreeMap;

public class PrimitivePacket extends InterceptQuorumPacket {
    
    public static final int STRING = 0;
    public static final int BYTE = 1;
    public static final int BOOLEAN = 2;
    public static final int INTEGER = 3;
    public static final int LONG = 4;
    public static final int FLOAT = 5;
    public static final int DOUBLE = 6;
    public static final int VECTOR = 7;
    public static final int MAP = 8;
    
    protected int dataType;
    
    public PrimitivePacket(int id, String callbackName, int fromId, int toId, String data, String tag) {
        dataType = STRING;
        this.tag = tag;
        this.id = id;
        this.callbackId = callbackName;
        this.fromId = fromId;
        this.toId = toId;
        this.data = data.getBytes();
    }
    
    public PrimitivePacket(int id, String callbackName, int fromId, int toId, byte data, String tag) {
        dataType = BYTE;
        this.tag = tag;
        this.id = id;
        this.callbackId = callbackName;
        this.fromId = fromId;
        this.toId = toId;
        this.data = new byte[] { data };
    }
    
    public PrimitivePacket(int id, String callbackName, int fromId, int toId, boolean data, String tag) {
        dataType = STRING;
        this.tag = tag;
        this.id = id;
        this.callbackId = callbackName;
        this.fromId = fromId;
        this.toId = toId;
        this.data = data ? new byte[] { 1 } : new byte[] { 0 };
    }

    public PrimitivePacket(int id, String callbackName, int fromId, int toId, int data, String tag) {
        dataType = INTEGER;
        this.tag = tag;
        this.id = id;
        this.callbackId = callbackName;
        this.fromId = fromId;
        this.toId = toId;
        this.data = ByteBuffer.allocate(Integer.SIZE).putInt(data).array();
    }
    
    public PrimitivePacket(int id, String callbackName, int fromId, int toId, long data, String tag) {
        dataType = LONG;
        this.tag = tag;
        this.id = id;
        this.callbackId = callbackName;
        this.fromId = fromId;
        this.toId = toId;
        this.data = ByteBuffer.allocate(Long.SIZE).putLong(data).array();
    }
    
    public PrimitivePacket(int id, String callbackName, int fromId, int toId, float data, String tag) {
        dataType = FLOAT;
        this.tag = tag;
        this.id = id;
        this.callbackId = callbackName;
        this.fromId = fromId;
        this.toId = toId;
        this.data = ByteBuffer.allocate(Float.SIZE).putFloat(data).array();
    }

    public PrimitivePacket(int id, String callbackName, int fromId, int toId, double data, String tag) {
        dataType = DOUBLE;
        this.tag = tag;
        this.id = id;
        this.callbackId = callbackName;
        this.fromId = fromId;
        this.toId = toId;
        this.data = ByteBuffer.allocate(Double.SIZE).putDouble(data).array();
    }
    
    public PrimitivePacket(int id, String callbackName, int fromId, int toId, List data, String tag) {
        dataType = VECTOR;
        this.tag = tag;
        this.id = id;
        this.callbackId = callbackName;
        this.fromId = fromId;
        this.toId = toId;
        this.data = ByteBuffer.allocate(Integer.SIZE).putInt(data.size()).array();
    }

    public PrimitivePacket(int id, String callbackName, int fromId, int toId, TreeMap data, String tag) {
        dataType = MAP;
        this.tag = tag;
        this.id = id;
        this.callbackId = callbackName;
        this.fromId = fromId;
        this.toId = toId;
        this.data = ByteBuffer.allocate(Integer.SIZE).putInt(data.size()).array();
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = super.hashCode();
        result = prime * result + dataType;
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (!super.equals(obj))
            return false;
        if (getClass() != obj.getClass())
            return false;
        PrimitivePacket other = (PrimitivePacket) obj;
        return getId() == other.getId();
    }

    public int getDataType() {
        return dataType;
    }

    public void setDataType(int dataType) {
        this.dataType = dataType;
    }

    public String toString() {
        String packetDetail = "primitive id=" + id + " from=" + fromId + 
                " to=" + toId + " tag=" + tag + " datatype=" + dataType + " data=";
        switch (dataType) {
        case STRING:
            packetDetail += new String(data);
            break;
        default:
            ByteBuffer buff = ByteBuffer.wrap(data);
            switch (dataType) {
                case BYTE:
                    packetDetail += buff.get();
                    break;
                case BOOLEAN:
                    packetDetail += buff.get() == 0 ? false : true;
                    break;
                case INTEGER:
                case VECTOR:
                case MAP:
                    packetDetail += buff.getInt();
                    break;
                case LONG:
                    packetDetail += buff.getLong();
                    break;
                case FLOAT:
                    packetDetail += buff.getFloat();
                    break;
                case DOUBLE:
                    packetDetail += buff.getDouble();
                    break;
            }
        }
        packetDetail += " obsolete=" + obsolete + " obsoleteBy=" + obsoleteBy;
        return packetDetail;
    }
     
}
