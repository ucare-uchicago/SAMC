package mc.zookeeper.quorum;

import org.apache.jute.Record;
import org.apache.zookeeper.server.quorum.QuorumPacket;

public class RecordPacket extends InterceptQuorumPacket {
    
    Record record;

    public RecordPacket() {
    }

    public RecordPacket(int id, String callbackName, int fromId, int toId, Record record, String tag) {
        super(id, callbackName, fromId, toId, null, tag);
        this.record = record;
    }

    @Override
    public String toString() {
        String packetDetail = "record " + "id=" + id + " from=" + fromId + 
                " to=" + toId + " tag=" + tag + " class=" + 
                record.getClass().getSimpleName();
        if (record instanceof QuorumPacket) {
            QuorumPacket packet = (QuorumPacket) record;
            packetDetail += " type=" + packet.getType() + " zxid=" + packet.getZxid();
        }
        packetDetail += " obsolete=" + obsolete + " obsoleteBy=" + obsoleteBy;
        return packetDetail;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = super.hashCode();
        result = prime * result + ((record == null) ? 0 : record.hashCode());
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
        RecordPacket other = (RecordPacket) obj;
        return this.getId() == other.getId();
    }

    public Record getRecord() {
        return record;
    }

    public void setRecord(Record record) {
        this.record = record;
    }
    
}
