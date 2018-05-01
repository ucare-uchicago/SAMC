package mc.zookeeper.quorum;

import mc.InterceptPacket;

public abstract class InterceptQuorumPacket extends InterceptPacket {
    
    protected String tag;

    public InterceptQuorumPacket() {
        super();
    }

    public InterceptQuorumPacket(int id, String callbackName, int fromId, int toId, byte[] data, String tag) {
        super(id, callbackName, fromId, toId, data);
        this.tag = tag;
    }

    public String getTag() {
        return tag;
    }

    public void setTag(String tag) {
        this.tag = tag;
    }

}
