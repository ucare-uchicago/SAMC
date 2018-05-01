package mc;

import java.util.Collections;
import java.util.LinkedList;

public class AbstractGlobalState {
    
    public LinkedList<Integer> localState;
    
    public AbstractGlobalState() {
        localState = new LinkedList<Integer>();
    }
    
    public void addAbstractLocalState(AbstractLocalState ls) {
        localState.add(ls.getStateValue());
    }
    
    public int getStateValue() {
        Collections.sort(localState);
        return hashCode();
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result
                + ((localState == null) ? 0 : localState.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        AbstractGlobalState other = (AbstractGlobalState) obj;
        if (localState == null) {
            if (other.localState != null)
                return false;
        } else if (!localState.equals(other.localState))
            return false;
        return true;
    }
    
}
