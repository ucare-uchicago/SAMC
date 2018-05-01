package org.apache.jute;

import java.io.Serializable;

public aspect RecordAspect {
    
    declare parents: Record extends Serializable;

}
