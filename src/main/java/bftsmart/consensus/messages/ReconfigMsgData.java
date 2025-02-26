package bftsmart.consensus.messages;

import bftsmart.tom.core.messages.TOMMessage;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class ReconfigMsgData implements Serializable {
    private static final long serialVersionUID = 202502270001L;

    private int seq;
    private int from;
    private String fromConfig;
    private int type;

    /**
     * Constructor
     */
    public ReconfigMsgData(int seq, int from, String fromConfig, int type) {
        this.from = from;
        this.fromConfig = fromConfig;
        this.type = type;
        this.seq = seq;
    }

    /**
     * Copy Constructor (Deep Copy)
     */
    public ReconfigMsgData(ReconfigMsgData other) {
        this.from = other.from;
        this.fromConfig = other.fromConfig;
        this.type = other.type;
        this.seq = other.seq;


    }

    public void setType(int type) {
        this.type = type;
    }

    public int getType() {
        return this.type;
    }

    public int getFrom() {
        return this.from;
    }

    public int getSeq() {
        return this.seq; // Return a copy to prevent modification
    }


    public String getFromConfig() {
        return this.fromConfig;
    }
}
