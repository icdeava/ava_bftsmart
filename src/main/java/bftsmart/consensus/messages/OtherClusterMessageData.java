package bftsmart.consensus.messages;

import bftsmart.tom.core.messages.TOMMessage;
import bftsmart.tom.leaderchange.CertifiedDecision;

import java.io.*;

public class OtherClusterMessageData implements Serializable {
    private static final long serialVersionUID = 3108950874674512359L;
    public int[] consId;
    public int[] regencies;
    public int[] leaders;
    public CertifiedDecision[] cDecs;

    public TOMMessage[][] requests;
    public String fromConfig;
    public int from;

    public int from_cid_start;
    public int from_cid_end;

    public boolean skip_iter = false;

    public int type = 1;

    public OtherClusterMessageData(int[] consId, int[] regencies, int[] leaders, CertifiedDecision[] cDecs,
                                   TOMMessage[][] requests, int from, String fromConfig, int from_cid_start, int from_cid_end, int type)
    {
        this.consId = consId;
        this.regencies = regencies;
        this.leaders = leaders;
        this.cDecs = cDecs;
        this.requests = requests;
        this.fromConfig = fromConfig;
        this.from = from;
        this.from_cid_start = from_cid_start;
        this.from_cid_end = from_cid_end;
        this.type = type;
    }

    public OtherClusterMessageData(
                                   TOMMessage[][] requests, int from, String fromConfig, int type)
    {

        this.requests = requests;
        this.fromConfig = fromConfig;
        this.from = from;
        this.type = 2;

    }


    public void setSkipIter(boolean x)
    {
        this.skip_iter = x;
    }


    public void setType(int type) {
        this.type = type;
    }
}
