package bftsmart.consensus.messages;

import bftsmart.communication.SystemMessage;
import bftsmart.tom.core.messages.TOMMessage;
import bftsmart.tom.leaderchange.CertifiedDecision;
import bftsmart.tom.util.TOMUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.ConcurrentModificationException;

public class OtherClusterMessage extends SystemMessage {

    private transient Logger logger = LoggerFactory.getLogger(this.getClass());


    private transient OtherClusterMessageData ocmd;
    private byte[] payload;


    /**
     * Empty constructor
     */
    public OtherClusterMessage(){

    }


    /**
     * Constructor
     * @param from replica that creates this message
     * @param type type of the message (STOP, SYNC, CATCH-UP)
     * @param ts timestamp of leader change and synchronization
     * @param payload dada that comes with the message
     */
    public OtherClusterMessage(int[] consId, int[] regencies, int[] leaders, CertifiedDecision[] cDecs,
                     TOMMessage[][] requests, int from, String fromConfig, int from_cid_start, int from_cid_end, int type) throws IOException {
        super(from);

        this.ocmd = new OtherClusterMessageData(consId, regencies, leaders, cDecs,
                requests, from, fromConfig, from_cid_start, from_cid_end, type);
        this.payload = convertToBytes(this.ocmd);

    }

    public void setOcmdType(int type) throws IOException {
        this.ocmd.setType(type);
        this.payload = convertToBytes(this.ocmd);
    }


//    public OtherClusterMessageData getOcmd() {
//        return this.ocmd;
//    }

    public int getSender()
    {
        return this.ocmd.from;
    }


    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);

        out.writeObject(this.payload);
//        logger.info("WRITING BYTES HERE");
//        logger.info("While Writing, this.ocmd.consId is {}",this.ocmd.consId);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException{
        super.readExternal(in);
//        logger.info("READING BYTES HERE");
        this.payload = (byte[]) in.readObject();
        this.ocmd = convertFromBytes(this.payload);
//        logger.info("After Reading, this.ocmd.consId, this.ocmd.leaders is {}, {}",this.ocmd.consId, this.ocmd.leaders);
    }


    public byte[] convertToBytes(OtherClusterMessageData OCmsg) throws IOException {
        
        boolean success = false;
        
        byte b[] = null;
        
        while(success!=true)
        {
            try{
            
            
            ByteArrayOutputStream bosCTB = new ByteArrayOutputStream();
            ObjectOutput outCTB = new ObjectOutputStream(bosCTB);

            outCTB.writeObject(OCmsg);


            b = bosCTB.toByteArray();
            outCTB.close();
            bosCTB.close();
            
            success = true;
            
            }
        catch (ConcurrentModificationException e) {
                    logger.info("ConcurrentModificationException caught during serialization: " + e.getMessage());
                }
            
            
        }
            
        return b;

    }

    public OtherClusterMessageData convertFromBytes(byte[] data) throws IOException, ClassNotFoundException {

        ByteArrayInputStream bis = new ByteArrayInputStream(data);
        ObjectInput in = new ObjectInputStream(bis);
        return (OtherClusterMessageData) in.readObject();
    }

    public OtherClusterMessageData getOcmd() throws IOException, ClassNotFoundException {
        return convertFromBytes(this.payload);
    }


}
