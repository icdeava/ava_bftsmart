package bftsmart.consensus.messages;

import bftsmart.communication.SystemMessage;
import bftsmart.tom.core.messages.TOMMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.List;

public class ReconfigMessage extends SystemMessage {

    private static final long serialVersionUID = 7806820727429192926L; // Ensure serialization compatibility

    private transient Logger logger = LoggerFactory.getLogger(this.getClass());

    private ReconfigMsgData reconfigData; // Removed transient to allow serialization
    private byte[] payload;

    /**
     * Empty constructor
     */
    public ReconfigMessage() {
    }

    /**
     * Constructor
     *
     * @param seq    seq
     * @param from        Sender ID
     * @param fromConfig  Configuration source
     * @param type        Message type
     */
    public ReconfigMessage(int seq, int from, String fromConfig, int type) throws IOException {
        super(from);
        this.reconfigData = new ReconfigMsgData(seq, from, fromConfig, type);
        this.payload = convertToBytes(this.reconfigData);
    }



    /**
     * Get sender ID
     */
    public int getSender() {
        return this.reconfigData != null ? this.reconfigData.getFrom() : -1; // Return -1 if data is missing
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);
        out.writeObject(this.payload);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        super.readExternal(in);
        this.payload = (byte[]) in.readObject();
        this.reconfigData = convertFromBytes(this.payload); // Ensure reconfigData is restored
    }

    /**
     * Convert ReconfigMsgData to byte array (deep copy)
     */
    private byte[] convertToBytes(ReconfigMsgData data) throws IOException {
        if (data == null) return null;

        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        ObjectOutputStream out = new ObjectOutputStream(bos);
        out.writeObject(data);
        out.close();

        return bos.toByteArray();
    }

    /**
     * Convert byte array back to ReconfigMsgData
     */
    private ReconfigMsgData convertFromBytes(byte[] data) throws IOException, ClassNotFoundException {
        if (data == null) return null;

        ByteArrayInputStream bis = new ByteArrayInputStream(data);
        ObjectInputStream in = new ObjectInputStream(bis);
        return (ReconfigMsgData) in.readObject();
    }

    /**
     * Returns the deserialized ReconfigMsgData object
     */
    public ReconfigMsgData getReconfigData() throws IOException, ClassNotFoundException {
        if (this.reconfigData == null && this.payload != null) {
            this.reconfigData = convertFromBytes(this.payload); // Ensure lazy loading
        }
        return this.reconfigData;
    }
}
