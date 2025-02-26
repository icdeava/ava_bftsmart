/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package bftsmart.tom.core;

import bftsmart.communication.ServerCommunicationSystem;
import bftsmart.tom.core.messages.TOMMessage;
import bftsmart.tom.core.messages.TOMMessageType;
import io.netty.channel.Channel;

import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.slf4j.LoggerFactory;


/**
 *
 * @author joao
 */
public class ReplyManager {
        
    private LinkedList<ReplyThread> threads;
    private int iteration;
    private ReconfigReplyThread reconfigThread;
//    private thread
    
    public ReplyManager(int numThreads, ServerCommunicationSystem cs) {
        
        this.threads = new LinkedList();
        this.iteration = 0;
        
        for (int i = 0; i < numThreads; i++) {
            this.threads.add(new ReplyThread(cs));
        }

        this.reconfigThread = new ReconfigReplyThread(cs);
        
        for (ReplyThread t : threads)
            t.start();

        reconfigThread.start();
    }
    
    public void send (TOMMessage msg) {
        
        iteration++;
        threads.get((iteration % threads.size())).send(msg);

    }

    public void sendReconfig(TOMMessage tm) {
        reconfigThread.sendReconfig(tm);
    }
}
class ReplyThread extends Thread {
        
    private LinkedBlockingQueue<TOMMessage> replies;

    private ServerCommunicationSystem cs = null;
    
    private final Lock queueLock = new ReentrantLock();
    private final Condition notEmptyQueue = queueLock.newCondition();
    
    private Map<Integer, Channel> channels;
    
    ReplyThread(ServerCommunicationSystem cs) {
        this.cs = cs;
        this.replies = new LinkedBlockingQueue<TOMMessage>();

        this.channels = new HashMap<>();
    }
    
    void send(TOMMessage msg) {
        
        try {
            queueLock.lock();
            replies.put(msg);
            notEmptyQueue.signalAll();
            queueLock.unlock();
        } catch (InterruptedException ex) {
            Logger.getLogger(ReplyThread.class.getName()).log(Level.SEVERE, null, ex);
        }
    }


    
    public void run() {


        while (true) {

            try {
                
                LinkedList<TOMMessage> list = new LinkedList<>();

                queueLock.lock();
                while (replies.isEmpty())
                {
                    notEmptyQueue.await(10, TimeUnit.MILLISECONDS);
                }
                replies.drainTo(list);
                queueLock.unlock();
                
                for (TOMMessage msg : list) {

                        cs.getClientsConn().send(new int[] {msg.getSender()}, msg.reply, false);
                }




            } catch (InterruptedException ex) {
                LoggerFactory.getLogger(this.getClass()).error("Could not retrieve reply from queue",ex);
            }

        }

    }


}



class ReconfigReplyThread extends Thread {

    private LinkedBlockingQueue<TOMMessage> reconfigreplies;

    private ServerCommunicationSystem cs = null;

    private final Lock queueLock = new ReentrantLock();
    private final Condition notEmptyQueue = queueLock.newCondition();

    private Map<Integer, Channel> channels;

    ReconfigReplyThread(ServerCommunicationSystem cs) {
        this.cs = cs;
        this.reconfigreplies = new LinkedBlockingQueue<TOMMessage>();
    }

    public void run() {

        while (true) {

            try {

                LinkedList<TOMMessage> list = new LinkedList<>();
                LinkedList<TOMMessage> reconfiglist = new LinkedList<>();

                queueLock.lock();
                while (reconfigreplies.isEmpty())
                {
                    notEmptyQueue.await(10, TimeUnit.MILLISECONDS);
                }
                reconfigreplies.drainTo(reconfiglist);
                queueLock.unlock();

                for (TOMMessage msg: reconfiglist)
                {
                    if (msg.getReqType()== TOMMessageType.RECONFIG)
                    {
                        int [] tosendTo = new int[] {msg.getreconfignodeid()};
                        LoggerFactory.getLogger(this.getClass()).debug("sending reconfig reply with replymanager for seq: " + msg.getSequence()
                                + " to destination: "+ Arrays.toString(tosendTo));
                        cs.getClientsConn().send(tosendTo, msg, true);
                    }
                }

            } catch (InterruptedException ex) {
                LoggerFactory.getLogger(this.getClass()).error("Could not retrieve reply from queue",ex);
            }

        }

    }

    public void sendReconfig(TOMMessage tm) {

        try {
            queueLock.lock();
            reconfigreplies.put(tm);
            queueLock.unlock();
        } catch (InterruptedException ex) {
            Logger.getLogger(ReplyThread.class.getName()).log(Level.SEVERE, null, ex);
        }

    }
}