/**
Copyright (c) 2007-2013 Alysson Bessani, Eduardo Alchieri, Paulo Sousa, and the authors indicated in the @author tags

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/
package bftsmart.tom.leaderchange;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import bftsmart.communication.ServerCommunicationSystem;
import bftsmart.demo.counter.ClusterInfo;
import bftsmart.reconfiguration.ServerViewController;
import bftsmart.reconfiguration.util.HostsConfig;
import bftsmart.statemanagement.SMMessage;
import bftsmart.statemanagement.standard.StandardSMMessage;
import bftsmart.tom.core.TOMLayer;
import bftsmart.tom.core.messages.TOMMessage;
import bftsmart.tom.util.TOMUtil;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This thread serves as a manager for all timers of pending requests.
 *
 */
public class RequestsTimer {
    
    private Logger logger = LoggerFactory.getLogger(this.getClass());

    private Timer timer = new Timer("request timer");
    private RequestTimerTask rtTask = null;
//    private RemoteViewChangeTimerTask rvcTask = null;

    private TOMLayer tomLayer; // TOM layer
    private long timeout;
    private long shortTimeout;
    private TreeSet<TOMMessage> watched = new TreeSet<TOMMessage>();

    private ReentrantReadWriteLock rwLock = new ReentrantReadWriteLock();
    
    private boolean enabled = true;
    
    private ServerCommunicationSystem communication; // Communication system between replicas
    private ServerViewController controller; // Reconfiguration manager
    
    private HashMap <Integer, Timer> stopTimers = new HashMap<>();
    
    //private Storage st1 = new Storage(100000);
    //private Storage st2 = new Storage(10000);




    public RequestsTimer(TOMLayer tomLayer, ServerCommunicationSystem communication, ServerViewController controller, int rvc_time) {
        this.tomLayer = tomLayer;

        this.communication = communication;
        this.controller = controller;

        this.timeout = rvc_time;
        this.shortTimeout = -1;
    }





    /**
     * Creates a new instance of RequestsTimer
     * @param tomLayer TOM layer
     */
    public RequestsTimer(TOMLayer tomLayer, ServerCommunicationSystem communication, ServerViewController controller) {
        this.tomLayer = tomLayer;
        
        this.communication = communication;
        this.controller = controller;
        
        this.timeout = 80000;//this.controller.getStaticConf().getRequestTimeout();
        this.shortTimeout = -1;
    }

    public void setShortTimeout(long shortTimeout) {
        this.shortTimeout = shortTimeout;
    }
    
    public void startTimer() {

        if (rtTask == null) {
            logger.info("NO ARGS, shortTimeout, timeout are {}, {}", shortTimeout, timeout);
//            shortTimeout = 1;
            long t = (shortTimeout > -1 ? shortTimeout : timeout);
            //shortTimeout = -1;
            rtTask = new RequestTimerTask();

            if (timer==null)
            {
                timer = new Timer("new timer due to timer being null");
            }

            try
            {
                if (controller.getCurrentViewN() > 1)
                {
                    logger.info("scheduling timer task");
                    timer.schedule(rtTask, t);
                }
            }
            catch (IllegalStateException e)
            {
                logger.info("IllegalStateException caught: {}", e.getMessage());
            }

        }
    }


    public void startTimer(TOMMessage req) {

        if (rtTask == null) {
            logger.info("Args, shortTimeout, timeout are {}, {}", shortTimeout, timeout);
//            shortTimeout = 1;
            long t = (shortTimeout > -1 ? shortTimeout : timeout);
            //shortTimeout = -1;
            rtTask = new RequestTimerTask(req);

            if (controller.getCurrentViewN() > 1)
            {
                logger.info("scheduling timer task");
                timer.schedule(rtTask, t);
            }
        }
    }
    
    public void stopTimer() {

        logger.debug("Stopping timer");
        if (rtTask != null) {

            logger.debug("rtTask not null");

            rtTask.cancel();
            rtTask = null;

//            rvcTask.cancel();
//            rvcTask = null;

        }
    }
    
    public void Enabled(boolean phase) {
        
        enabled = phase;
    }
    
    public boolean isEnabled() {
    	return enabled;
    }
    
    /**
     * Creates a timer for the given request
     * @param request Request to which the timer is being createf for
     */
    public void watch(TOMMessage request) {


        //long startInstant = System.nanoTime();
        rwLock.writeLock().lock();
        watched.add(request);


        if (watched.size() >= 1 && enabled)
        {
            startTimer(request);
            logger.debug("starting timer for request: {}, with reqid: {}, opid:{}, sequence: {}, " +
                            "session:{}, replyserver:{}, req_view_id:{}",
                    request, request.getId(), request.getOperationId(), request.getSequence(),
                    request.getSession(), request.getReplyServer(), request.getViewID());
        }
        rwLock.writeLock().unlock();
    }

    /**
     * Cancels a timer for a given request
     * @param request Request whose timer is to be canceled
     */
    public void unwatch(TOMMessage request) {
        logger.debug("unwatching request: {}, with watched.size() :{} ", request, watched.size());
        rwLock.writeLock().lock();
        if (watched.remove(request) && watched.isEmpty())
        {
            logger.debug("going to stop timer with watched.isempty: {}", watched.isEmpty());
            stopTimer();
        }
        rwLock.writeLock().unlock();
    }

    /**
     * Cancels all timers for all messages
     */
    public void clearAll() {
        TOMMessage[] requests = new TOMMessage[watched.size()];
        rwLock.writeLock().lock();
        
        watched.toArray(requests);

        for (TOMMessage request : requests) {
            if (request != null && watched.remove(request) && watched.isEmpty() && rtTask != null) {
                rtTask.cancel();
                rtTask = null;
            }
        }
        rwLock.writeLock().unlock();
    }
    
    public void run_lc_protocol(int lc_cid) {
        logger.info("\n\n\n RUNNING LC PROTOCOL\n\n\n" + System.currentTimeMillis());
        
        long t = (shortTimeout > -1 ? shortTimeout : timeout);
        
//        System.out.println("(RequestTimerTask.run) I SOULD NEVER RUN WHEN THERE IS NO TIMEOUT");

        LinkedList<TOMMessage> pendingRequests = new LinkedList<>();

        try {
        
            rwLock.readLock().lock();
        
            for (Iterator<TOMMessage> i = watched.iterator(); i.hasNext();) {
                TOMMessage request = i.next();
                if ((System.currentTimeMillis() - request.receptionTimestamp ) > t) {
                    pendingRequests.add(request);
                }
            }
            
        } finally {
            
            rwLock.readLock().unlock();
        }


        
        if (!pendingRequests.isEmpty()) {
            
            logger.info("The following requests timed out: " + pendingRequests);
            
            for (ListIterator<TOMMessage> li = pendingRequests.listIterator(); li.hasNext(); ) {
                TOMMessage request = li.next();
                if (!request.timeout) {
                    
                    logger.info("Forwarding requests {} to leader {},  with request.getId(): {}, request.getOperationId(): {}"
                            , request, tomLayer.execManager.getCurrentLeader(),
                            request.getId(), request.getOperationId());


                    request.signed = request.serializedMessageSignature != null;

//                    if (request.getOperationId())
                    {
                        tomLayer.forwardRequestToLeader(request);
                    }

                    request.timeout = true;
                    li.remove();
                }
            }

            if (!pendingRequests.isEmpty()) {
                logger.info("Attempting to start leader change for requests {}", pendingRequests);
                //Logger.debug = true;
                //tomLayer.requestTimeout(pendingRequests);
                //if (reconfManager.getStaticConf().getProcessId() == 4) Logger.debug = true;
                tomLayer.getSynchronizer().triggerTimeout(pendingRequests);
            }
            else {
                logger.info("triggerTimeout for pendingRequests {}, " +
                        "with tomLayer.getDeliveryThread().getLastCID() = {}, lc_cid: {}",
                        pendingRequests, tomLayer.getDeliveryThread().getLastCID(), lc_cid);

                tomLayer.getSynchronizer().triggerTimeout(pendingRequests);

//                tomLayer.clientsManager.getCIDForRequest();

                SMMessage smsg = new StandardSMMessage(controller.getStaticConf().getProcessId(),
                        lc_cid, TOMUtil.REMOTE_NODE_READY, 0, null, null, -1, -1);



                ClusterInfo cinfo = new ClusterInfo();
                HashMap<Integer, HostsConfig.Config> hostmap = cinfo.getAllConnectionsMap();
                int clusterid = hostmap.get(this.tomLayer.getDeliveryThread().getNodeId()).ClusterNumber;


                List<Integer> tgtList = new ArrayList<Integer>();

                //						for (int i=0; i < this.cinfo.totalCount; i++)
                for (int i : hostmap.keySet()) {
                    if (cinfo.getAllConnectionsMap().get(i).ClusterNumber != clusterid) {
                        tgtList.add(i);
                    }

                }
                //							logger.info("tgtList is {}", tgtList);
                int[] tgtArray = tgtList.stream().filter(Objects::nonNull).mapToInt(Integer::intValue).toArray();



                tomLayer.getDeliveryThread().sendLastOcmd();

                logger.info("\n----1, SENDING REMOTE NOTE READY MSG to : {}", tgtArray);
                tomLayer.getCommunication().send(tgtArray, smsg);


                logger.info("\n----1, SENT REMOTE NOTE READY MSG to : {}", tgtArray);





            }
        } else {
            
            logger.info("Timeout triggered with no expired requests with t: {}", t);
            tomLayer.getSynchronizer().triggerTimeout(pendingRequests);
            logger.info("Timeout triggered DONE, sending REMOTE_NODE_READY msg with cid: {} and lastcid: {}, lc_cid: {}",
                    tomLayer.getLastExec(), tomLayer.getDeliveryThread().getLastCID(), lc_cid);

            SMMessage smsg = new StandardSMMessage(controller.getStaticConf().getProcessId(),
                    lc_cid, TOMUtil.REMOTE_NODE_READY, 0, null, null, -1, -1);



            ClusterInfo cinfo = new ClusterInfo();
            HashMap<Integer, HostsConfig.Config> hostmap = cinfo.getAllConnectionsMap();
            int clusterid = hostmap.get(this.tomLayer.getDeliveryThread().getNodeId()).ClusterNumber;


            List<Integer> tgtList = new ArrayList<Integer>();

            //						for (int i=0; i < this.cinfo.totalCount; i++)
            for (int i : hostmap.keySet()) {
                if (cinfo.getAllConnectionsMap().get(i).ClusterNumber != clusterid) {
                    tgtList.add(i);
                }

            }
            //							logger.info("tgtList is {}", tgtList);
            int[] tgtArray = tgtList.stream().filter(Objects::nonNull).mapToInt(Integer::intValue).toArray();




            logger.info("\n----2, SENDNING REMOTE NOTE READY MSG to : {}", tgtArray);
            tomLayer.getCommunication().send(tgtArray, smsg);




            tomLayer.getDeliveryThread().sendLastOcmd();



//
        }
        
    }
    
    public void setSTOP(int regency, LCMessage stop) {
        
        stopSTOP(regency);
        
        SendStopTask stopTask = new SendStopTask(stop);
        Timer stopTimer = new Timer("Stop message");
        
        stopTimer.schedule(stopTask, timeout);
        
       stopTimers.put(regency, stopTimer);

    }   
    
    public void stopAllSTOPs() {
        Iterator stops = getTimers().iterator();
        while (stops.hasNext()) {
            stopSTOP((Integer) stops.next());
        }
    }
    
    public void stopSTOP(int regency){
        
        Timer stopTimer = stopTimers.remove(regency);
        if (stopTimer != null) stopTimer.cancel();

    }
    
    public Set<Integer> getTimers() {
        
        return ((HashMap <Integer,Timer>) stopTimers.clone()).keySet();
        
    }
    
    public void shutdown() {
        timer.cancel();
        stopAllSTOPs();
        LoggerFactory.getLogger(this.getClass()).info("RequestsTimer stopped.");

    }
    
    class RequestTimerTask extends TimerTask {

        TOMMessage Myreq = null;

        public RequestTimerTask(TOMMessage req) {
            Myreq = req;
        }

        public RequestTimerTask() {
        }

        @Override
        /**
         * This is the code for the TimerTask. It executes the timeout for the first
         * message on the watched list.
         */
        public void run() {

            logger.info("1, timeout, trigger, Remote View Notify");


            int[] myself = new int[1];
            myself[0] = controller.getStaticConf().getProcessId();

            if (tomLayer.getDeliveryThread().invalid_rvc())
            {
                logger.info("Invalid Remote View Notify");

                return;
            }

            try
            {
                ClusterInfo cinfo = new ClusterInfo();
                HashMap<Integer, HostsConfig.Config> hostmap = cinfo.getAllConnectionsMap();
//                logger.info("inside requesttimertask, clusterid is {}, " +
//                        "Myreq: {}, storedCIDforMyreq: {}, opid: {}, seq: {}, getID: {},  session:{}",
//                        tomLayer.getDeliveryThread().getNodeId(),
//                        Myreq, tomLayer.clientsManager.getCIDForRequest(Myreq)
//                , Myreq.getOperationId(), Myreq.getSequence(), Myreq.getId(), Myreq.getSession());



                int cidForRVC = 8000;

                if (this.Myreq!=null)
                {
                    cidForRVC = tomLayer.clientsManager.getCIDForRequest(Myreq);
                }

                if (cidForRVC == -1)
                {
                    logger.info("Invalid Remote View Notify");

                    return;
                }

                if ((tomLayer.getDeliveryThread().getNodeId() > 3) && (cidForRVC > 0))
                {
                    tomLayer.getDeliveryThread().remote_view_notify(cidForRVC);
                    logger.info("sent LCMessage for cid: {}", cidForRVC);
                }


            }
            catch (IOException | ClassNotFoundException | InterruptedException e) {

                throw new RuntimeException(e);
            }

            logger.info("Completed Remote View Notify");


//            communication.send(myself, new LCMessage(-1, TOMUtil.TRIGGER_LC_LOCALLY, -1, null));


        }
    }

//
//    class RemoteViewChangeTimerTask extends TimerTask {
//
//        @Override
//        /**
//         * This is the code for the TimerTask. It executes the timeout for the first
//         * message on the watched list.
//         */
//        public void run() {
//
//            logger.info("2, SENDING LCMessage locally");
//
//
//            int[] myself = new int[1];
//            myself[0] = controller.getStaticConf().getProcessId();
//
//            communication.send(myself, new LCMessage(-1, TOMUtil.TRIGGER_LC_LOCALLY, -1, null));
//
//        }
//    }




    
    class SendStopTask extends TimerTask {
        
        private LCMessage stop;
        
        public SendStopTask(LCMessage stop) {
            this.stop = stop;
        }

        @Override
        /**
         * This is the code for the TimerTask. It sends a STOP
         * message to the other replicas
         */
        public void run() {

                logger.info("Re-transmitting STOP message to install regency " + stop.getReg());
                communication.send(controller.getCurrentViewOtherAcceptors(),this.stop);

                setSTOP(stop.getReg(), stop); //repeat
        }
        
    }
}
