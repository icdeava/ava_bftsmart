/*
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
package bftsmart.tom.core;

import bftsmart.communication.SystemMessage;
import bftsmart.consensus.Decision;
import bftsmart.consensus.messages.OtherClusterMessage;
import bftsmart.consensus.messages.OtherClusterMessageData;
import bftsmart.consensus.messages.ReconfigMessage;
import bftsmart.consensus.messages.ReconfigMsgData;
import bftsmart.demo.counter.ClusterInfo;
import bftsmart.reconfiguration.ReconfigureReply;
import bftsmart.reconfiguration.ServerViewController;
import bftsmart.reconfiguration.views.View;
import bftsmart.reconfiguration.util.HostsConfig;
import bftsmart.statemanagement.ApplicationState;
import bftsmart.statemanagement.SMMessage;
import bftsmart.statemanagement.standard.StandardSMMessage;
import bftsmart.tom.MessageContext;
import bftsmart.tom.ServiceReplica;
import bftsmart.tom.core.messages.ForwardedMessage;
import bftsmart.tom.core.messages.TOMMessage;
import bftsmart.tom.leaderchange.CertifiedDecision;
import bftsmart.tom.server.Recoverable;
import bftsmart.tom.util.BatchReader;
import bftsmart.tom.util.TOMUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.locks.*;

import static bftsmart.tom.core.messages.TOMMessageType.RECONFIG;

/**
 * This class implements a thread which will deliver totally ordered requests to
 * the application
 */
public final class DeliveryThread extends Thread {
    private final Logger logger = LoggerFactory.getLogger(this.getClass());
    private boolean doWork = true;
    private int lastReconfig = -2;
    private int lastPendingReconfig = -2;

    private int lastcid = -2;

    private int lastLCLockMsg = -2;

    private int last_skip_cid = -2;
    private OtherClusterMessage lastocmd;

    private final LinkedBlockingQueue<Decision> decided;


    private HashMap<Integer, Integer> hmap = new HashMap<Integer, Integer>();

    private HashSet<Integer> reconfigProcessed = new HashSet<Integer>();



    private final LinkedBlockingQueue<OtherClusterMessage> decidedOtherClusters;


    private final TOMLayer tomLayer; // TOM layer
    private final ServiceReplica receiver; // Object that receives requests from clients


    private final Recoverable recoverer; // Object that uses state transfer
    private final ServerViewController controller;
    private final Lock decidedLock = new ReentrantLock();

    private final Condition notEmptyQueue = decidedLock.newCondition();



    private final Lock decidedLockOtherClusters = new ReentrantLock();

    private final Lock ReconfigLockMC = new ReentrantLock();
    private final Lock LcLockMC = new ReentrantLock();

    private final Condition notEmptyQueueOtherClusters = decidedLockOtherClusters.newCondition();

    private final Condition ReconfigLockMCCondition = ReconfigLockMC.newCondition();
    private final Condition LcLockMCCondition = LcLockMC.newCondition();


    private final Lock decidedLockOtherClustersReconfig = new ReentrantLock();
    private final Condition notEmptyQueueOtherClustersReconfig = decidedLockOtherClustersReconfig.newCondition();



    //Variables used to pause/resume decisions delivery
    private final Lock pausingDeliveryLock = new ReentrantLock();
    private final Condition deliveryPausedCondition = pausingDeliveryLock.newCondition();
    private int isPauseDelivery;

    private int rvc_timeout = 20;
    private int last_rvc_msg = -1;

    OtherClusterMessage ocmd;

    HashMap<Integer, Decision> LastDecisionSaved = new HashMap<Integer, Decision>();


    ConcurrentHashMap<Integer, HashMap<Integer, OtherClusterMessage>> SavedMultiClusterMessages =
            new ConcurrentHashMap<Integer, HashMap<Integer, OtherClusterMessage>>();


    ConcurrentHashMap<Integer, Integer> pending_recs_tracker =
            new ConcurrentHashMap<Integer, Integer>();

    ConcurrentHashMap<Integer, Integer> reconfig_echo_tracker =
            new ConcurrentHashMap<Integer, Integer>();



    ConcurrentHashMap<Integer, Integer> reconfig_ready_counter =
            new ConcurrentHashMap<Integer, Integer>();


    ConcurrentHashMap<Integer, long[]> times_tracker =
            new ConcurrentHashMap<Integer, long[]>();


    public ConcurrentHashMap<Integer, long[]> getTimes_tracker()
    {
        return times_tracker;

    }
    ConcurrentHashMap<Integer, Integer> rvc_tracker =
            new ConcurrentHashMap<Integer, Integer>();

    ConcurrentHashMap<Integer, Integer> Saved_rvc_l =
            new ConcurrentHashMap<Integer, Integer>();


    ConcurrentHashMap<Integer, HashMap<Integer, OtherClusterMessage>> Saved_rvc_mc =
            new ConcurrentHashMap<Integer, HashMap<Integer, OtherClusterMessage>>();


    ConcurrentHashMap<Integer, TOMMessage[][]> SavedMessagesForExec =
            new ConcurrentHashMap<Integer, TOMMessage[][]>();

    Set<Integer> ReceivedOtherClusterMsgs = new HashSet<>();

    ConcurrentHashMap<Integer, OtherClusterMessageData> SavedDecisionsToBeExecuted = new ConcurrentHashMap<Integer, OtherClusterMessageData>();
    ConcurrentHashMap<Integer, Integer> DoneMainLoopExec = new ConcurrentHashMap<Integer, Integer>();


    ReadWriteLock readWriteLock = new ReentrantReadWriteLock();


    ClusterInfo cinfo;


    Runtime runtime;
    long initialMemory;

    long finalMemory;
    long memoryUsed;
    double memoryUsedInMB;


    public HashMap<Integer, Integer> getHmap() {
        return hmap;
    }

    public void setHmap(HashMap<Integer, Integer> hmp) {
        hmap = hmp;
    }


    /**
     * Creates a new instance of DeliveryThread
     *
     * @param tomLayer TOM layer
     * @param receiver Object that receives requests from clients
     */
    public DeliveryThread(TOMLayer tomLayer, ServiceReplica receiver, Recoverable recoverer,
                          ServerViewController controller) {
        super("Delivery Thread");
        this.decided = new LinkedBlockingQueue<>();

        this.decidedOtherClusters = new LinkedBlockingQueue<>();


        this.tomLayer = tomLayer;
        this.receiver = receiver;
        this.recoverer = recoverer;
        // ******* EDUARDO BEGIN **************//
        this.controller = controller;
        // ******* EDUARDO END **************//

        this.cinfo = new ClusterInfo();

        for (int i = 0; i < cinfo.nClusters; i++) hmap.put(i, -1);
    }

    public Recoverable getRecoverer() {
        return recoverer;
    }


    public void receive_lcomplain_send_rcomplain(int cid) throws IOException, ClassNotFoundException, InterruptedException {


        logger.info("receive_lcomplain_send_rcomplain activated");
        readWriteLock.writeLock().lock();
        int clusterid = Integer.parseInt(
                this.ocmd.getOcmd().fromConfig.replaceAll("[^0-9]",
                        ""));


        int[] tgtArray = cinfo.getFPlusOneArray(clusterid).stream().filter(Objects::nonNull).mapToInt(Integer::intValue).toArray();


        SMMessage smsg = new StandardSMMessage(controller.getStaticConf().getProcessId(),
                lastcid, TOMUtil.REMOTE_VIEW_CHANGE, 0, null, null, 0, -1);

        SMMessage smsgAmp = new StandardSMMessage(controller.getStaticConf().getProcessId(),
                lastcid, TOMUtil.REMOTE_VIEW_CHANGE_LCOMPLAIN, 0, null, null, -1, -1);


        int[] tgtArrayAmp = controller.getCurrentViewAcceptors();

        Saved_rvc_l.put(cid,
                Saved_rvc_l.getOrDefault(cid,0)+1);

        if (Saved_rvc_l.get(cid)==controller.getCurrentViewF() + 1)
        {
            tomLayer.getCommunication().send(tgtArrayAmp, smsgAmp);
        }


        if (Saved_rvc_l.get(cid) >= 2 * controller.getCurrentViewF() + 1)
        {

            {
                tomLayer.getCommunication().send(tgtArray, smsg);

                logger.info("receive_lcomplain_send_rcomplain: Waiting After Sending Remote View Change message to Leader sent to" +
                        "{}", tgtArray);

            }

        }


        readWriteLock.writeLock().unlock();

    }

    public boolean invalid_rvc()
    {

        int last_rvc_time = -21;

        for (int key : rvc_tracker.keySet()) {
            if (rvc_tracker.get(key) > last_rvc_time) {
                last_rvc_time = rvc_tracker.get(key);
            }
        }

        return (((int) System.currentTimeMillis() / 1000) - last_rvc_time) < 20;

    }


    public void remote_view_notify(int cid) throws IOException, ClassNotFoundException, InterruptedException {

        logger.info("remote_view_notify initiated");

        if (rvc_tracker.contains(cid)) {
            logger.info("Already initiated");
            return;
        }

        int last_rvc_time = -21;

        for (int key : rvc_tracker.keySet()) {
            if (rvc_tracker.get(key) > last_rvc_time) {
                last_rvc_time = rvc_tracker.get(key);
            }
        }

        logger.info(" time diff is : {}", ((((int) System.currentTimeMillis() / 1000) - last_rvc_time) < 20));


        if ((((int) System.currentTimeMillis() / 1000) - last_rvc_time) < 20) {
            return;
        }


        {

            logger.info("Already initiated");

            rvc_tracker.put(cid, (int) System.currentTimeMillis() / 1000);


            int clusterid = Integer.parseInt(
                    this.ocmd.getOcmd().fromConfig.replaceAll("[^0-9]",
                            ""));


            int[] tgtArray = controller.getCurrentViewAcceptors();


            SMMessage smsg = new StandardSMMessage(controller.getStaticConf().getProcessId(),
                    lastcid, TOMUtil.REMOTE_VIEW_CHANGE_LCOMPLAIN, 0, null, null, -1, -1);




            {
                tomLayer.getCommunication().send(tgtArray, smsg);

                logger.info("remote_view_notify:Waiting After Sending Remote View Change message to Leader sent to" +
                        "{} for cid:{}", tgtArray, cid);


                logger.debug("Waiting After Sending Remote View Change message DONE");


            }

        }


    }

    /**
     * Invoked by the TOM layer, to deliver a decision
     *
     * @param dec Decision established from the consensus
     */
    public void delivery(Decision dec) {


        decidedLock.lock();

        try {
            decided.put(dec);

            // clean the ordered messages from the pending buffer

//            TOMMessage[] requests = extractMessagesFromDecision(dec);
//            tomLayer.clientsManager.requestsOrdered(requests);
            logger.debug("Consensus " + dec.getConsensusId() + " finished. Decided size=" + decided.size());

            if (times_tracker.containsKey(dec.getConsensusId()))
            {
                long[] values = times_tracker.get(dec.getConsensusId());
                values[1] = (System.currentTimeMillis() );
                times_tracker.put(dec.getConsensusId(), values);
            }

        } catch (Exception e) {
            logger.info("Could not insert decision into decided queue and mark requests as delivered", e);
        }

//        if (!containsReconfig(dec)) {
        if (2>1) {

//                logger.info("Decision from consensus " + dec.getConsensusId() + " does not contain reconfiguration");
            // set this decision as the last one from this replica
            tomLayer.setLastExec(dec.getConsensusId());
            // define that end of this execution
            tomLayer.setInExec(-1);
        } // else if (tomLayer.controller.getStaticConf().getProcessId() == 0)
        // System.exit(0);
        else {
            logger.info("Decision from consensus " + dec.getConsensusId() + " has reconfiguration");
            lastReconfig = dec.getConsensusId();
        }

        notEmptyQueue.signalAll();
        decidedLock.unlock();
    }


    public void executeMessages(int tid) {
        logger.info("executeMessages for tid: " + tid);


        long ExecStartTime = System.nanoTime();

        OtherClusterMessageData tempOcmd = null;
        try {
            tempOcmd = SavedMultiClusterMessages.get(tid).get(this.cinfo.getClusterNumber(getNodeId())).getOcmd();
        } catch (IOException e) {
            throw new RuntimeException(e);
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }


        Decision lastDecision = LastDecisionSaved.get(tid);

        lastocmd = SavedMultiClusterMessages.get(tid).get(this.cinfo.getClusterNumber(getNodeId()));


        TOMMessage[] requests = extractMessagesFromDecision(lastDecision);
        tomLayer.clientsManager.requestsOrdered(requests);

        logger.debug("Getting lastdecision for cid: {}", tid);
        
        
        
        long startTime = System.currentTimeMillis();

        
        deliverMessages(tempOcmd.consId, tempOcmd.regencies, tempOcmd.leaders,
                tempOcmd.cDecs, SavedMessagesForExec.get(tid));
    
        
        long elapsedTime = System.currentTimeMillis() - startTime;



        if (tid % 100 == 0) {
            logger.debug("deleting old info from tid: {} to {}", tid - 1100, tid - 100);
            for (int i = Math.max(tid - 100, 0); i < tid; i++) {

                SavedMessagesForExec.remove(i);
                LastDecisionSaved.remove(i);
                SavedMultiClusterMessages.remove(i);
                DoneMainLoopExec.remove(i);
            }

        }


        // define the last stable consensus... the stable consensus can
        // be removed from the leaderManager and the executionManager
        // TODO: Is this part necessary? If it is, can we put it
        // inside setLastExec
        int cid = lastDecision.getConsensusId();
        if (cid > 2) {
            int stableConsensus = cid - 3;

            tomLayer.execManager.removeConsensus(stableConsensus);
        }


        long ExecEndTime = System.nanoTime();

        logger.debug("Ending Exec for cId:{},  " +
                        "ExecLatency: {}, decided.size(): {}",
                lastcid, ExecEndTime - ExecStartTime, decided.size());



    }

    public void deliveryOtherCluster(OtherClusterMessage msg) throws IOException, ClassNotFoundException {


        readWriteLock.writeLock().lock();

        if (msg.getOcmd().type == 1) {
//
            logger.debug("reached inside deliveryOtherCluster type 1, from_cid_start: {} "
                    + "from: {}, fromConfig: {}, msg.getSender(): {}," +
                            " msg instanceof Forwarded: {}, getCurrentViewN: {}, last exec: {}",
                    msg.getOcmd().from_cid_start,
                    msg.getOcmd().from
                    , msg.getOcmd().fromConfig, msg.getSender(), ((SystemMessage) msg instanceof ForwardedMessage),
                    controller.getCurrentViewN(), this.tomLayer.getLastExec());


//			int clusterid = cinfo.getAllConnectionsMap().get(this.receiver.getId()).ClusterNumber;


            if (!othermsgs_received_mc(msg.getOcmd().from_cid_start)) {

                int[] tgtArray = controller.getCurrentViewOtherAcceptors();

//                msg.setOcmdType(2);



                boolean containsReconfig = false;
                
                if (msg.getOcmd().requests!=null)
                {
                    for (TOMMessage[] req: msg.getOcmd().requests)
                    {
                        for (TOMMessage d: req)

                        if (d.getReqType()==RECONFIG)
                        {
                            containsReconfig = true;

                            logger.info("reached inside deliveryOtherCluster Contains Reconfig");
                        }
                    }
                }
        


//consensusIds, regenciesIds, leadersIds, cDecs, requests
        
                OtherClusterMessage newocmd = new OtherClusterMessage(msg.getOcmd().consId,
                        msg.getOcmd().regencies, msg.getOcmd().leaders,
                        msg.getOcmd().cDecs, msg.getOcmd().requests,
                        this.getNodeId(), msg.getOcmd().fromConfig, msg.getOcmd().from_cid_start,
                        msg.getOcmd().from_cid_end, 2);


//                logger.info("Sending type 2 message to {}", tgtArray);

                logger.debug("Sending type 2 message to {}, with msg= {} for cid: {}",
                        tgtArray, newocmd, msg.getOcmd().from_cid_start);

//                this.tomLayer.getCommunication().send(tgtArray, msg);

                this.tomLayer.getCommunication().send(tgtArray, newocmd);
                deliveryOtherCluster(newocmd);
                


            }


        }

        if (msg.getOcmd().type == 2) {

            logger.debug("reached inside deliveryOtherCluster type 2, from_cid_start, from, fromConfig, msg.getSender()," +
                            " are {}, {}, {}, {},  getCurrentViewN: {}", msg.getOcmd().from_cid_start,
                    msg.getOcmd().from
                    , msg.getOcmd().fromConfig, msg.getSender(),
                    controller.getCurrentViewN());

            int clusterId = Integer.parseInt(
                    msg.getOcmd().fromConfig.replaceAll("[^0-9]",
                            ""));


            HashMap<Integer, OtherClusterMessage> tempMap = SavedMultiClusterMessages.get(msg.getOcmd().from_cid_start);


            if (tempMap == null) tempMap = new HashMap<Integer, OtherClusterMessage>();

            tempMap.put(clusterId, msg);


            SavedMultiClusterMessages.put(msg.getOcmd().from_cid_start, tempMap);


            if (othermsgs_received_mc(msg.getOcmd().from_cid_start)) {
                Integer temp_cid = msg.getOcmd().from_cid_start;

                if (times_tracker.containsKey(temp_cid))
                {
                    long[] values = times_tracker.get(temp_cid);
                    values[2] = (System.currentTimeMillis() );
                    times_tracker.put(temp_cid, values);
                }

                logger.debug("executing for tid: {}", msg.getOcmd().from_cid_start);
                executeMessages(msg.getOcmd().from_cid_start);


                if (times_tracker.containsKey(temp_cid))
                {

                    long[] values = times_tracker.get(temp_cid);
                    logger.debug("values size, data: {}, {}",values.length, values);
                    values[3] = (System.currentTimeMillis() );
                    times_tracker.put(temp_cid, values);
                }



            } else logger.debug("Not executing for tid: {}", msg.getOcmd().from_cid_start);


        }


        readWriteLock.writeLock().unlock();


    }


    public boolean othermsgs_received_mc(int tid) {


        HashMap<Integer, OtherClusterMessage> temp = SavedMultiClusterMessages.get(tid);

        if (temp == null) return false;

        return temp.size() == this.cinfo.nClusters;


    }


    /**
     * THIS IS JOAO'S CODE, TO HANDLE STATE TRANSFER
     */
    private final ReentrantLock deliverLock = new ReentrantLock();
    private final Condition canDeliver = deliverLock.newCondition();


    /**
     * @deprecated This method does not always work when the replica was already delivering decisions.
     * This method is replaced by {@link #pauseDecisionDelivery()}.
     * Internally, the current implementation of this method uses {@link #pauseDecisionDelivery()}.
     */
    @Deprecated
    public void deliverLock() {
        pauseDecisionDelivery();
    }

    /**
     * @deprecated Replaced by {@link #resumeDecisionDelivery()} to work in pair with {@link #pauseDecisionDelivery()}.
     * Internally, the current implementation of this method calls {@link #resumeDecisionDelivery()}
     */
    @Deprecated
    public void deliverUnlock() {
        resumeDecisionDelivery();
    }

    /**
     * Pause the decision delivery.
     */
    public void pauseDecisionDelivery() {
        pausingDeliveryLock.lock();
        isPauseDelivery++;
        pausingDeliveryLock.unlock();

        // release the delivery lock to avoid blocking on state transfer
        decidedLock.lock();

        notEmptyQueue.signalAll();
        decidedLock.unlock();

        deliverLock.lock();
    }

    public void resumeDecisionDelivery() {
        pausingDeliveryLock.lock();
        if (isPauseDelivery > 0) {
            isPauseDelivery--;
        }
        if (isPauseDelivery == 0) {
            deliveryPausedCondition.signalAll();
        }
        pausingDeliveryLock.unlock();
        deliverLock.unlock();
    }

    /**
     * This method is used to restart the decision delivery after awaiting a state.
     */
    public void canDeliver() {
        canDeliver.signalAll();
    }

    public void update(ApplicationState state) {

        int lastCID = recoverer.setState(state);

        // set this decision as the last one from this replica

        logger.info("Setting last CID to " + lastCID);

        if (lastCID > 0) {
            tomLayer.setLastExec(lastCID + 1);

        } else {
            tomLayer.setLastExec(lastCID);

        }


        // define the last stable consensus... the stable consensus can
        // be removed from the leaderManager and the executionManager
        if (lastCID > 2) {
            int stableConsensus = lastCID - 3;
            tomLayer.execManager.removeOutOfContexts(stableConsensus);
        }

        // define that end of this execution
        // stateManager.setWaiting(-1);
        tomLayer.setNoExec();

        logger.info("Current decided size: " + decided.size());
        decided.clear();

//		decidedOtherClusters.clear();

        logger.info("All finished up to " + lastCID);

    }


    public void resendocmd() throws IOException, ClassNotFoundException {

        if (2 > 1) {

            HashMap<Integer, HostsConfig.Config> hostmap = cinfo.getAllConnectionsMap();
            int clusterid = hostmap.get(this.receiver.getId()).ClusterNumber;


            List<Integer> tgtList = new ArrayList<Integer>();

            for (int i : hostmap.keySet()) {
                if (cinfo.getAllConnectionsMap().get(i).ClusterNumber != clusterid) {
                    tgtList.add(i);
                }

            }

            int[] tgtArray = tgtList.stream().filter(i -> i != null).mapToInt(Integer::intValue).toArray();


            try {
                logger.info("\n\n\n\n\n SENDING AFTER RECONFIG to {} with ocmd from_cid_start: {}",
                        tgtArray, lastocmd.getOcmd().from_cid_start);
            } catch (IOException | ClassNotFoundException e) {
                throw new RuntimeException(e);
            }

            this.ocmd.getOcmd().setSkipIter(true);
            this.tomLayer.getCommunication().send(tgtArray, this.ocmd);


        }
    }


    public void sending_other_clusters(int[] consensusIds, int[] regenciesIds, int[] leadersIds,
                                       CertifiedDecision[] cDecs, TOMMessage[][] requests,
                                       ArrayList<Decision> decisions, Decision lastDecision) throws InterruptedException, IOException, ClassNotFoundException {


        logger.debug("-XOXOXOXOX---- {}, {}, {}, {}, {}, {}, {} are with currentViewID: {} and time = {}",
                consensusIds, regenciesIds, leadersIds, cDecs, requests,
                this.receiver.getId(), this.receiver.getConfig(), controller.getCurrentViewId(), System.currentTimeMillis());


        OtherClusterMessage completeOcmd = new OtherClusterMessage(consensusIds, 
                regenciesIds, leadersIds, cDecs, requests,
                this.receiver.getId(), this.receiver.getConfig(), decisions.get(0).getConsensusId(),
                lastDecision.getConsensusId(), 1);


        this.ocmd = new OtherClusterMessage(null, null, null, null, null,
                this.receiver.getId(), this.receiver.getConfig(), decisions.get(0).getConsensusId(),
                lastDecision.getConsensusId(), 1);


        int clusterid = Integer.parseInt(
                this.ocmd.getOcmd().fromConfig.replaceAll("[^0-9]",
                        ""));



        int[] tgtArray = cinfo.getFPlusOneArray(clusterid).stream().filter(Objects::nonNull).mapToInt(Integer::intValue).toArray();



        logger.info("\n\n\n\n\n\n\n\n Time, tgtArray, consensusIds, consensusIds[0], lastcid is {},  {}, {}, {}, {}, ocmd = {}",
                System.currentTimeMillis() / 1000, tgtArray,
                consensusIds, consensusIds[0], lastcid, this.ocmd);



//	if (2>1)
        if ((lastcid != -1000) && (this.receiver.getId() == tomLayer.execManager.getCurrentLeader()))
        {
            logger.info("\n\n\n\n\n SENDING OTHER CLUSTERS {} THE DECIDED VALUES", tgtArray);


            this.tomLayer.getCommunication().send(tgtArray, this.ocmd);


        } 
        else 
        {
            if ((this.receiver.getId() == tomLayer.execManager.getCurrentLeader()) && (clusterid != 0)) {
                logger.info("\n\n\n\n\n SENDING OTHER CLUSTERS THE DECIDED VALUES");
                this.tomLayer.getCommunication().send(tgtArray, this.ocmd);
            }


            logger.debug("Not sending multicluster msg, clusterid==1 is  {}", clusterid == 1);
            //                this.tomLayer.getCommunication().send(tgtArray, this.ocmd);
        }
        
        
        
        
        logger.debug("OtherClusterMessage Sent to {}, with type {}",
                tgtArray, this.ocmd.getOcmd().type);


        HashMap<Integer, OtherClusterMessage> tempMap = SavedMultiClusterMessages.get(this.ocmd.getOcmd().from_cid_start);


        if (tempMap == null) tempMap = new HashMap<Integer, OtherClusterMessage>();

        tempMap.put(clusterid, completeOcmd);


        logger.info("saving msg for execution, with tid: {}, requests: {}",
                this.ocmd.getOcmd().from_cid_start, requests);


        SavedMessagesForExec.put(this.ocmd.getOcmd().from_cid_start, requests);

        SavedMultiClusterMessages.put(completeOcmd.getOcmd().from_cid_start, tempMap);

    }


    /**
     * This is the code for the thread. It delivers decisions to the TOM request
     * receiver object (which is the application)
     */
    @Override
    public void run() {
        boolean init = true;
        while (doWork) {
            pausingDeliveryLock.lock();
            while (isPauseDelivery > 0) {
                deliveryPausedCondition.awaitUninterruptibly();
            }
            pausingDeliveryLock.unlock();
            deliverLock.lock();

            /* THIS IS JOAO'S CODE, TO HANDLE STATE TRANSFER */
            //deliverLock();
            while (tomLayer.isRetrievingState()) {
                logger.info("Retrieving State");
                canDeliver.awaitUninterruptibly();

                // if (tomLayer.getLastExec() == -1)
                if (init) {
                    logger.info(
                            "\n\t\t###################################"
                                    + "\n\t\t    Ready to process operations    "
                                    + "\n\t\t###################################");

                    runtime = Runtime.getRuntime();

                    initialMemory = runtime.totalMemory() - runtime.freeMemory();


                    init = false;
                }
            }

            try {


                logger.info("zsdqwd Before the prewait");

                ArrayList<Decision> decisions = new ArrayList<>();
                decidedLock.lock();
                if (decided.isEmpty()) {
                    notEmptyQueue.await();
                }

                logger.debug("zsdqwd Current size of the decided queue: {}", decided.size());
                decided.drainTo(decisions, 1);

//				if (controller.getStaticConf().getSameBatchSize()) {
//					decided.drainTo(decisions, 1);
//				} else {
//					decided.drainTo(decisions);
//				}

                decidedLock.unlock();

                if (!doWork)
                    break;

                if (decisions.size() > 0) {

                    TOMMessage[][] requests = new TOMMessage[decisions.size()][];


                    int[] consensusIds = new int[requests.length];
                    int[] leadersIds = new int[requests.length];
                    int[] regenciesIds = new int[requests.length];
                    CertifiedDecision[] cDecs;
                    cDecs = new CertifiedDecision[requests.length];
                    int count = 0;
                    for (Decision d : decisions) {
                        requests[count] = extractMessagesFromDecision(d);




                        TOMMessage[] reqs = extractMessagesFromDecision(d);
                        for (TOMMessage req : reqs) {


                            logger.debug("proving cid for request: {}, with cid:{}", req, d.getConsensusId());

                            tomLayer.clientsManager.provideCIDForRequest(req, d.getConsensusId());
                        }





                        consensusIds[count] = d.getConsensusId();
                        leadersIds[count] = d.getLeader();
                        regenciesIds[count] = d.getRegency();

                        CertifiedDecision cDec = new CertifiedDecision(this.controller.getStaticConf().getProcessId(),
                                d.getConsensusId(), d.getValue(), d.getDecisionEpoch().proof);
                        cDecs[count] = cDec;

                        // cons.firstMessageProposed contains the performance counters

                        if (requests[count][0].equals(d.firstMessageProposed)) {
                            long time = requests[count][0].timestamp;
                            long seed = requests[count][0].seed;
                            int numOfNonces = requests[count][0].numOfNonces;
                            requests[count][0] = d.firstMessageProposed;
                            requests[count][0].timestamp = time;
                            requests[count][0].seed = seed;
                            requests[count][0].numOfNonces = numOfNonces;
                        }

                        count++;
                    }

                    Decision lastDecision = decisions.get(decisions.size() - 1);


                    lastcid = lastDecision.getConsensusId();

                    logger.debug("saving lastdecision for cid: {} with consid {}",
                            lastcid, lastDecision.getConsensusId());
                    
                    
                    readWriteLock.writeLock().lock();


                    LastDecisionSaved.put(lastcid, lastDecision);

                    long consensusEndTime = System.nanoTime();

                    logger.debug("Ending Consensus for cId:{},  " +
                                    "consensusEndTime: {}",
                            lastcid, consensusEndTime);



                    sending_other_clusters(consensusIds, regenciesIds, leadersIds,
                            cDecs, requests, decisions, lastDecision);


                    long MCEndTime = System.nanoTime();

                    logger.debug("Ending MC for cId:{},  " +
                                    "MCLatency: {}",
                            lastcid, MCEndTime - consensusEndTime);

                    HashMap<Integer, OtherClusterMessage> temp = SavedMultiClusterMessages.get(lastcid);
                    logger.debug("Main LOOP othermsgs_received_mc for tid: {}, is temp size, nclusters is {}, {}, temp keyset {}", lastcid, temp.size(), this.cinfo.nClusters, temp.keySet());

                    if (othermsgs_received_mc(lastcid)) {

                        Integer temp_cid = lastcid;

                        if (times_tracker.containsKey(lastcid))
                        {


                            long[] values = times_tracker.get(temp_cid);
                            values[2] = (System.currentTimeMillis() );
                            times_tracker.put(temp_cid, values);
                        }


                        executeMessages(lastcid);

//                        if (controller.hasUpdates() && lastcid - lastReconfig > 1 && this.getNodeId()!=7)
//                        {
//                            logger.info("DeliveryThread: has updates, processing reconfig for reconfig no: "+ lastPendingReconfig +
//                                    "with lastcid: " + lastcid);
//                            processReconfigMessages(lastPendingReconfig);
//                            lastReconfig = lastcid;
//                        }

                        if (times_tracker.containsKey(lastcid))
                        {


                            long[] values = times_tracker.get(temp_cid);
                            values[3] = (System.currentTimeMillis() );
                            times_tracker.put(temp_cid, values);
                        }


                    }


//
//                    finalMemory = runtime.totalMemory() - runtime.freeMemory();
//                    memoryUsed = finalMemory - initialMemory;
//                    memoryUsedInMB = memoryUsed / (1024.0 * 1024.0);
//
//                    MemoryMXBean memoryBean = ManagementFactory.getMemoryMXBean();
//                    long usedMemory = memoryBean.getHeapMemoryUsage().getUsed();
//
//
//                    logger.info(" cid ={}, Memory Used: {} MB, size of SavedMultiClusterMessages = {}," +
//                                    "Sizeof  SavedDecisionsToBeExecuted {}, " +
//                                    "sizeof  SavedMessagesForExec{}, sizeof decided = {}, " +
//                                    "sizeof LastDecisionSaved {}, usedMemory: {}", lastcid,
//                            memoryUsedInMB, SavedMultiClusterMessages.keySet().size(),
//                            SavedDecisionsToBeExecuted.size(), SavedMessagesForExec.size(),
//                            decided.size(), LastDecisionSaved.size(), usedMemory);


//                    logger.info("Adding to DoneMainLoopExec, cid: {}", lastcid);
//                    DoneMainLoopExec.put(lastcid,1);

                    readWriteLock.writeLock().unlock();


                }
            } catch (Exception e) {
                logger.error("Error while processing decision", e);
            }

            // THIS IS JOAO'S CODE, TO HANDLE STATE TRANSFER
            //deliverUnlock();
            //******************************************************************
            deliverLock.unlock();
        }
        logger.info("DeliveryThread stopped.");

    }

    private TOMMessage[] extractMessagesFromDecision(Decision dec) {
        logger.debug("extractMessagesFromDecision deserialized");
        TOMMessage[] requests = dec.getDeserializedValue();
        if (requests == null) {
            // there are no cached deserialized requests
            // this may happen if this batch proposal was not verified
            // TODO: this condition is possible?

            logger.debug("Interpreting and verifying batched requests.");

            // obtain an array of requests from the decisions obtained
            BatchReader batchReader = new BatchReader(dec.getValue(), controller.getStaticConf().getUseSignatures() == 1);
            requests = batchReader.deserialiseRequests(controller);
        } else {
            logger.debug("Using cached requests from the propose.");
        }

        return requests;
    }

    public void deliverUnordered(TOMMessage request, int regency) {

        MessageContext msgCtx = new MessageContext(request.getSender(), request.getViewID(), request.getReqType(),
                request.getSession(), request.getSequence(), request.getOperationId(), request.getReplyServer(),
                request.serializedMessageSignature, System.currentTimeMillis(), 0, 0, regency, -1, -1, null, null,
                false); // Since the request is unordered,
        // there is no consensus info to pass

        logger.debug("deliverUnordered READ ONLY");

        msgCtx.readOnly = true;
        receiver.receiveReadonlyMessage(request, msgCtx);
    }

    private void deliverMessages(int[] consId, int[] regencies, int[] leaders, CertifiedDecision[] cDecs,
                                 TOMMessage[][] requests) {
        receiver.receiveMessages(consId, regencies, leaders, cDecs, requests);
    }

    // no utility function
    private void deliverMessages(int[] consId, int[] regencies, int[] leaders, CertifiedDecision[] cDecs,
                                 TOMMessage[][] requests, ArrayList<OtherClusterMessage> ocmArray) throws IOException, ClassNotFoundException {
        receiver.receiveMessages(consId, regencies, leaders, cDecs, requests, ocmArray);
    }

    public void processReconfigMessages(int consId) {

        if (!reconfigProcessed.contains(consId))
        {
            reconfigProcessed.add(consId);

            byte[] response = controller.executeUpdates(consId);
//            byte[] response = null;

            TOMMessage[] dests = controller.clearUpdates();
//            logger.info("processReconfigMessages for cid:"+consId+
//                    "controller.getCurrentViewId(): "+controller.getCurrentViewId());

            if (controller.getCurrentView().isMember(receiver.getId())) {
                for (TOMMessage dest : dests) {

                    logger.debug("sending reconfig reply with dest.getSender() are {}, dest.getSequence: {}, dest.getOperationId(): {}", dest.getSender(),
                            dest.getSequence(), dest.getOperationId());

                    TOMMessage replyreconfig = new TOMMessage(controller.getStaticConf().getProcessId(), dest.getSession(),
                              dest.getSequence(), dest.getOperationId(), response,
                                    controller.getCurrentViewId(), RECONFIG);

                    replyreconfig.setreconfignodeid(dest.getSender());

                    receiver.replyReconfig(replyreconfig);


                }

//            tomLayer.getCommunication().updateServersConnections();

            } else {
                logger.info("Supposed to Restarting receiver normally");
                receiver.restart();
            }

        }

    }

    public void shutdown() {
        this.doWork = false;

        logger.info("Shutting down delivery thread");

        decidedLock.lock();
        notEmptyQueue.signalAll();
        decidedLock.unlock();
    }

    public void DTsignalWaitingForLCMessageOCReply() {
        decidedLockOtherClustersReconfig.lock();
        this.notEmptyQueueOtherClustersReconfig.signalAll();
        decidedLockOtherClustersReconfig.unlock();
    }

    public int getNodeIdDT() {
        return this.receiver.getId();
    }

    public int getLastCID() {
        return this.lastcid;
    }


    public int getNodeId() {
        return this.receiver.getId();
    }

    public void signalMCWaiting() {
        logger.info("Signalling DT to proceed after reconfig");
        decidedLockOtherClusters.lock();
        notEmptyQueueOtherClusters.signalAll();//reset_rvc_timeout();
        decidedLockOtherClusters.unlock();
    }

    public void signalReconfigConfirmationNewNode() {
        ReconfigLockMC.lock();
        ReconfigLockMC.unlock();
    }

    public void signalRemoteChange(SystemMessage sm) {
        if (((SMMessage) sm).getCID() > lastLCLockMsg) {

            logger.info("Signalling LcLockMCCondition to proceed with msg CID: {}, lastLCLockMsg: {} ",
                    ((SMMessage) sm).getCID(), lastLCLockMsg);

            lastLCLockMsg = ((SMMessage) sm).getCID();
        }

    }


    public void sendLastOcmd() {

        try {
            logger.info("sending last ocmd cid start: {} with consid {}", this.lastocmd.getOcmd().from_cid_start, Arrays.toString(this.lastocmd.getOcmd().consId));
        } catch (IOException | ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
        HashMap<Integer, HostsConfig.Config> hostmap = cinfo.getAllConnectionsMap();
        int clusterid = hostmap.get(this.receiver.getId()).ClusterNumber;


        List<Integer> tgtList = new ArrayList<Integer>();

        for (int i : hostmap.keySet()) {
            if (cinfo.getAllConnectionsMap().get(i).ClusterNumber != clusterid) {
                tgtList.add(i);
            }

        }
        //							logger.info("tgtList is {}", tgtList);
        int[] tgtArray = tgtList.stream().filter(Objects::nonNull).mapToInt(Integer::intValue).toArray();

        this.tomLayer.getCommunication().send(tgtArray, this.lastocmd);

        logger.debug("SENT last ocmd");


    }

    public void increase_rvc_timeout(int cid) {
        decidedLockOtherClusters.lock();

        logger.info("increased rvc_timeout");

        this.rvc_timeout += 20;
        this.last_rvc_msg = cid;


        decidedLockOtherClusters.unlock();


    }


    public void store_start_consensus_time(int consensusId) {

        long[] values = {((System.currentTimeMillis() )), -1, -1, -1};
        times_tracker.put(consensusId, values);

    }

    public void deliveryReconfig(ReconfigMessage msg) throws IOException, ClassNotFoundException {

        readWriteLock.writeLock().lock();
        try {
            ReconfigMsgData reconfigData = msg.getReconfigData();
            int type = reconfigData.getType();
            int tempKey = reconfigData.getSeq();
//            logger.info("deliveryReconfig for reconfig seq: {} for type: {}", tempKey, type);

            if (type == 1) {  // Type 1: Initial processing
                if (this.receiver.getId() == tomLayer.execManager.getCurrentLeader()) {


                    pending_recs_tracker.put(tempKey, pending_recs_tracker.getOrDefault(tempKey, 0) + 1);
//                    logger.info("pending_recs_tracker.get(tempKey) is "+ pending_recs_tracker.get(tempKey));

                    if (pending_recs_tracker.get(tempKey) >= (controller.getCurrentViewN() - controller.getCurrentViewF())) {
                        logger.debug("Enough agg messages received for reconfig seq: " + tempKey);

                        pending_recs_tracker.put(tempKey, -1);
                        pending_recs_tracker.remove(tempKey - 1);

//                        logger.info("AFter enough pending_recs_tracker.get(tempKey) is "+ pending_recs_tracker.get(tempKey));


                        int[] tgtArray = controller.getOrigViewAcceptors();



//                        logger.info("going to send agg msg to tgtArray" + Arrays.toString(tgtArray));

                        ReconfigMessage newMsg2 = new ReconfigMessage(reconfigData.getSeq(),
                                this.getNodeId(), reconfigData.getFromConfig(), 2);

                        this.tomLayer.getCommunication().send(tgtArray, newMsg2);

                    }
                }
            }

            if (type == 2) {  // Type 2: Aggregation processing
                int[] tgtArray = controller.getOrigViewAcceptors();
                ReconfigMessage newMsg = new ReconfigMessage(reconfigData.getSeq(),
                        this.getNodeId(), reconfigData.getFromConfig(), 3);
                this.tomLayer.getCommunication().send(tgtArray, newMsg);

            }

            if (type == 3) {  // Type 3: Echo processing
                reconfig_echo_tracker.put(tempKey, reconfig_echo_tracker.getOrDefault(tempKey, 0) + 1);
                if (reconfig_echo_tracker.get(tempKey) >= (controller.getCurrentViewN() - controller.getCurrentViewF())) {
                    logger.debug("Enough echo messages received for reconfigfor reconfig seq:" + tempKey);

                    reconfig_echo_tracker.put(tempKey, -1);
                    reconfig_echo_tracker.remove(tempKey - 1);

                    int[] tgtArray = controller.getOrigViewAcceptors();
                    ReconfigMessage newMsg = new ReconfigMessage(reconfigData.getSeq(),
                            this.getNodeId(), reconfigData.getFromConfig(), 4);
                    this.tomLayer.getCommunication().send(tgtArray, newMsg);

                }
            }

            if (type == 4) {  // Type 4: Ready processing
                reconfig_ready_counter.put(tempKey, reconfig_ready_counter.getOrDefault(tempKey, 0) + 1);
                if (reconfig_ready_counter.get(tempKey) >= (controller.getCurrentViewN() - controller.getCurrentViewF())) {
                    logger.debug("Enough ready messages received for reconfig for reconfig seq" + tempKey);

                    reconfig_ready_counter.put(tempKey, -1);
                    reconfig_ready_counter.remove(tempKey - 1);

//                    if (lastcid - lastReconfig > 1 || this.getNodeId()==7)
                    {

                        logger.debug("deliveryReconfig: has updates, processing reconfig for reconfig no: "+ reconfigData.getSeq() +
                                "with lastcid: " + lastcid);
                        processReconfigMessages(reconfigData.getSeq());
//                        lastReconfig = lastcid;

                    }
//                    else
//                    {
//                        logger.info("delaying reconfiguration for reconfig seq: {}", reconfigData.getSeq());
//                        lastPendingReconfig = reconfigData.getSeq();
//                    }

                }
            }
        } finally {
            readWriteLock.writeLock().unlock();  // Always unlock in a finally block to prevent deadlocks
        }
    }

    public void sendRecs(TOMMessage request) throws IOException {
        readWriteLock.writeLock().lock();


        if (this.receiver.getId() != tomLayer.execManager.getCurrentLeader() )
        {

            ReconfigMessage newocmd = new ReconfigMessage(request.getSequence(),
                    this.getNodeId(), this.receiver.getConfig(), 1);

            int[] tgtArray = new int[1];
            tgtArray[0] = tomLayer.execManager.getCurrentLeader();
            logger.debug("sending recs msg");
            this.tomLayer.getCommunication().send(tgtArray, newocmd);

        }
        else
        {

            int tempkey = request.getSequence();
            pending_recs_tracker.put(tempkey,
                    pending_recs_tracker.getOrDefault(tempkey,0)+1);

        }

        readWriteLock.writeLock().unlock();


    }



}