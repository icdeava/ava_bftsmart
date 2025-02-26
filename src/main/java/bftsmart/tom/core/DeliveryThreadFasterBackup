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
import bftsmart.demo.counter.ClusterInfo;
import bftsmart.reconfiguration.ServerViewController;
import bftsmart.reconfiguration.util.HostsConfig;
import bftsmart.statemanagement.ApplicationState;
import bftsmart.statemanagement.SMMessage;
import bftsmart.statemanagement.standard.StandardSMMessage;
import bftsmart.tom.MessageContext;
import bftsmart.tom.ServiceReplica;
import bftsmart.tom.core.messages.ForwardedMessage;
import bftsmart.tom.core.messages.TOMMessage;
import bftsmart.tom.core.messages.TOMMessageType;
import bftsmart.tom.leaderchange.CertifiedDecision;
import bftsmart.tom.leaderchange.LCMessage;
import bftsmart.tom.leaderchange.LCMessageOtherCluster;
import bftsmart.tom.server.Recoverable;
import bftsmart.tom.util.BatchReader;
import bftsmart.tom.util.TOMUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.print.attribute.standard.MediaSize;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.*;

import static bftsmart.tom.core.messages.TOMMessageType.RECONFIG;

/**
 * This class implements a thread which will deliver totally ordered requests to
 * the application
 *
 */
public final class DeliveryThread extends Thread {
    private final Logger logger = LoggerFactory.getLogger(this.getClass());
    private boolean doWork = true;
    private int lastReconfig = -2;
    private int lastcid = -2;

    private int lastLCLockMsg = -2;

    private int last_skip_cid = -2;
    private OtherClusterMessage lastocmd;

    private final LinkedBlockingQueue<Decision> decided;


    private HashMap<Integer, Integer> hmap = new HashMap<Integer, Integer>();


    /** Tejas Code**/

    private final LinkedBlockingQueue<OtherClusterMessage> decidedOtherClusters;
    /** Tejas Ends **/

    private final TOMLayer tomLayer; // TOM layer
    private final ServiceReplica receiver; // Object that receives requests from clients




    private final Recoverable recoverer; // Object that uses state transfer
    private final ServerViewController controller;
    private final Lock decidedLock = new ReentrantLock();

    private final Condition notEmptyQueue = decidedLock.newCondition();

    /** Tejas **/

    private final Lock decidedLockOtherClusters = new ReentrantLock();

    private final Lock ReconfigLockMC = new ReentrantLock();
    private final Lock LcLockMC = new ReentrantLock();

    private final Condition notEmptyQueueOtherClusters = decidedLockOtherClusters.newCondition();

    private final Condition ReconfigLockMCCondition = ReconfigLockMC.newCondition();
    private final Condition LcLockMCCondition = LcLockMC.newCondition();



    private final Lock decidedLockOtherClustersReconfig = new ReentrantLock();
    private final Condition notEmptyQueueOtherClustersReconfig = decidedLockOtherClustersReconfig.newCondition();

    /** Tejas END **/

    //Variables used to pause/resume decisions delivery
    private final Lock pausingDeliveryLock = new ReentrantLock();
    private final Condition deliveryPausedCondition = pausingDeliveryLock.newCondition();
    private int isPauseDelivery;

    private int rvc_timeout = 80;
    private int last_rvc_msg = -1;

    OtherClusterMessage ocmd;

    ConcurrentHashMap<Integer, Decision> LastDecisionSaved = new ConcurrentHashMap<Integer, Decision>();


    ConcurrentHashMap<Integer, HashMap<Integer, OtherClusterMessage>> SavedMultiClusterMessages =
            new ConcurrentHashMap<Integer, HashMap<Integer, OtherClusterMessage>>();


    ConcurrentHashMap<Integer, TOMMessage[][]> SavedMessagesForExec =
            new ConcurrentHashMap<Integer, TOMMessage[][]>();

    Set<Integer> ReceivedOtherClusterMsgs = new HashSet<>();

    ConcurrentHashMap<Integer, OtherClusterMessageData> SavedDecisionsToBeExecuted = new ConcurrentHashMap<Integer, OtherClusterMessageData>();
    ConcurrentHashMap<Integer, Integer> DoneMainLoopExec = new ConcurrentHashMap<Integer, Integer>();


    ReadWriteLock readWriteLock = new ReentrantReadWriteLock();


    ClusterInfo cinfo;


    public HashMap<Integer, Integer> getHmap()
    {
        return hmap;
    }
    public void setHmap(HashMap<Integer, Integer> hmp)
    {
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

        for (int i=0; i < cinfo.nClusters;i++) hmap.put(i,-1);
    }

    public Recoverable getRecoverer() {
        return recoverer;
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
            TOMMessage[] requests = extractMessagesFromDecision(dec);
            tomLayer.clientsManager.requestsOrdered(requests);
            logger.info("Consensus " + dec.getConsensusId() + " finished. Decided size=" + decided.size());

        } catch (Exception e) {
            logger.info("Could not insert decision into decided queue and mark requests as delivered", e);
        }

        if (!containsReconfig(dec)) {
            logger.info("Decision from consensus " + dec.getConsensusId() + " does not contain reconfiguration");
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



    public void executeMessages(int tid)
    {
        logger.info("executeMessages for tid: " + tid);

        OtherClusterMessageData tempOcmd = null;
        try {
            tempOcmd = SavedMultiClusterMessages.get(tid).get(this.cinfo.getClusterNumber(getNodeId())).getOcmd();
        } catch (IOException e) {
            throw new RuntimeException(e);
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }


        Decision lastDecision = LastDecisionSaved.get(tid);

        logger.info("Getting lastdecision for cid: {}", tid);

        deliverMessages(tempOcmd.consId,tempOcmd.regencies, tempOcmd.leaders,
                tempOcmd.cDecs, SavedMessagesForExec.get(tid));


        if ((tid%1000==0) && (tid>1100))
        {
            logger.info("deleting old info from tid: {} to {}",tid-1100, tid-100);
            for (int i = tid - 1100; i< tid-100 ; i++)
            {

                SavedMessagesForExec.remove(i);
                LastDecisionSaved.remove(i);
                SavedMultiClusterMessages.remove(i);
                DoneMainLoopExec.remove(i);
            }

        }

        // ******* EDUARDO BEGIN ***********//
        if (controller.hasUpdates()) {
            processReconfigMessages(lastDecision.getConsensusId());
        }
        if (lastReconfig > -2 && lastReconfig <= lastDecision.getConsensusId()) {

            // set the consensus associated to the last decision as the last executed
            logger.debug("Setting last executed consensus to " + lastDecision.getConsensusId());
            tomLayer.setLastExec(lastDecision.getConsensusId());
            // define that end of this execution
            tomLayer.setInExec(-1);
            // ******* EDUARDO END **************//

            lastReconfig = -2;
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


    }

    public void deliveryOtherCluster(OtherClusterMessage msg) throws IOException, ClassNotFoundException
    {
        readWriteLock.writeLock().lock();


        if(msg.getOcmd().type==1)
        {

            logger.info("Tejas: reached inside deliveryOtherCluster type 1, from_cid_start, from, fromConfig, msg.getSender()," +
                            " msg instanceof Forwarded are {}, {}, {}, {}, {}, getCurrentViewN: {}", msg.getOcmd().from_cid_start,
                    msg.getOcmd().from
                    , msg.getOcmd().fromConfig, msg.getSender(),((SystemMessage) msg instanceof ForwardedMessage),
                    controller.getCurrentViewN());


//			int clusterid = cinfo.getAllConnectionsMap().get(this.receiver.getId()).ClusterNumber;


            int[] tgtArray = controller.getCurrentViewOtherAcceptors();
//
//
            OtherClusterMessage newocmd = new OtherClusterMessage(msg.getOcmd().consId, msg.getOcmd().regencies, msg.getOcmd().leaders,
                    msg.getOcmd().cDecs, msg.getOcmd().requests,
                    this.getNodeId(), msg.getOcmd().fromConfig, msg.getOcmd().from_cid_start,
                    msg.getOcmd().from_cid_end, 2);
//			msg.setOcmdType(2);

            logger.info("Sending type 2 message to {}, with msg= {}", tgtArray, newocmd);

            this.tomLayer.getCommunication().send(tgtArray, newocmd);

            logger.info("Sent type 2 message to {} with msg = {}", tgtArray, newocmd);
//			this.tomLayer.getCommunication().send(tgtArray, this.ocmd);

        }

        if (msg.getOcmd().type==2)
        {

            logger.info("Tejas: reached inside deliveryOtherCluster type 2, from_cid_start, from, fromConfig, msg.getSender()," +
                            " are {}, {}, {}, {},  getCurrentViewN: {}", msg.getOcmd().from_cid_start,
                    msg.getOcmd().from
                    , msg.getOcmd().fromConfig, msg.getSender(),
                    controller.getCurrentViewN());

            int clusterId = Integer.parseInt(
                    msg.getOcmd().fromConfig.replaceAll("[^0-9]",
                            ""));






            HashMap<Integer, OtherClusterMessage> tempMap= SavedMultiClusterMessages.get(msg.getOcmd().from_cid_start);



            if (tempMap==null)	tempMap = new HashMap<Integer, OtherClusterMessage>();

            tempMap.put(clusterId, msg);



            SavedMultiClusterMessages.put(msg.getOcmd().from_cid_start, tempMap);





            if (SavedMessagesForExec.containsKey(msg.getOcmd().from_cid_start))
            {
                TOMMessage[][] requests2 = SavedMessagesForExec.get(msg.getOcmd().from_cid_start);

                for (TOMMessage[] requestsFromConsensus : requests2) {
                    for (TOMMessage request : requestsFromConsensus) {
                        logger.info("Checking ReqType3 request.getReqType() is {}, with keyset {}",
                                request.getReqType(), SavedMultiClusterMessages.get(msg.getOcmd().from_cid_start).keySet());
                    }
                }
            }






            logger.info("DoneMainLoopExec.contains(msg.getOcmd().from_cid_start) for cid: {}, is : {}",
                    msg.getOcmd().from_cid_start, DoneMainLoopExec.contains(msg.getOcmd().from_cid_start));

            if (othermsgs_received_mc(msg.getOcmd().from_cid_start))
            {
                logger.info("executing for tid: {}", msg.getOcmd().from_cid_start);
                executeMessages(msg.getOcmd().from_cid_start);
            }
            else logger.info("Not executing for tid: {}", msg.getOcmd().from_cid_start);


        }


        readWriteLock.writeLock().unlock();


    }




    public boolean othermsgs_received_mc(int tid)
    {


        HashMap<Integer, OtherClusterMessage> temp = SavedMultiClusterMessages.get(tid);
        logger.info("othermsgs_received_mc for tid: {}, is temp size, nclusters is {}, {}, temp keyset {}",tid, temp.size(), this.cinfo.nClusters, temp.keySet());



        return temp.size() == this.cinfo.nClusters;


    }



    private boolean containsReconfig(Decision dec) {
        TOMMessage[] decidedMessages = dec.getDeserializedValue();

        for (TOMMessage decidedMessage : decidedMessages) {
            if (decidedMessage.getReqType() == RECONFIG
                    && decidedMessage.getViewID() == controller.getCurrentViewId()) {
                return true;
            }
        }
        return false;
    }
    /** THIS IS JOAO'S CODE, TO HANDLE STATE TRANSFER */
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

        if ( lastCID >0 )
        {
            tomLayer.setLastExec(lastCID + 1);

        }
        else
        {
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

        if (2>1) {

            HashMap<Integer, HostsConfig.Config> hostmap = cinfo.getAllConnectionsMap();
            int clusterid = hostmap.get(this.receiver.getId()).ClusterNumber;


            List<Integer> tgtList = new ArrayList<Integer>();

            //						for (int i=0; i < this.cinfo.totalCount; i++)
            for (int i : hostmap.keySet()) {
                if (cinfo.getAllConnectionsMap().get(i).ClusterNumber != clusterid) {
                    tgtList.add(i);
                }

            }
            //							logger.info("tgtList is {}", tgtList);
            int[] tgtArray = tgtList.stream().filter(i -> i != null).mapToInt(Integer::intValue).toArray();



            try {
                logger.info("\n\n\n\n\n SENDING AFTER RECONFIG to {} with ocmd from_cid_start: {}",
                        tgtArray,lastocmd.getOcmd().from_cid_start);
            } catch (IOException | ClassNotFoundException e) {
                throw new RuntimeException(e);
            }

            this.ocmd.getOcmd().setSkipIter(true);
            this.tomLayer.getCommunication().send(tgtArray, this.ocmd);




        }
    }


    public void sending_other_clusters(int[] consensusIds, int[] regenciesIds, int[] leadersIds,
                                       CertifiedDecision[] cDecs, TOMMessage[][] requests,
                                       ArrayList<Decision> decisions, Decision lastDecision) throws InterruptedException, IOException, ClassNotFoundException
    {



        logger.info("-XOXOXOXOX---- {}, {}, {}, {}, {}, {}, {} are with currentViewID: {} and time = {}",
                consensusIds, regenciesIds, leadersIds, cDecs, requests,
                this.receiver.getId(), this.receiver.getConfig(), controller.getCurrentViewId(), System.currentTimeMillis());
        /** Tejas START **/

        this.ocmd = new OtherClusterMessage(consensusIds, regenciesIds, leadersIds, cDecs, requests,
                this.receiver.getId(), this.receiver.getConfig(), decisions.get(0).getConsensusId(),
                lastDecision.getConsensusId(), 2);



        int clusterid = Integer.parseInt(
                this.ocmd.getOcmd().fromConfig.replaceAll("[^0-9]",
                        ""));


//	HashMap<Integer, OtherClusterMessage> tempMap2= SavedMultiClusterMessages.get(this.ocmd.getOcmd().from_cid_start);
//	TOMMessage[][] requests2 = tempMap2.get(clusterid).getOcmd().requests;
//	TOMMessage[][] requests2 = tempMap.get(clusterid).getOcmd().requests;

//	for (TOMMessage[] requestsFromConsensus : requests2) {
//		for (TOMMessage request : requestsFromConsensus) {
//			logger.info("Checking ReqType request.getReqType() is {}", request.getReqType());
//		}
//	}



        int[] tgtArray = cinfo.getFPlusOneArray(clusterid).stream().filter(Objects::nonNull).mapToInt(Integer::intValue).toArray();

        logger.info("\n\n\n\n\n\n\n\n tgtArray, consensusIds, consensusIds[0], lastcid is {}, {}, {}, {}, ocmd = {}", tgtArray,
                consensusIds, consensusIds[0], lastcid, this.ocmd);



//	if (2>1)
        if ((lastcid!=-1500) &&  (this.receiver.getId() == tomLayer.execManager.getCurrentLeader()) )
        {
            //									logger.info("\n\n\n\n\n SENDING OTHER CLUSTERS THE DECIDED VALUES");
            this.tomLayer.getCommunication().send(tgtArray, this.ocmd);

        }
        else
        {
            logger.info("Not sending multicluster msg, clusterid==1 is  {}", clusterid==1);
            //                this.tomLayer.getCommunication().send(tgtArray, this.ocmd);
        }
        logger.info("OtherClusterMessage Sent to {}", tgtArray);




        HashMap<Integer, OtherClusterMessage> tempMap= SavedMultiClusterMessages.get(this.ocmd.getOcmd().from_cid_start);





        if (tempMap==null)	tempMap = new HashMap<Integer, OtherClusterMessage>();

        tempMap.put(clusterid, this.ocmd);



        logger.info("saving msg for execution, with tid: {}, requests: {}",
                this.ocmd.getOcmd().from_cid_start,	requests);




        SavedMessagesForExec.put(this.ocmd.getOcmd().from_cid_start, requests);

        SavedMultiClusterMessages.put(this.ocmd.getOcmd().from_cid_start, tempMap);



        TOMMessage[][] requests2 = SavedMessagesForExec.get(this.ocmd.getOcmd().from_cid_start);

        for (TOMMessage[] requestsFromConsensus : requests2) {
            for (TOMMessage request : requestsFromConsensus) {
                logger.info("Checking ReqType4 request.getReqType() is {}", request.getReqType());
            }
        }


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

                logger.info("zsdqwd Current size of the decided queue: {}", decided.size());
                decided.drainTo(decisions, 1);

//				if (controller.getStaticConf().getSameBatchSize()) {
//					decided.drainTo(decisions, 1);
//				} else {
//					decided.drainTo(decisions);
//				}

                decidedLock.unlock();

                if (!doWork)
                    break;

                if (decisions.size() > 0)
                {
                    TOMMessage[][] requests = new TOMMessage[decisions.size()][];
                    int[] consensusIds = new int[requests.length];
                    int[] leadersIds = new int[requests.length];
                    int[] regenciesIds = new int[requests.length];
                    CertifiedDecision[] cDecs;
                    cDecs = new CertifiedDecision[requests.length];
                    int count = 0;
                    for (Decision d : decisions) {
                        requests[count] = extractMessagesFromDecision(d);
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

                    logger.info("saving lastdecision for cid: {} with consid {}",
                            lastcid, lastDecision.getConsensusId());

                    LastDecisionSaved.put(lastcid, lastDecision);



                    readWriteLock.writeLock().lock();

                    sending_other_clusters(consensusIds, regenciesIds, leadersIds,
                            cDecs, requests, decisions, lastDecision);

                    HashMap<Integer, OtherClusterMessage> temp = SavedMultiClusterMessages.get(lastcid);
                    logger.info("Main LOOP othermsgs_received_mc for tid: {}, is temp size, nclusters is {}, {}, temp keyset {}",lastcid, temp.size(), this.cinfo.nClusters, temp.keySet());

                    if (othermsgs_received_mc(lastcid))
                    {
                        executeMessages(lastcid);
                    }


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

        msgCtx.readOnly = true;
        receiver.receiveReadonlyMessage(request, msgCtx);
    }

    private void deliverMessages(int[] consId, int[] regencies, int[] leaders, CertifiedDecision[] cDecs,
                                 TOMMessage[][] requests) {
        receiver.receiveMessages(consId, regencies, leaders, cDecs, requests);
    }


    private void deliverMessages(int[] consId, int[] regencies, int[] leaders, CertifiedDecision[] cDecs,
                                 TOMMessage[][] requests, ArrayList<OtherClusterMessage> ocmArray) throws IOException, ClassNotFoundException
    {
        receiver.receiveMessages(consId, regencies, leaders, cDecs, requests, ocmArray);
    }

    private void processReconfigMessages(int consId) {
        byte[] response = controller.executeUpdates(consId);
        TOMMessage[] dests = controller.clearUpdates();
//		logger.info("dests are {}", dests);

        if (controller.getCurrentView().isMember(receiver.getId())) {
            for (TOMMessage dest : dests) {

                logger.info("dest.getSender() are {}", dest.getSender());
                tomLayer.getCommunication().send(new int[]{dest.getSender()},
                        new TOMMessage(controller.getStaticConf().getProcessId(), dest.getSession(),
                                dest.getSequence(), dest.getOperationId(), response,
                                controller.getCurrentViewId(), RECONFIG));
            }

            tomLayer.getCommunication().updateServersConnections();

        } else {
            logger.info("Restarting receiver");
            receiver.restart();
        }
    }

    public void shutdown() {
        this.doWork = false;

        logger.info("Shutting down delivery thread");

        decidedLock.lock();
        notEmptyQueue.signalAll();
        decidedLock.unlock();
    }

    public void DTsignalWaitingForLCMessageOCReply()
    {
        decidedLockOtherClustersReconfig.lock();
        this.notEmptyQueueOtherClustersReconfig.signalAll();
        decidedLockOtherClustersReconfig.unlock();
    }

    public int getNodeIdDT()
    {
        return this.receiver.getId();
    }

    public int getLastCID() {
        return this.lastcid;
    }


    public int getNodeId()
    {
        return this.receiver.getId();
    }

    public void signalMCWaiting() {
        logger.info("Signalling DT to proceed after reconfig");
        decidedLockOtherClusters.lock();
        notEmptyQueueOtherClusters.signalAll();//reset_rvc_timeout();
        decidedLockOtherClusters.unlock();
    }

    public LinkedBlockingQueue<OtherClusterMessage> getDecidedOtherClusters() {
//		LinkedBlockingQueue<OtherClusterMessage> tmp = decidedOtherClusters;
        return decidedOtherClusters;
    }

    public void setDecidedOtherClusters(LinkedBlockingQueue<OtherClusterMessage> DOC) {
        decidedLockOtherClusters.lock();

        for (OtherClusterMessage ocm:DOC)
        {
            try {
                this.decidedOtherClusters.put(ocm);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }

        decidedLockOtherClusters.unlock();
    }

    public void signalReconfigConfirmationNewNode() {
        ReconfigLockMC.lock();
//		ReconfigLockMCCondition.signalAll();
        ReconfigLockMC.unlock();
    }

    public void signalRemoteChange(SystemMessage sm) {
        if( ((SMMessage) sm).getCID() > lastLCLockMsg)
        {

            try {
                logger.info("lastcid is {}, last ocmd's cid is {}",lastcid, lastocmd.getOcmd().from_cid_start);
            } catch (IOException e) {
                throw new RuntimeException(e);
            } catch (ClassNotFoundException e) {
                throw new RuntimeException(e);
            }





            logger.info("Signalling LcLockMCCondition to proceed with msg CID: {}, lastLCLockMsg: {} ",
                    ((SMMessage) sm).getCID(), lastLCLockMsg);
            LcLockMC.lock();
            LcLockMCCondition.signalAll();
            LcLockMC.unlock();
            lastLCLockMsg = ((SMMessage) sm).getCID();
        }

    }


    public void sendLastOcmd()
    {
        HashMap<Integer, HostsConfig.Config> hostmap = cinfo.getAllConnectionsMap();
        int clusterid = hostmap.get(this.receiver.getId()).ClusterNumber;


        List<Integer> tgtList = new ArrayList<Integer>();

        //						for (int i=0; i < this.cinfo.totalCount; i++)
        for (int i : hostmap.keySet()) {
            if (cinfo.getAllConnectionsMap().get(i).ClusterNumber != clusterid) {
                tgtList.add(i);
            }

        }
        //							logger.info("tgtList is {}", tgtList);
        int[] tgtArray = tgtList.stream().filter(Objects::nonNull).mapToInt(Integer::intValue).toArray();

        this.tomLayer.getCommunication().send(tgtArray, this.ocmd);

    }

    public void increase_rvc_timeout(int cid) {
        decidedLockOtherClusters.lock();

        logger.info("increased rvc_timeout");

        this.rvc_timeout += 20;
        this.last_rvc_msg = cid;


        decidedLockOtherClusters.unlock();


    }


    public void reset_rvc_timeout() {
//		decidedLockOtherClusters.lock();

        logger.info("reset rvc_timeout");

        this.rvc_timeout = 20;
//		decidedLockOtherClusters.unlock();


    }



    /*
     * public int size() { return decided.size(); }
     */
}