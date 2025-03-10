/**
 * Copyright (c) 2007-2013 Alysson Bessani, Eduardo Alchieri, Paulo Sousa, and the authors indicated in the @author tags
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package bftsmart.statemanagement.standard;

import bftsmart.consensus.Decision;
import bftsmart.demo.counter.ClusterInfo;
import bftsmart.statemanagement.StateManager;

import java.util.*;
import java.util.concurrent.locks.ReentrantLock;

import bftsmart.tom.core.ExecutionManager;
import bftsmart.consensus.messages.ConsensusMessage;
import bftsmart.consensus.messages.MessageFactory;
import bftsmart.reconfiguration.views.View;
import bftsmart.statemanagement.ApplicationState;
import bftsmart.statemanagement.SMMessage;
import bftsmart.tom.core.DeliveryThread;
import bftsmart.tom.core.TOMLayer;
import bftsmart.tom.core.messages.TOMMessage;
import bftsmart.tom.server.defaultservices.DefaultApplicationState;
import bftsmart.tom.util.TOMUtil;
import bftsmart.consensus.Consensus;
import bftsmart.consensus.Epoch;
import bftsmart.tom.leaderchange.CertifiedDecision;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.naming.ldap.HasControls;

/**
 *
 * @author Marcel Santos
 *
 */
public class StandardStateManager extends StateManager {

    private Logger logger = LoggerFactory.getLogger(this.getClass());

    private int replica;
    private ReentrantLock lockTimer = new ReentrantLock();
    private Timer stateTimer = null;
    private final static long INIT_TIMEOUT = 40000;
    private long timeout = INIT_TIMEOUT;

    private ClusterInfo cinfo = new ClusterInfo();

    @Override
    public void init(TOMLayer tomLayer, DeliveryThread dt) {

        super.init(tomLayer, dt);

        changeReplica(); // initialize replica from which to ask the complete state

    }

    private void changeReplica() {
        logger.info("Changing Replica");
        int[] processes = this.SVController.getCurrentViewOtherAcceptors();
        Random r = new Random();

        int pos;
        do {
            //pos = this.SVController.getCurrentViewPos(replica);
            //replica = this.SVController.getCurrentViewProcesses()[(pos + 1) % SVController.getCurrentViewN()];

            if (processes != null && processes.length > 1) {
                pos = r.nextInt(processes.length);
                replica = processes[pos];
            } else {
                replica = 0;
                break;
            }
        } while (replica == SVController.getStaticConf().getProcessId());
    }

    @Override
    protected void requestState() {
        if (tomLayer.requestsTimer != null) {
            tomLayer.requestsTimer.clearAll();
        }

        changeReplica(); // always ask the complete state to a different replica

        SMMessage smsg = new StandardSMMessage(SVController.getStaticConf().getProcessId(),
                waitingCID, TOMUtil.SM_REQUEST, replica, null, null, -1, -1);
        tomLayer.getCommunication().send(SVController.getCurrentViewOtherAcceptors(), smsg);

        logger.info("I just sent a request to the other replicas for the state up to CID {} TO {}", waitingCID, SVController.getCurrentViewOtherAcceptors());

        TimerTask stateTask = new TimerTask() {
            public void run() {
                logger.info("Timeout to retrieve state");
                StandardSMMessage msg = new StandardSMMessage(-1, waitingCID, TOMUtil.TRIGGER_SM_LOCALLY, -1, null, null, -1, -1);
                triggerTimeout(msg);
            }
        };

        stateTimer = new Timer("state timer");
        
        
        timeout = timeout + 20;
        logger.info("timeout is {}",timeout);

        stateTimer.schedule(stateTask, timeout);
    }

    @Override
    public void stateTimeout() {
        lockTimer.lock();
        logger.debug("Timeout for the replica that was supposed to send the complete state. Changing desired replica.");
        if (stateTimer != null) {
            stateTimer.cancel();
        }
        changeReplica();
        reset();
        requestState();
        lockTimer.unlock();
    }

    @Override
    public void SMRequestDeliver(SMMessage msg, boolean isBFT) {
        if (SVController.getStaticConf().isStateTransferEnabled() && dt.getRecoverer() != null) {
            StandardSMMessage stdMsg = (StandardSMMessage) msg;
            boolean sendState = stdMsg.getReplica() == SVController.getStaticConf().getProcessId();



            ApplicationState thisState = dt.getRecoverer().getState(msg.getCID(), sendState);



//            if (thisState.getCertifiedDecision(SVController) == null) {
            if (thisState == null) {

                logger.info("For some reason, I am sending a void state, {}, msg cid:  {}", thisState, msg.getCID());
                thisState = dt.getRecoverer().getState(-1, sendState);
            }


            thisState.setStateHmap(dt.getHmap());
//            thisState.setDecidedOtherClusters(dt.getDecidedOtherClusters());

            logger.info("Sending State, State.getLastCID() is {}, {} with hmap:{}",
                    thisState, thisState.getLastCID(), thisState.getStateHmap());


            int[] targets = {msg.getSender()};
            SMMessage smsg = new StandardSMMessage(SVController.getStaticConf().getProcessId(),
                    msg.getCID(), TOMUtil.SM_REPLY, -1, thisState, SVController.getCurrentView(),
                    tomLayer.getSynchronizer().getLCManager().getLastReg(), tomLayer.execManager.getCurrentLeader());

            logger.info("Sending state to {} ... with msg CID: {}, with thisState: {}, state: {}, " +
                            "smsg.getState().getCertifiedDecision(SVController): {}",
                    targets, msg.getCID(), thisState, smsg.getState(),smsg.getState().getCertifiedDecision(SVController));
            tomLayer.getCommunication().send(targets, smsg);
            logger.info("Sent");

//            tomLayer.signalMCWaiting();
        }
    }

    @Override
    public void SMReplyDeliver(SMMessage msg, boolean isBFT) {
        lockTimer.lock();
        if (SVController.getStaticConf().isStateTransferEnabled()) {
            if (waitingCID != -1 && msg.getCID() == waitingCID) {
                int currentRegency = -1;
                int currentLeader = -1;
                View currentView = null;
                CertifiedDecision currentProof = null;

                if (!appStateOnly) {
                    senderRegencies.put(msg.getSender(), msg.getRegency());
                    senderLeaders.put(msg.getSender(), msg.getLeader());
                    senderViews.put(msg.getSender(), msg.getView());
                    senderProofs.put(msg.getSender(), msg.getState().getCertifiedDecision(SVController));
                    logger.info("msg.getState().getCertifiedDecision(SVController) is {} and msg.getState(), msg.getSender()" +
                                    "is {}, {}, ((StandardSMMessage)msg).getState() ): {}",
                            msg.getState().getCertifiedDecision(SVController), msg.getState(), msg.getSender(),
                            ((StandardSMMessage)msg).getState() );

                    if (enoughRegencies(msg.getRegency())) {
                        currentRegency = msg.getRegency();
                    }
                    if (enoughLeaders(msg.getLeader())) {
                        currentLeader = msg.getLeader();
                    }
                    if (enoughViews(msg.getView())) {
                        currentView = msg.getView();
                    }
                    if (enoughProofs(waitingCID, this.tomLayer.getSynchronizer().getLCManager())) {
                        currentProof = msg.getState().getCertifiedDecision(SVController);
                        logger.info("XXXXXXX--- currentProof is {}",currentProof);
                    }

                } else {
                    currentLeader = tomLayer.execManager.getCurrentLeader();
                    currentRegency = tomLayer.getSynchronizer().getLCManager().getLastReg();
                    currentView = SVController.getCurrentView();
                }

                if (msg.getSender() == replica && msg.getState().getSerializedState() != null) {
                    logger.info("Expected replica sent state. Setting it to state");
                    state = msg.getState();
                    if (stateTimer != null) {
                        stateTimer.cancel();
                    }
                }

                senderStates.put(msg.getSender(), msg.getState());

                logger.info("Verifying more than F replies");
                if (enoughReplies()) {
                    logger.info("More than F confirmed");
                    ApplicationState otherReplicaState = getOtherReplicaState();


                    int haveState = 0;
                    if (state != null) {
                        byte[] hash = null;
                        hash = tomLayer.computeHash(state.getSerializedState());
                        if (otherReplicaState != null) {
                            if (Arrays.equals(hash, otherReplicaState.getStateHash())) {
                                haveState = 1;
                            } else if (getNumEqualStates() > SVController.getCurrentViewF()) {
                                haveState = -1;
                            }
                        }
                    }

//                    haveState = 1;
                    logger.info("----OPOPOPO----- otherReplicaState, haveState, currentRegency, currentLeader, currentView != null,isBFT" +
                            " currentProof != null, appStateOnly are {}, {}, {}, {}, {}, {}, {} and {}",
                            otherReplicaState, haveState, currentRegency, currentLeader, currentView != null,isBFT,
                            currentProof != null,appStateOnly);

//                    if (otherReplicaState != null && haveState == 1 && currentRegency > -1
                    logger.info("check Vars: {}, {}, {}, {}, {}",otherReplicaState != null,
                            currentRegency > -1, currentLeader > -1, currentView != null,  (!isBFT || appStateOnly));
                    if (otherReplicaState != null && currentRegency > -1
                            && currentLeader > -1 && currentView != null && (!isBFT || currentProof != null || appStateOnly)) {
//                            && currentLeader > -1 && currentView != null) {

                        logger.info("Received state. Will install it");
                        msg.getState();

                        tomLayer.getSynchronizer().getLCManager().setLastReg(currentRegency);
                        tomLayer.getSynchronizer().getLCManager().setNextReg(currentRegency);
                        tomLayer.getSynchronizer().getLCManager().setNewLeader(currentLeader);
                        tomLayer.execManager.setNewLeader(currentLeader);

//                        if (currentProof != null && !appStateOnly) {
                        if (!appStateOnly) {

                            logger.info("Installing proof for consensus " + waitingCID);

                            Consensus cons = execManager.getConsensus(waitingCID);
                            Epoch e = null;

                            for (ConsensusMessage cm : currentProof.getConsMessages()) {

                                e = cons.getEpoch(cm.getEpoch(), true, SVController);
                                if (e.getTimestamp() != cm.getEpoch()) {

                                    logger.info("Strange... proof contains messages from more than just one epoch");
                                    e = cons.getEpoch(cm.getEpoch(), true, SVController);
                                }
                                e.addToProof(cm);

                                if (cm.getType() == MessageFactory.ACCEPT) {
                                    e.setAccept(cm.getSender(), cm.getValue());
                                } else if (cm.getType() == MessageFactory.WRITE) {
                                    e.setWrite(cm.getSender(), cm.getValue());
                                }

                            }

//                            if (e != null) {
                            if (e != null) {

                                byte[] hash = tomLayer.computeHash(currentProof.getDecision());
                                e.propValueHash = hash;
                                e.propValue = currentProof.getDecision();
                                e.deserializedPropValue = tomLayer.checkProposedValue(currentProof.getDecision(), false);
                                cons.decided(e, false);
                                
                                
                                long startTime = System.nanoTime();
                                double st = (startTime) / 1_000_000_000.0;

                                logger.info("Successfully installed proof for consensus " + waitingCID+ 
                                        ", with time(sec) = "+ st);
                                


                            } else {

//                                byte[] hash = null;
//                                e.propValueHash = new byte[2];
//                                e.propValue = new byte[2];
//                                e.deserializedPropValue = new TOMMessage[1];
//                                cons.decided(e, false);


                                logger.error("Failed to install proof for consensus CORRECTLY: " + waitingCID);

                            }

                        }

                        // I might have timed out before invoking the state transfer, so
                        // stop my re-transmission of STOP messages for all regencies up to the current one
                        if (currentRegency > 0) {
                            tomLayer.getSynchronizer().removeSTOPretransmissions(currentRegency - 1);
                        }
                        //if (currentRegency > 0)
                        //    tomLayer.requestsTimer.setTimeout(tomLayer.requestsTimer.getTimeout() * (currentRegency * 2));

                        dt.pauseDecisionDelivery();
                        waitingCID = -1;

//                        if (state!=null)
                        dt.update(state);

                        if (!appStateOnly && execManager.stopped()) {
                            Queue<ConsensusMessage> stoppedMsgs = execManager.getStoppedMsgs();
                            for (ConsensusMessage stopped : stoppedMsgs) {
                                if (stopped.getNumber() > state.getLastCID() /*msg.getCID()*/) {
                                    execManager.addOutOfContextMessage(stopped);
                                }
                            }
                            execManager.clearStopped();
                            execManager.restart();
                        }

                        tomLayer.processOutOfContext();

                        if (SVController.getCurrentViewId() != currentView.getId()) {
                            logger.info("Installing current view!");
                            SVController.reconfigureTo(currentView);
                        }

                        isInitializing = false;

                        dt.canDeliver();
                        dt.resumeDecisionDelivery();

                        reset();

                        logger.info("I updated the state!");

                        SMMessage smsg = new StandardSMMessage(SVController.getStaticConf().getProcessId(),
                                waitingCID, TOMUtil.NEW_NODE_READY, replica, null, null, -1, -1);

                        tomLayer.getCommunication().send(SVController.getCurrentViewOtherAcceptors(), smsg);


                        tomLayer.requestsTimer.Enabled(true);
                        tomLayer.requestsTimer.startTimer();

                        if (stateTimer != null) {
                            stateTimer.cancel();
                        }

                        if (appStateOnly) {
                            logger.info("ffafsaxz --- resumeLC is happening");
                            appStateOnly = false;
                            tomLayer.getSynchronizer().resumeLC();
                        }

                    } else if (otherReplicaState == null && (SVController.getCurrentViewN() / 2) < getReplies()) {

                        logger.info("otherReplicaState == null  && (SVController.getCurrentViewN() / 2) < getReplies()");

                        waitingCID = -1;
                        reset();

                        if (stateTimer != null) {
                            stateTimer.cancel();
                        }

                        if (appStateOnly) {
                            logger.info("appStateOnly requesting state");
                            requestState();
                        }
                    } else if (haveState == -1) {
                        logger.info("The replica from which I expected the state, sent one which doesn't match the hash of the others, or it never sent it at all");

                        changeReplica();
                        reset();
                        requestState();

                        if (stateTimer != null) {
                            stateTimer.cancel();
                        }
                    } else if (haveState == 0 && (SVController.getCurrentViewN() - SVController.getCurrentViewF()) <= getReplies()) {

                       logger.info("Could not obtain the state, retrying");
                        reset();
                        if (stateTimer != null) {
                            stateTimer.cancel();
                        }
                        waitingCID = -1;
                        //requestState();
                    } else {
                        logger.info("State transfer not yet finished");

                    }
                }
            }
        }
        lockTimer.unlock();
    }

    /**
     * Search in the received states table for a state that was not sent by the
     * expected replica. This is used to compare both states after received the
     * state from expected and other replicas.
     *
     * @return The state sent from other replica
     */
    private ApplicationState getOtherReplicaState() {
        int[] processes = SVController.getCurrentViewProcesses();
        for (int process : processes) {
            if (process == replica) {
                continue;
            } else {
                ApplicationState otherState = senderStates.get(process);
                if (otherState != null) {
                    return otherState;
                }
            }
        }
        return null;
    }

    private int getNumEqualStates() {
        List<ApplicationState> states = new ArrayList<ApplicationState>(receivedStates());
        int match = 0;
        for (ApplicationState st1 : states) {
            int count = 0;
            for (ApplicationState st2 : states) {
                if (st1 != null && st1.equals(st2)) {
                    count++;
                }
            }
            if (count > match) {
                match = count;
            }
        }
        return match;
    }

}
