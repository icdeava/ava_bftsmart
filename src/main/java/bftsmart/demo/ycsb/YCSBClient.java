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
package bftsmart.demo.ycsb;

import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Properties;
import java.util.Set;
import java.util.Vector;
import java.util.concurrent.atomic.AtomicInteger;

import bftsmart.demo.counter.ClusterInfo;
import bftsmart.reconfiguration.VMServices;
import bftsmart.tom.ServiceProxy;

import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.Client;
import com.yahoo.ycsb.DB;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author Marcel Santos
 *
 */
public class YCSBClient extends DB {

    private static AtomicInteger counter = new AtomicInteger();
    private ServiceProxy proxy = null;
    private int myId;

    VMServices vms;


    private static AtomicInteger txn_counter_atomic = new AtomicInteger();

    public int TxnCounter = 0;

    Client c;


    MemoryMXBean memoryBean = ManagementFactory.getMemoryMXBean();
    long usedMemory = memoryBean.getHeapMemoryUsage().getUsed();


    int reconfigCLient = -2;




    int nnodes ;
    int[] smartIds ;
    String[] ipAddresses ;

    int port;
    int portRR;
    String config_id;

    int ClientID;




    public YCSBClient() {
    }

    @Override
    public void init() {
        Properties props = getProperties();
        int initId = Integer.valueOf((String) props.get("smart-initkey"));
        ClientID = Integer.valueOf((String) props.get("ClientID"));
        ClusterInfo cinfo = new ClusterInfo();

        int ncls = cinfo.nClusters;

        myId = initId +20* ( (int) (ClientID/ncls) )*ClientID+counter.addAndGet(1);
//        myId = initId + counter.addAndGet(1);



        proxy = new ServiceProxy(myId, "config"+Integer.toString(ClientID%ncls));


        vms = new VMServices(null,"config"+Integer.toString(ClientID%ncls));




        nnodes = Integer.parseInt("1");
        smartIds = new int[nnodes];
        ipAddresses = new String[nnodes];


//        System.out.println("cinfo.hm.get(17).host is "+ cinfo.hm.get(17).host);

        for (int i = 0; i < nnodes;i++)
        {
            smartIds[i] = Integer.parseInt("7");
            ipAddresses[i] = cinfo.hm.get(7).host;
        }

        port = Integer.parseInt("10000");
        portRR = Integer.parseInt("20000");
        config_id = "config0";


        System.out.println("YCSBKVClient. Initiated client with  myId: " + myId+ ", ClientID"
                + ClientID+ " ncls: "+ncls+ ", initId: "+ initId);
    }

    @Override
    public int delete(String arg0, String arg1) {
        throw new UnsupportedOperationException();
    }

    @Override
    public int insert(String table, String key,
                      HashMap<String, ByteIterator> values) {

        Iterator<String> keys = values.keySet().iterator();
        HashMap<String, byte[]> map = new HashMap<>();
        while (keys.hasNext()) {
            String field = keys.next();
            map.put(field, values.get(field).toArray());
        }
        YCSBMessage msg = YCSBMessage.newInsertRequest(table, key, map);
        byte[] reply = proxy.invokeOrdered(msg.getBytes());
        YCSBMessage replyMsg = YCSBMessage.getObject(reply);
        return replyMsg.getResult();
    }

    @Override
    public int read(String table, String key,
                    Set<String> fields, HashMap<String, ByteIterator> result) {
        HashMap<String, byte[]> results = new HashMap<>();
        YCSBMessage request = YCSBMessage.newReadRequest(table, key, fields, results);
        if (ClientID !=reconfigCLient)
        {
            byte[] reply = proxy.invokeUnordered(request.getBytes());
            YCSBMessage replyMsg = YCSBMessage.getObject(reply);
            return replyMsg.getResult();
        }
        return 1;

    }

    @Override
    public int scan(String arg0, String arg1, int arg2, Set<String> arg3,
                    Vector<HashMap<String, ByteIterator>> arg4) {
        throw new UnsupportedOperationException();
    }

    @Override
    public int update(String table, String key,
                      HashMap<String, ByteIterator> values) {
        Iterator<String> keys = values.keySet().iterator();
        HashMap<String, byte[]> map = new HashMap<>();
        while (keys.hasNext()) {
            String field = keys.next();
            map.put(field, values.get(field).toArray());
        }
        YCSBMessage msg = YCSBMessage.newUpdateRequest(table, key, map);


        usedMemory = memoryBean.getHeapMemoryUsage().getUsed();

        byte[] reply = null;
        if (ClientID!=reconfigCLient)
        {
            reply = proxy.invokeOrdered(msg.getBytes());

        }


        YCSBMessage replyMsg = null;


        if (reply!=null)
        {
            replyMsg = YCSBMessage.getObject(reply);

        }



//

//      inital sleep before starting to issue reconfig
        if ((TxnCounter==0) && (ClientID==reconfigCLient))
        {
            try {
                for (int iter=0;iter<25;iter++)
                {
                    Thread.sleep(1000);
                    System.out.println("1 second passed");

                }
            } catch (InterruptedException ex) {
                System.out.println("ex is "+ex.getMessage());
            }
            vms.removeServers(smartIds, TxnCounter);
            TxnCounter = txn_counter_atomic.addAndGet(1);

            vms.addServerMultiple(smartIds, ipAddresses, port, portRR, TxnCounter);
            TxnCounter = txn_counter_atomic.addAndGet(1);

            try {
                for (int iter=0;iter<55;iter++)
                {
                    Thread.sleep(1000);
                    System.out.println("1 second passed");

                }
            } catch (InterruptedException ex) {
                System.out.println("ex is "+ex.getMessage());
            }

        }







        boolean successfull_reconfig = true;

        int upperLimit = 4000001;

        if ((TxnCounter> upperLimit)&& (ClientID==reconfigCLient))
        {

            try {
                for (int iter=0;iter<300;iter++)
                {
                    Thread.sleep(1000);
                    System.out.println("1 second passed");

                }            } catch (InterruptedException ex) {
                System.out.println("ex is "+ex.getMessage());
            }
        }
//
//
        if (ClientID==reconfigCLient)
        {
            if( (TxnCounter<=upperLimit)&&(TxnCounter>=0)&&((TxnCounter)%2==1) )
            {
//                System.out.println("test123: add server request");
                successfull_reconfig = vms.addServerMultiple(smartIds, ipAddresses, port, portRR, TxnCounter);

//                if (!successfull_reconfig)
//                {
//                    TxnCounter = txn_counter_atomic.addAndGet(1);
//                }

            }
            if ((TxnCounter<=upperLimit)&& (TxnCounter>=0)&&((TxnCounter)%2==0) )
            {
//                System.out.println("test123: leave server request");

                successfull_reconfig = vms.removeServers(smartIds, TxnCounter);
//
//                if (!successfull_reconfig)
//                {
//                    TxnCounter = txn_counter_atomic.addAndGet(1);
//                }
            }
        }



        TxnCounter = txn_counter_atomic.addAndGet(1);

        // sleep between reconfig reqs

        if (ClientID==reconfigCLient)
        {
            try {

                Thread.sleep(20000);
                System.out.println("20 second passed");

            } catch (InterruptedException ex)
            {
                System.out.println("ex is "+ex.getMessage());
            }
        }


        if (reply!=null)
        {
            return replyMsg.getResult();

        }
        else
        {
            return 1;
        }


    }

}
