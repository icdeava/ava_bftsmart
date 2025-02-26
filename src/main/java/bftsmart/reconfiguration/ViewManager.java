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
package bftsmart.reconfiguration;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.LinkedList;
import java.util.List;
import java.util.StringTokenizer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import bftsmart.communication.server.ServerConnection;
import bftsmart.reconfiguration.views.View;
import bftsmart.tom.util.KeyLoader;
import java.util.Arrays;
import java.util.HashMap;

/**
 *
 * @author eduardo
 */
public class ViewManager {

    private Logger logger = LoggerFactory.getLogger(this.getClass());
    
    private int id;
    private Reconfiguration rec = null;
    //private Hashtable<Integer, ServerConnection> connections = new Hashtable<Integer, ServerConnection>();
    private ServerViewController controller;
    //Need only inform those that are entering the systems, as those already
    //in the system will execute the reconfiguration request
    private List<Integer> addIds = new LinkedList<Integer>();
    
    
    private HashMap<Integer, ServerConnection> connections = new HashMap<Integer, ServerConnection>();


    public ViewManager(KeyLoader loader) {
        this("config0", loader);
    }

    public ViewManager(String configHome, KeyLoader loader) {
        this.id = loadID(configHome);
        this.controller = new ServerViewController(id, configHome, loader);
        this.rec = new Reconfiguration(id, configHome, loader);
    }

    public void connect(){
        this.rec.connect();
    }
    
    private int loadID(String configHome) {
        try {
            String path = "";
            String sep = System.getProperty("file.separator");
            if (configHome == null || configHome.equals("")) {
                path = "config" + sep + "system.config";
            } else {
                path = configHome + sep + "system.config";
            }
            FileReader fr = new FileReader(path);
            BufferedReader rd = new BufferedReader(fr);
            String line = null;
            while ((line = rd.readLine()) != null) {
                if (!line.startsWith("#")) {
                    StringTokenizer str = new StringTokenizer(line, "=");
                    if (str.countTokens() > 1
                            && str.nextToken().trim().equals("system.ttp.id")) {
                        fr.close();
                        rd.close();
                        return Integer.parseInt(str.nextToken().trim());
                    }
                }
            }
            fr.close();
            rd.close();
            return -1;
        } catch (Exception e) {
            logger.error("Could not load ID", e);
            return -1;
        }
    }

    public void addServer(int id, String ip, int port, int portRR) {
        this.controller.getStaticConf().addHostInfo(id, ip, port, portRR);
        rec.addServer(id, ip, port, portRR);
        addIds.add(id);
    }

    public void removeServer(int id) {
        rec.removeServer(id);
    }

    public void setF(int f) {
        rec.setF(f);
    }
    

    public void executeUpdates() {

        logger.info("executeUpdates(), addIds: " + addIds);
        connect();

        boolean successful_reconfig = false;

        ReconfigureReply r = new ReconfigureReply();

        View v;

        while(successful_reconfig!=true)
        {
            try
                {
                    r = rec.execute();
                    v = r.getView();
//                    logger.info();
                    logger.info("New view f: " + v.getId()+", processes: "+Arrays.toString(v.getProcesses())+ ", v.getN(): "+
                            v.getN());
                    successful_reconfig = true;


                }
            catch (Exception e) 
                {
                    logger.info("Exception caught with addIds: "+addIds );
                }

        }



        VMMessage msg = new VMMessage(id, r);


        Integer[] addIds_array = new Integer[addIds.size()];
        for (int i = 0; i < addIds.size(); i++) {
            addIds_array[i] = addIds.get(i);
        }


        if (addIds.size() > 0) { 

            logger.info("addIds.toArray(new Integer[1]) is "+ addIds_array);
            sendResponse(addIds_array, msg);
            addIds.clear();
        }
    }

    private ServerConnection getConnection(int remoteId) {
         return new ServerConnection(controller, null, remoteId, null, null);
    }

    public void sendResponse(Integer[] targets, VMMessage sm) {
        ByteArrayOutputStream bOut = new ByteArrayOutputStream();

        try {
            new ObjectOutputStream(bOut).writeObject(sm);
        } catch (IOException ex) {
            logger.error("Could not serialize message", ex);
        }

        byte[] data = bOut.toByteArray();

        for (Integer i : targets) {
            try {
                if (i.intValue() != id) {
                    
                    
                    
                    if(this.connections.containsKey(i.intValue()))
                    {
                        if (this.connections.get(i.intValue())!=null)
                        {
                            this.connections.get(i.intValue()).send(data);

                        }
                        else
                        {
                            this.connections.put(i.intValue(), getConnection(i.intValue()));
                            this.connections.get(i.intValue()).send(data);
                            
                        }
                        
                    }
                    else
                    {
                        
                        this.connections.put(i.intValue(), getConnection(i.intValue()));
                        this.connections.get(i.intValue()).send(data);

                    }
                    
//                    getConnection(i.intValue()).send(data);
                    
                    
                }
            } catch (InterruptedException ex) {
               // ex.printStackTrace();
                logger.error("Failed to send data to target", ex);
            }
        }
    }

    public void close() {
        rec.close();
    }

    public boolean executeUpdates(int cid) {
        
        
        
        logger.info("addIds: " + addIds+ ", cid: "+cid);
        connect();

        boolean successful_reconfig = false;

        ReconfigureReply r = new ReconfigureReply();

        View v;
        
        int counter = 0;

//        while((successful_reconfig!=true)) // &&(counter<10)
        {
            try
                {
                    boolean check = (rec==null);
//                    logger.info("Trying reconfig execution, check="+ check);
                    
                    r = rec.execute(cid);
                    v = controller.getCurrentView();//r.getView();
                    logger.info("New view with id: " + v.getId()+
                            ", for cid: "+cid+", processes: "+Arrays.toString(v.getProcesses())+ ", v.getN(): "+
                            v.getN());
                    successful_reconfig = true;


                }
            catch (Exception e) 
                {
                    logger.info("Exception caught with addIds: "+addIds 
                    + ", cid: "+ cid+ " exception: "+e.getMessage());
                    
                    e.printStackTrace();
                    
                    counter = counter + 1;
                    
                    return successful_reconfig;
                }

        }



        VMMessage msg = new VMMessage(id, r);


        Integer[] addIds_array = new Integer[addIds.size()];
        for (int i = 0; i < addIds.size(); i++) {
            addIds_array[i] = addIds.get(i);
        }


        if (addIds.size() > 0) { 

            logger.info("addIds.toArray(new Integer[1]) is "+ addIds_array);
            sendResponse(addIds_array, msg);
            addIds.clear();
        }
        return successful_reconfig;
    }
}
