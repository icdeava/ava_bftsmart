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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import bftsmart.tom.util.KeyLoader;

/**
 * This class is used by the trusted client to add and remove replicas from the group
 */

public class VMServices {
    
    private KeyLoader keyLoader;
    private String configDir= "config0";
    
    private ViewManager VM;
    
    private Logger logger = LoggerFactory.getLogger(this.getClass());
    
    /**
     * Constructor. It adopts the default RSA key loader and default configuration path.
     */
    public VMServices() { // for the default keyloader and provider
        
        keyLoader = null;
        configDir = "config0";
    }

    
    /**
     * Constructor.
     * 
     * @param keyLoader Key loader to use to fetch keys from disk
     * @param configDir Configuration path
     */
    public VMServices(KeyLoader keyLoader, String configDir) {
        
        this.keyLoader = keyLoader;
        this.configDir = configDir;
    }
    
    /**
     * Adds a new server to the group
     * 
     * @param id ID of the server to be added (needs to match the value in config/hosts.config)
     * @param ipAddress IP address of the server to be added (needs to match the value in config/hosts.config)
     * @param port Port of the server to be added (needs to match the value in config/hosts.config)
     */
//    public void addServer(int id, String ipAddress, int port, int portRR) {
//        
//        ViewManager viewManager = new ViewManager(configDir, keyLoader);
//        
//        viewManager.addServer(id, ipAddress, port, portRR);
//        
//        execute(viewManager);
//
//    }
    
    
        public boolean addServerMultiple(int[] ids, String[] ipAddresss, int port, int portRR, int cid) {
            
            logger.info("cid for join requests"+cid);
            
            if (this.VM==null)
            {
                this.VM = new ViewManager(configDir, keyLoader);

            }

            for(int i=0; i < ids.length;i++)
            {
                int id = ids[i];
                String ipAddress = ipAddresss[i];
                        
                this.VM.addServer(id, ipAddress, port, portRR);
                
            }

        
        return execute(this.VM,cid);

    }
    
    /**
     * Removes a server from the group
     * 
     * @param id ID of the server to be removed 
     */
//    public void removeServer (int id) {
//        
//        ViewManager viewManager = new ViewManager(keyLoader);
//        
//        viewManager.removeServer(id);
//        
//        execute(viewManager);
//
//    }
//    
        /**
     * Removes a server from the group
     * 
     * @param ids
     */
    public boolean removeServers (int[] ids, int cid) {

        if (this.VM==null)
        {
            this.VM = new ViewManager(configDir, keyLoader);

        }      
        
        for (int id:ids)
        {
            this.VM.removeServer(id);

        }
        
        logger.info("cid for leave requests"+cid);

        return execute(this.VM, cid);

    }
//    public void updateClusters() {
//
//        ViewManager viewManager = new ViewManager(keyLoader);
//
//        execute(viewManager);
//
//    }
    
    

    
    public boolean execute(ViewManager viewManager, int cid) {
        
    logger.info("cid for execute"+cid);


    boolean succesfull_reconfig = viewManager.executeUpdates(cid);

//    viewManager.close();
    
    return succesfull_reconfig;
    }
    
    
}