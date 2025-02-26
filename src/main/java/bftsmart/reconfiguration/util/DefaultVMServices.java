/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package bftsmart.reconfiguration.util;

import bftsmart.reconfiguration.VMServices;
import bftsmart.tom.util.KeyLoader;

/**
 *
 * @author joao
 */
public class DefaultVMServices extends VMServices {

    public DefaultVMServices()
    {
        super();
    }


    public DefaultVMServices(KeyLoader keyLoader, String config_id) {
        super(keyLoader, config_id);
    }

    public static void main(String[] args) throws InterruptedException {

        System.out.println("args.length is "+args.length);

        if(args.length < 5){
            System.out.println("####Tpp Service[Disjoint]####");

//            int smartId = Integer.parseInt(args[0]);
            
            int[] smartIds = new int[args.length];

            for(int iter = 0; iter < args.length;iter++)
            {
                smartIds[iter] = Integer.parseInt(args[iter]);
            }
            
//            (new DefaultVMServices()).removeServer(smartId);
            
            (new DefaultVMServices()).removeServers(smartIds, -1);
                
        }else if(args.length >= 5){
            System.out.println("####Tpp Service[Join]####");
            int nnodes = Integer.parseInt(args[0]);
            
            
            int[] smartIds = new int[nnodes];
            String[] ipAddresses = new String[nnodes];

            for (int i = 0; i < nnodes;i++)
            {
                
                smartIds[i] = Integer.parseInt(args[1+i*5]);
                ipAddresses[i] = args[2+i*5];

            }
            
            int port = Integer.parseInt(args[3]);
            int portRR = Integer.parseInt(args[4]);
            String config_id = args[5];


//            (new DefaultVMServices(null, config_id)).addServer(smartId, ipAddress, port, portRR);

            System.out.println("####Tpp Service[Join]####"+smartIds +", "+ipAddresses);

            (new DefaultVMServices(null, config_id)).addServerMultiple(smartIds, ipAddresses, port, portRR, -1);

//            (new DefaultVMServices()).addServer(smartId, ipAddress, port, portRR);

        }else{
            System.out.println("Usage: java -jar TppServices <smart id> [ip address] [port]");
            System.exit(1);
        }

        Thread.sleep(2000);//2s
        

        System.exit(0);
    }


}
