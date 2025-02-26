///**
// Copyright (c) 2007-2013 Alysson Bessani, Eduardo Alchieri, Paulo Sousa, and the authors indicated in the @author tags
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
// http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
// */
//package bftsmart.demo.counter;
//
//import java.io.ByteArrayInputStream;
//import java.io.ByteArrayOutputStream;
//import java.io.DataInputStream;
//import java.io.DataOutputStream;
//import java.io.IOException;
//
//import bftsmart.tom.ServiceProxy;
//import com.yahoo.ycsb.Client;
//
///**
// * Example client that updates a BFT replicated service (a counter).
// *
// * @author alysson
// */
//public class CounterClient {
//
//    public static void main(String[] args) throws IOException {
//        if (args.length < 2) {
//            System.out.println("Usage: java ... CounterClient <process id> <increment> [<number of operations>]");
//            System.out.println("       if <increment> equals 0 the request will be read-only");
//            System.out.println("       default <number of operations> equals 1000");
//            System.exit(-1);
//        }
////        Client
//        int newValue = -1;
//
//        ClusterInfo cinfo = new ClusterInfo();
//
//
//        ServiceProxy[] ServiceProxyArr = new ServiceProxy[cinfo.nClusters];
//
//        for (int i=0; i < cinfo.nClusters;i++)
//        {
//            ServiceProxyArr[i] = new ServiceProxy(Integer.parseInt(args[0]), "config"+Integer.toString(i));
//        }
//
//        long startTime = System.nanoTime();
//
//        try {
//
//            int inc = Integer.parseInt(args[1]);
//            int numberOfOps = (args.length > 2) ? Integer.parseInt(args[2]) : 1000;
//            byte[] reply;
//
//
//            for (int i = 0; i < numberOfOps; i++) {
//
//                ByteArrayOutputStream out = new ByteArrayOutputStream(4);
//                new DataOutputStream(out).writeInt(inc);
//
//                System.out.print("Invocation " + i);
//
//
//                for (int iter = 0; iter < cinfo.nClusters;iter++)
//                {
//                    reply = (inc == 0)?
//                            ServiceProxyArr[iter].invokeUnordered(out.toByteArray()):
//                            ServiceProxyArr[iter].invokeOrdered(out.toByteArray());
//
//
//                    if(reply != null) {
//                        newValue = new DataInputStream(new ByteArrayInputStream(reply)).readInt();
//                    } else {
//                        System.out.println(", ERROR! Exiting.");
//                        break;
//                    }
//
//                }
//
//                System.out.println(", returned value: " + newValue);
//
//
//
//
//
//
//            }
//        } catch(IOException | NumberFormatException e){
//            for (int i=0; i < cinfo.nClusters;i++)
//            {
//                ServiceProxyArr[i].close();
//            }
//
//        }
//
//        long estimatedTime = System.nanoTime() - startTime;
//
//        System.out.println("Time elapsed is "+estimatedTime);
//
//
//    }
//}