package bftsmart.demo.ycsb;

import bftsmart.tom.ServiceProxy;

import java.util.concurrent.CountDownLatch;

class Task extends Thread
{

    private int id;
    private String config;

    private byte[] reply;


    private ServiceProxy sProx;


    private YCSBMessage msg;

    private final CountDownLatch latch;

    public Task(int id, String config, CountDownLatch latch)
    {
        this.id = id;
        this.config = config;

        this.sProx = new ServiceProxy(this.id, this.config);
        this.latch = latch;

    }


    public Task(int id, String config)
    {
        this.id = id;
        this.config = config;

        this.sProx = new ServiceProxy(this.id, this.config);
        this.latch = null;
    }


    public void setMsg(YCSBMessage msg)
    {
        this.msg = msg;
    }



    @Override
    public void run()
    {
        latch.countDown();
        try
        {

//            this.reply = this.sProx.invokeOrdered(msg.getBytes());
            this.sProx.invokeOrderedNoReply(msg.getBytes());

//            if(this.reply == null) {
//                System.out.println(", ERROR! Exiting.");
//            }


        }
        catch (Exception e)
        {
            e.printStackTrace();
        }
    }
}