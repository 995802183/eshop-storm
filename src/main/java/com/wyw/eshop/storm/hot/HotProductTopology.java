package com.wyw.eshop.storm.hot;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import org.apache.storm.utils.Utils;

public class HotProductTopology {


    public static void main(String[]args){
        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout("AccessLogKafkaSpout",new AccessLogKafkaSpout(),1);
        builder.setBolt("LogParseBolt",new LogParseBolt(),5)
                .setNumTasks(1)
                .shuffleGrouping("AccessLogKafkaSpout");
        builder.setBolt("HotProductCountBolt",new HotProductCountBolt(),1)
                .setNumTasks(1)
                .fieldsGrouping("LogParseBolt",new Fields("productId"));

        Config config = new Config();
        if(args != null && args.length > 0){
            config.setNumWorkers(3);
            try {
                StormSubmitter.submitTopology(args[0],config,builder.createTopology());
            }catch (Exception e){
                e.printStackTrace();
            }
        }else{
            LocalCluster localCluster = new LocalCluster();
            localCluster.submitTopology("HotProductTopology",config,builder.createTopology());
            Utils.sleep(30000);
            localCluster.shutdown();
        }
    }
}
