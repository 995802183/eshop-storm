package com.wyw.eshop.storm.hello;

import org.apache.log4j.Logger;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;

/**
 * @author wyw
 */
public class HelloworldTopology {
    private static final Logger logger = Logger.getLogger(HelloworldTopology.class);
    private static class SentenceSpout extends BaseRichSpout {
        private SpoutOutputCollector collector;
        private Random random;

        @Override
        public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
            this.collector = spoutOutputCollector;
            this.random=new Random(10);
        }

        @Override
        public void nextTuple() {
            Utils.sleep(100);
            String[] sentences = {"this is first hello world","hello xiao ming","how are you","fine thank you ","and you ",
                    "i am fine","thank you","what is you name","my name is han mei mei","what is her name"};
            String sentence = sentences[random.nextInt(sentences.length)];
            logger.info("sentence:[ "+sentence+" ]");
            collector.emit(new Values(sentence));
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
            outputFieldsDeclarer.declare(new Fields("sentence"));
        }
    }

    private static class SplitSentenceBolt extends BaseRichBolt{
        private OutputCollector outputCollector;

        @Override
        public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
            this.outputCollector = outputCollector;
        }

        @Override
        public void execute(Tuple tuple) {
            String sentence = tuple.getStringByField("sentence");
            String[] words = sentence.split(" ");
            for(String word:words){
                outputCollector.emit(new Values(word));
            }
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
            outputFieldsDeclarer.declare(new Fields("word"));
        }
    }

    private static class WordCountBolt extends BaseRichBolt{
        private OutputCollector outputCollector;
        private Map<String,Long> wordCount = new HashMap<>();
        @Override
        public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
            this.outputCollector = outputCollector;
        }

        @Override
        public void execute(Tuple tuple) {
            String word = tuple.getStringByField("word");
            Long count = wordCount.get(word);
            if(count == null){
                count = 0L;
            }
            count++;
            wordCount.put(word,count);
            logger.info("word:"+word+", count:"+count);
            outputCollector.emit(new Values(word,count));
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
            outputFieldsDeclarer.declare(new Fields("word","count"));
        }
    }

    public static void main(String[]args){
        //spout executor
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("SentenceSpout",new SentenceSpout(),1);
        builder.setBolt("SplitSentenceBolt",new SplitSentenceBolt(),1)
                .setNumTasks(1)
                .shuffleGrouping("SentenceSpout");
        builder.setBolt("wordCountBolt",new WordCountBolt(),1)
                .setNumTasks(1)
                .fieldsGrouping("SplitSentenceBolt",new Fields("word"));
        Config config = new Config();
        config.setDebug(true);
        if(args != null && args.length >0){
            try {
                config.setNumWorkers(3);
                StormSubmitter.submitTopology(args[0],config,builder.createTopology());
            } catch (AlreadyAliveException e) {
                e.printStackTrace();
            } catch (InvalidTopologyException e) {
                e.printStackTrace();
            } catch (AuthorizationException e) {
                e.printStackTrace();
            }
        }else{
            config.setMaxTaskParallelism(3);
            LocalCluster localCluster = new LocalCluster();
            localCluster.submitTopology("HelloworldTopology",config,builder.createTopology());
            Utils.sleep(60000);
        }
    }
}