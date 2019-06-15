package com.wyw.eshop.storm.hot;

import com.alibaba.fastjson.JSONArray;
import com.wyw.eshop.storm.zk.ZookeeperSession;
import org.apache.commons.lang.StringUtils;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.trident.util.LRUMap;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.utils.Utils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class HotProductCountBolt extends BaseRichBolt {
    private OutputCollector outputCollector;
    private LRUMap<Long,Long> productCountMap = new LRUMap<>(1000);
    private ZookeeperSession zkSession;
    private Integer taskid;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.outputCollector = outputCollector;
        this.zkSession = ZookeeperSession.getInstance();
        this.taskid = topologyContext.getThisTaskId();
        new Thread(new ProductCountThread()).start();
        //
        initTaskid(taskid);
    }

    private void initTaskid(int taskid){
        zkSession.acquireDistributeLock();
        String path = "/taskid-list";
        zkSession.createNode(path);
        String taskidList = zkSession.getNodeData(path);
        if(StringUtils.isNotBlank(taskidList)){
            taskidList += ","+taskid;
        }else{
            taskidList += taskid;
        }
        zkSession.setNodeData(path,taskidList);

        zkSession.releaseDistributedLock();
    }

    @Override
    public void execute(Tuple tuple) {
        Long productId = tuple.getLongByField("productId");

        Long count = productCountMap.get(productId);
        if(count == null){
            count = 0L;
        }
        count++;
        productCountMap.put(productId,count);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }

    private class ProductCountThread implements Runnable{

        @Override
        public void run() {
            List<Map.Entry<Long,Long>> topnProductList = new ArrayList<>();
            List<Long> productidList = new ArrayList<>();
            while (true){
                try{
                    topnProductList.clear();
                    productidList.clear();
                    int topn = 3;

                    if(productCountMap.size() == 0){
                        Utils.sleep(100);
                        continue;
                    }

                    for(Map.Entry<Long, Long> productCountEntry : productCountMap.entrySet()){

                        if(topnProductList.size() == 0){
                            topnProductList.add(productCountEntry);
                        }else{
                            Boolean bigger = false;
                            for(Integer i = 0; i<topnProductList.size();i++){
                                Map.Entry<Long, Long> topnProductEntry = topnProductList.get(i);
                                if(productCountEntry.getValue() > topnProductEntry.getValue()){
                                    Integer lastIndex = topnProductList.size() < topn? topnProductList.size()-1:topn-2;
                                    for(int j = lastIndex; j >= i; j--){
                                        if(j+1 == topnProductList.size()){
                                            topnProductList.add(null);
                                        }
                                        topnProductList.set(j+1,topnProductList.get(j));
                                    }
                                    topnProductList.set(i,productCountEntry);
                                    bigger = true;
                                    break;
                                }
                            }
                            if(!bigger){
                                if(topnProductList.size() < topn){
                                    topnProductList.add(productCountEntry);
                                }
                            }
                        }
                    }

                    for(Map.Entry<Long,Long> entry : topnProductList){
                        productidList.add(entry.getKey());
                    }

                    String topnProductListJson = JSONArray.toJSONString(productidList);
                    zkSession.createNode("/task-hot-product-list-"+taskid);
                    zkSession.setNodeData("/task-hot-product-list-"+taskid,topnProductListJson);

                    Utils.sleep(5000);
                }catch (Exception e){
                    e.printStackTrace();
                }
            }
        }
    }
}
