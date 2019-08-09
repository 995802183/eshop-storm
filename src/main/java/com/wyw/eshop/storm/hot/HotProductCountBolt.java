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

    private class HotProductFindThread implements Runnable{

        @Override
        public void run() {
            List<Map.Entry<Long,Long>> productCountList = new ArrayList<>();
            List<Long> hotProductIdList = new ArrayList<>();
            while (true){
                // 1、将LRUMap中的数据按照访问次数，进行全局的排序
                // 2、计算95%的商品的访问次数的平均值
                // 3、遍历排序后的商品访问次数，从最大的开始
                // 4、如果某个商品比如它的访问量是平均值的10倍，就认为是缓存的热点
                try {
                    productCountList.clear();
                    hotProductIdList.clear();
                    if(productCountMap.size() == 0){
                        Utils.sleep(100);
                        continue;
                    }

                    //1先做全局的排序
                    for(Map.Entry<Long,Long> productCountEntry: productCountMap.entrySet()) {
                        if(productCountList.size() == 0){
                            productCountList.add(productCountEntry);
                        }else{
                            Boolean bigger = false;
                            for(Integer i = 0;i<productCountList.size();i++){
                                Map.Entry<Long, Long> topnProductCountEntry = productCountList.get(i);
                                if(productCountEntry.getValue() > topnProductCountEntry.getValue()){
                                    int lastIndex = productCountList.size() < productCountMap.size()? productCountList.size()-1:productCountMap.size()-2;
                                    for(int j=lastIndex;j>-i;j--){
                                        if(j+1 == productCountList.size()){
                                            productCountList.add(null);
                                        }
                                        productCountList.set(i,productCountEntry);
                                        bigger = true;
                                        break;
                                    }
                                }
                            }
                            if(!bigger){
                                if(productCountList.size() < productCountMap.size()){
                                    productCountList.add(productCountEntry);
                                }
                            }
                        }
                    }

                    // 2、计算出95%的商品的访问次数的平均值
                    int calculateCount = (int)Math.floor(productCountList.size() * 0.95);
                    Long totalCount = 0L;
                    for(int i = productCountList.size()-1;i>=productCountList.size()-calculateCount;i--){
                        totalCount += productCountList.get(i).getValue();
                    }
                    Long avgCount = totalCount / calculateCount;
                    // 3、从第一个元素开始遍历，判断是否是平均值得10倍
                    for(Map.Entry<Long,Long> productCountEntry: productCountList){
                        if(productCountEntry.getValue() > 10 * avgCount){
                            hotProductIdList.add(productCountEntry.getKey());


                            //将缓存热点反向推送到流量分发的nginx中

                            //将缓存热点，那个商品对应的完整的缓存数据，发送请求到缓存服务去获取，反向推送到所有的后端应用nginx服务器上去
                        }
                    }
                    Utils.sleep(5000);
                }catch (Exception e){
                    e.printStackTrace();
                }
            }
        }
    }
}
