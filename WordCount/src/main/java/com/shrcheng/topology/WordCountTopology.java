package com.shrcheng.topology;

import com.shrcheng.bolt.ReportBolt;
import com.shrcheng.bolt.SplitSentenceBolt;
import com.shrcheng.bolt.WordCountBolt;
import com.shrcheng.source.Global;
import com.shrcheng.spout.SentenceSpout;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;

/**
 * WordCount主程序
 * @author ShrCheng
 */
public class WordCountTopology {
  
  public static void main(String[] args) throws Exception {
    TopologyBuilder topologyBuilder = new TopologyBuilder();//实例化topology
    topologyBuilder.setSpout(Global.WORD_SPOUT, new SentenceSpout());//发送数据至SplitSentenceBolt
    topologyBuilder.setBolt(Global.SPLIT_BOLT, new SplitSentenceBolt()).shuffleGrouping(Global.WORD_SPOUT);//发送数据至WordCountBolt
    topologyBuilder.setBolt(Global.WORD_COUNT_BOLT, new WordCountBolt()).fieldsGrouping(Global.SPLIT_BOLT, new Fields(Global.WORD));
    topologyBuilder.setBolt(Global.REPORT_BOLT, new ReportBolt()).globalGrouping(Global.WORD_COUNT_BOLT);
    Config config = new Config();//创建topology配置
    config.setDebug(false);
    LocalCluster localCluster = new LocalCluster();
    localCluster.submitTopology(Global.TOPOLOGY_NAME, config, topologyBuilder.createTopology());
    Thread.sleep(10000);
    localCluster.killTopology(Global.TOPOLOGY_NAME);//杀掉集群topology
    localCluster.shutdown();//关闭集群
  }

}
