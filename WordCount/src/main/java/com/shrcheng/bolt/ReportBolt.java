package com.shrcheng.bolt;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.shrcheng.source.Global;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;

/**
 * 数据报告Bolt
 * @author ShrCheng
 */
public class ReportBolt extends BaseRichBolt {

  private static final long serialVersionUID = -5716295208451214008L;

  private Map<String, Long> count;//存放数据

  @SuppressWarnings("rawtypes")
  public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
     this.count = new ConcurrentHashMap<String, Long>();
  }

  public void execute(Tuple input) {
    String sWord = input.getStringByField(Global.WORD);
    Long lCount = input.getLongByField(Global.COUNT);
    this.count.put(sWord, lCount);
  }

  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    // 由于数据已经计算完成,顾该Bolt不在发射任何数据
  }

  public void cleanup() {
    /**
     * 注意在开发Bolt时间,谨记当topology在Storm集群上运行时 Bolt的cleanup()方法不一定执行,
     * 顾在集群中不可使用cleanup()方法 
     * 由于我们是在开发模式下使用该方法,顾能保证该方法会被执行
     */
    System.out.println("------------------ count result ------------------");
    List<String> list = new ArrayList<String>();
    list.addAll(this.count.keySet());
    for (String sKey : list) {
      System.out.println(sKey + " : " + this.count.get(sKey));
    }
    System.out.println("---------------------------------------------------");
  }

}
