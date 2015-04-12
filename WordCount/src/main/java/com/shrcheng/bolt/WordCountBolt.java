package com.shrcheng.bolt;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.shrcheng.source.Global;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

/**
 * 数据统计Bolt
 * @author ShrCheng
 */
public class WordCountBolt extends BaseRichBolt {

  private static final long serialVersionUID = -8987054269565321272L;
  
  private OutputCollector outputCollector;
  
  private Map<String, Long> count;//标记数据总数

  @SuppressWarnings("rawtypes")
  public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
    this.outputCollector = collector;
    this.count = new ConcurrentHashMap<String, Long>();
  }

  public void execute(Tuple input) {
    String sWord = input.getStringByField(Global.WORD);
    Long lCount = this.count.get(sWord);//从已有的数据中获取该数据的总数
    if (lCount == null) {
      lCount = 0L;
    }
    lCount++;
    count.put(sWord, lCount);
    this.outputCollector.emit(new Values(sWord, lCount));
  }

  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    declarer.declare(new Fields(Global.WORD, Global.COUNT));
  }

}
