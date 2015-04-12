package com.shrcheng.spout;

import java.util.Map;

import com.shrcheng.source.Global;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

/**
 * 该类实现模拟数据发送源
 * @author ShrCheng
 */
public class SentenceSpout extends BaseRichSpout {

  private static final long serialVersionUID = -8020518758333438404L;

  private SpoutOutputCollector spoutOutputCollector;

  private String[] sSentences = {
    "i have a dog",
    "this is books",
    "goto my word",
    "i am working now"};//用于统计的数据源

  private int iIndex = 0;//标示数据发送的索引

  @SuppressWarnings("rawtypes")
  public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
    this.spoutOutputCollector = collector;
  }

  public void nextTuple() {
    spoutOutputCollector.emit(new Values(sSentences[iIndex]));//发射tuple数据
    iIndex++;
    if (iIndex >= sSentences.length){
      iIndex = 0;
    }
  }

  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    declarer.declare(new Fields(Global.SENTENCE));
  }

}
