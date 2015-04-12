package com.shrcheng.bolt;

import java.util.Map;

import com.shrcheng.source.Global;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

/**
 * 数据拆分Bolt
 * @author ShrCheng
 */
public class SplitSentenceBolt extends BaseRichBolt {

  private static final long serialVersionUID = 7489186951166565608L;

  private OutputCollector outputCollector;

  @SuppressWarnings("rawtypes")
  public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
    this.outputCollector = collector;
  }

  public void execute(Tuple input) {
    String sSentence = input.getStringByField(Global.SENTENCE);//获取tuple发送数据
    String[] sWords = sSentence.split(" ");//按空格拆分数据
    for (String sWord : sWords) {
      this.outputCollector.emit(new Values(sWord));//发送拆分数据
    }
  }

  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    declarer.declare(new Fields(Global.WORD));
  }

}
