package countword.spouts;

import java.util.Map;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

public class SignalsSpout implements IRichSpout{

	private SpoutOutputCollector collector;

	@Override
	public boolean isDistributed() {
		return true;
	}

	@Override
	public void ack(Object msgId) {}

	@Override
	public void close() {}

	@Override
	public void fail(Object msgId) {}

	@Override
	public void nextTuple() {
		collector.emit(new Values("refreshCache"));
		try {
			Thread.sleep(1000);
		} catch (InterruptedException e) {}
	}

	@Override
	public void open(Map conf, TopologyContext context,
			SpoutOutputCollector collector) {
		this.collector = collector;
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("action"));
	}

}
