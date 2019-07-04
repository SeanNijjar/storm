
package org.apache.storm.starter.thesis_perf_tests;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.ClusterSummary;
import org.apache.storm.generated.ExecutorSummary;
import org.apache.storm.generated.KillOptions;
import org.apache.storm.generated.Nimbus;
import org.apache.storm.generated.SpoutStats;
import org.apache.storm.generated.TopologyInfo;
import org.apache.storm.generated.TopologySummary;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.NimbusClient;
import org.apache.storm.utils.Utils;



public class SyntheticBenchmarkSpout extends BaseRichSpout {

    SpoutOutputCollector _collector;
    Random random;
    int fields_per_tuple;
    List<ValueGeneratorBase> field_value_generators;



    public SyntheticBenchmarkSpout(List<ValueGeneratorBase> value_generators, Random _rand) {
        super();
        this.random = _rand;
        int _fields_per_tuple = value_generators.size();
        this.fields_per_tuple = _fields_per_tuple;

        field_value_generators = value_generators;
    }

    @Override
    public void open(Map<String, Object> conf, TopologyContext context, SpoutOutputCollector collector) {
        _collector = collector;
    }

    @Override
    public void nextTuple() {

        Values tuple = new Values();
        for (int i = 0; i < fields_per_tuple; i++) {
            ValueGeneratorBase value_generator = field_value_generators.get(i);
            tuple.add(value_generator.NextValue());
        }

        _collector.emit(tuple);
    }

    @Override
    public void ack(Object id) {
        //Ignored
    }

    @Override
    public void fail(Object id) {
        _collector.emit(new Values(id), id);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("sentence"));
    }
}