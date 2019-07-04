package org.apache.storm.starter.thesis_perf_tests;

import org.apache.storm.starter.thesis_perf_tests.ValueGeneratorBase;

import java.util.List;
import java.util.Random;

public class ShortGenerator extends ValueGeneratorBase {

    public ShortGenerator(Random _random, List<Range> _ranges, int _num_values_to_generate) {
        super(_random, _ranges, _num_values_to_generate);
    }

    protected Object NextValueInRange(Integer range_index) {
        Range range = super.ranges.get(range_index);
        long range_size = range.high - range.low;
        Short val = (short)((super.random.nextInt() % range_size) + range.low);
        return val;
    }
}


