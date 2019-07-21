package org.apache.storm.starter.thesis_perf_tests;

import org.apache.storm.starter.thesis_perf_tests.ValueGeneratorBase;

import java.util.List;
import java.util.Random;

public class DoubleGenerator extends ValueGeneratorBase {

    public DoubleGenerator(Random _random, List<Range> _ranges, int _num_values_to_generate) {
        super(_random, _ranges, _num_values_to_generate);
    }

    @Override
    protected Object NextValueInRange(Integer range_index) {
        Double val = super.random.nextDouble();
        return val;
    }
}
