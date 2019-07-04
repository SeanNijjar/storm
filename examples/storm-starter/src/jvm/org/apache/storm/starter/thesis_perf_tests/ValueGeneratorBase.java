package org.apache.storm.starter.thesis_perf_tests;

import java.util.*;

public abstract class ValueGeneratorBase {

    protected Random random;
    protected List<Range> ranges;
    Integer num_values_to_generate;

    ArrayList<Object> values;
    Iterator<Object> values_iter;

    abstract protected Object NextValueInRange(Integer range_index);

    private void GenerateValuesForRange(Integer range_index, Integer num_values_to_generate) {
        for (int i = 0; i < num_values_to_generate; i++) {
            values.add(NextValueInRange(range_index));
        }
    }


    public ValueGeneratorBase(Random _random, List<Range> _ranges, int _num_values_to_generate) {
        random = _random;
        ranges = _ranges;
        num_values_to_generate = _num_values_to_generate;

        int num_values_per_range = num_values_to_generate / ranges.size();


        for (int i = 0; i < ranges.size(); i++) {
            GenerateValuesForRange(i, num_values_per_range);
        }

        Collections.shuffle(values, random);
        values_iter = values.iterator();
    }


    public Object NextValue() {
        if (values_iter.hasNext()) {
            return values_iter.next();
        }

        System.out.println("Done emitting values for t");
        return null;
    }
}
