package ru.edjll.hadoop;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapreduce.Mapper;
import ru.edjll.hadoop.type.map.input.KeyInputMapType;
import ru.edjll.hadoop.type.map.input.ValueInputMapType;
import ru.edjll.hadoop.type.map.output.ValueOutputMapType;

import java.io.IOException;
import java.util.List;

public class BackpackMapper extends Mapper<KeyInputMapType, ValueInputMapType, KeyInputMapType, ValueOutputMapType> {

    public static final Log LOG = LogFactory.getLog(BackpackMapper.class);

    public void map(KeyInputMapType key, ValueInputMapType value, Context context) throws IOException, InterruptedException {
        double weight = key.getFirstItemCount() * value.getWeights().get(key.getFirstItemIndex()) + key.getSecondItemCount() * value.getWeights().get(key.getSecondItemIndex());
        int[] combination = new int[value.getCounts().size()];

        combination[key.getFirstItemIndex()] = key.getFirstItemCount();
        combination[key.getSecondItemIndex()] = key.getSecondItemCount();

        LOG.info("[MAPPER] key = " + key);

        if (weight == value.getLimit()) {
            context.write(key, new ValueOutputMapType(value, combination));
        } else if (weight < value.getLimit()) {
            context.write(key, new ValueOutputMapType(value, combination));

            for (int i = 0, mainIndex = 0; i < combination.length; ) {
                if (combination[i] == value.getCounts().get(i) || i == key.getFirstItemIndex() || i == key.getSecondItemIndex()) {
                    if (i != key.getFirstItemIndex() && i != key.getSecondItemIndex()) {
                        combination[i] = 0;
                    }
                    i++;
                    if (i > mainIndex) {
                        mainIndex = i;
                        cleanArray(combination, key);
                    }
                } else {
                    combination[i]++;
                    i = 0;
                    if (validWeight(combination, value.getWeights(), value.getLimit()))
                        context.write(key, new ValueOutputMapType(value, combination));
                }
            }
        }
    }

    private void cleanArray(int[] combination, KeyInputMapType key) {
        for (int i = 0; i < combination.length; i++) {
            if (i != key.getFirstItemIndex() && i != key.getSecondItemIndex())
                combination[i] = 0;
        }
    }

    private boolean validWeight(int[] combination, List<Double> weights, double limit) {
        double weight = 0.0;

        for (int i = 0; i < combination.length; i++) {
            weight += combination[i] * weights.get(i);
            if (weight > limit) return false;
        }

        return true;
    }
}