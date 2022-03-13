package ru.edjll.hadoop;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapreduce.Reducer;
import ru.edjll.hadoop.type.map.input.KeyInputMapType;
import ru.edjll.hadoop.type.map.output.ValueOutputMapType;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

public class BackpackReducer extends Reducer<KeyInputMapType, ValueOutputMapType, KeyInputMapType, ValueOutputMapType> {

    private static final KeyInputMapType OUTPUT_KEY = new KeyInputMapType(-1, -1, -1, -1, 0);

    public static final Log LOG = LogFactory.getLog(BackpackReducer.class);

    public void reduce(KeyInputMapType key, Iterable<ValueOutputMapType> values, Context context) throws IOException, InterruptedException {
        Iterator<ValueOutputMapType> iterator = values.iterator();
        if (iterator.hasNext()) {
            ValueOutputMapType max = new ValueOutputMapType(iterator.next());
            double maxCost = calculateCost(max.getCombination(), max.getCosts());

            while (iterator.hasNext()) {
                ValueOutputMapType value = iterator.next();
                double cost = calculateCost(value.getCombination(), value.getCosts());

                LOG.info("===============================================================");
                LOG.info("[MAX] " + Arrays.toString(max.getCombination()) + " [MAX COST] " + maxCost);
                LOG.info("[VALUE] " + Arrays.toString(value.getCombination()) + " [COST] " + cost);
                LOG.info("[COMPARE] " + (cost > maxCost));
                LOG.info("===============================================================");

                if (cost > maxCost) {
                    maxCost = cost;
                    max = new ValueOutputMapType(value);
                }
            }
            context.write(OUTPUT_KEY, max);
        }
    }

    private double calculateCost(int[] combination, List<Double> costs) {
        double cost = 0.0;

        for (int i = 0; i < combination.length; i++) {
            cost += combination[i] * costs.get(i);
        }

        return cost;
    }
}