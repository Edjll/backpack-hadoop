package ru.edjll.hadoop.format.input.reader;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import ru.edjll.hadoop.format.input.BackpackInputSplit;
import ru.edjll.hadoop.type.map.input.KeyInputMapType;
import ru.edjll.hadoop.type.map.input.ValueInputMapType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class BackpackRecordReader extends RecordReader<KeyInputMapType, ValueInputMapType> {

    private KeyInputMapType key;
    private ValueInputMapType value;

    private boolean read = false;

    public BackpackRecordReader(TaskAttemptContext taskAttemptContext, BackpackInputSplit inputSplit) throws IOException, InterruptedException {
        initialize(inputSplit, taskAttemptContext);
    }

    public void close() {
    }

    @Override
    public void initialize(InputSplit inputSplitD, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        BackpackInputSplit inputSplit = (BackpackInputSplit) inputSplitD;

        key = new KeyInputMapType();
        key.setFirstItemIndex(inputSplit.getFirstItemIndex());
        key.setFirstItemCount(inputSplit.getFirstItemCount());
        key.setSecondItemIndex(inputSplit.getSecondItemIndex());
        key.setSecondItemCount(inputSplit.getSecondItemCount());

        FileSystem fileSystem = inputSplit.getPath().getFileSystem(taskAttemptContext.getConfiguration());
        FSDataInputStream inputStream = fileSystem.open(inputSplit.getPath());

        List<Text> items = new ArrayList<>();
        List<Integer> counts = new ArrayList<>();
        List<Double> weights = new ArrayList<>();
        List<Double> costs = new ArrayList<>();

        while (true) {
            String line = inputStream.readLine();
            if (line == null) {
                break;
            }
            String[] params = line.split("\t");
            items.add(new Text(params[0]));
            counts.add(Integer.parseInt(params[3]));
            weights.add(Double.parseDouble(params[1]));
            costs.add(Double.parseDouble(params[2]));
        }

        value = new ValueInputMapType();
        value.setItems(items);
        value.setCounts(counts);
        value.setWeights(weights);
        value.setCosts(costs);
        value.setLimit(inputSplit.getLimit());

        inputStream.close();
    }

    @Override
    public boolean nextKeyValue() {
        if (read) return false;

        read = true;
        return true;
    }

    @Override
    public KeyInputMapType getCurrentKey() {
        return key;
    }

    @Override
    public ValueInputMapType getCurrentValue() {
        return value;
    }

    public float getProgress() {
        return 0;
    }
}
