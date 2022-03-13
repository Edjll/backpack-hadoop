package ru.edjll.hadoop.format.input;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.*;
import ru.edjll.hadoop.format.input.reader.BackpackRecordReader;
import ru.edjll.hadoop.type.map.input.KeyInputMapType;
import ru.edjll.hadoop.type.map.input.ValueInputMapType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class BackpackInputFormat extends InputFormat<KeyInputMapType, ValueInputMapType> {

    private static final String KEY_FILE_PATH = "backpack.input.path";
    private static final String KEY_LIMIT = "backpack.limit";

    public static final Log LOG = LogFactory.getLog(BackpackInputFormat.class);

    public static void setLimit(Job job, double limit) {
        job.getConfiguration().set(KEY_LIMIT, String.valueOf(limit));
    }

    public static double getLimit(Configuration configuration) {
        String limit = configuration.get(KEY_LIMIT, "0");
        return Double.parseDouble(limit);
    }

    public static void setInputFilePath(Job job, Path inputPath) throws IOException {
        Path path = new Path(job.getWorkingDirectory(), inputPath);
        job.getConfiguration().set(KEY_FILE_PATH, path.toString());
    }

    public static Path getInputFilePath(Configuration configuration) {
        String filePath = configuration.get(KEY_FILE_PATH);
        return new Path(filePath);
    }

    @Override
    public List<InputSplit> getSplits(JobContext jobContext) throws IOException {
        Path inputFilePath = getInputFilePath(jobContext.getConfiguration());
        FileSystem fileSystem = inputFilePath.getFileSystem(jobContext.getConfiguration());
        FSDataInputStream inputStream = fileSystem.open(inputFilePath);
        List<InputSplit> inputSplits = new ArrayList<>();

        List<Integer> counts = new ArrayList<>();

        double limit = getLimit(jobContext.getConfiguration());

        LOG.info("[LIMIT] = " + limit);

        LOG.info("[START READING]");
        while (true) {
            String line = inputStream.readLine();
            if (line == null) break;
            LOG.info("[LINE] = " + line);
            String[] params = line.split("\t");
            counts.add(Integer.parseInt(params[3]));
        }

        List<Integer> fixedItems = IntStream
                .range(0, counts.size())
                .boxed()
                .sorted((a, b) -> counts.get(b) - counts.get(a))
                .limit(2)
                .collect(Collectors.toList());

        for (int firstItemCount = 0; firstItemCount <= counts.get(fixedItems.get(0)); firstItemCount++) {
            for (int secondItemCount = 0; secondItemCount <= counts.get(fixedItems.get(1)); secondItemCount++) {
                LOG.info("[FIRST ITEM] " + fixedItems.get(0) + " [FIRST ITEM COUNT] " + firstItemCount + " [SECOND ITEM] " + fixedItems.get(1) + " [SECOND ITEM COUNT] " + secondItemCount);
                inputSplits.add(new BackpackInputSplit(fixedItems.get(0), firstItemCount, fixedItems.get(1), secondItemCount, inputFilePath, limit));
            }
        }

        inputStream.close();

        return inputSplits;
    }

    @Override
    public RecordReader<KeyInputMapType, ValueInputMapType> createRecordReader(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        return new BackpackRecordReader(taskAttemptContext, (BackpackInputSplit) inputSplit);
    }
}
