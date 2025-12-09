package sales.job;

import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import sales.mapper.SortMapper;
import sales.model.SalesWritable;
import sales.model.SortResultsWritable;
import sales.reducer.SortReducer;


@Slf4j
public class SortJob implements Tool {

    private Configuration configuration;

    @Override
    public int run(String[] args) throws Exception {
        if (args.length < 2) {
            log.error("Usage: SortJob <input> <output> [numReducers]");
            return 1;
        }

        String inputPath = args[0];
        String outputPath = args[1];
        int numberOfReducers = args.length > 2 ? Integer.parseInt(args[2]) : 1;

        log.info("Запуск SortJob: input={}, output={}, reducers={}", inputPath, outputPath, numberOfReducers);

        Job job = Job.getInstance(configuration, "Sort Sales by Revenue");
        job.setJarByClass(SortJob.class);

        job.setMapperClass(SortMapper.class);
        job.setReducerClass(SortReducer.class);

        job.setSortComparatorClass(DoubleDecreasingComparator.class);

        job.setMapOutputKeyClass(DoubleWritable.class);
        job.setMapOutputValueClass(SortResultsWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));

        boolean success = job.waitForCompletion(true);
        return success ? 0 : 1;
    }

    @Override
    public void setConf(Configuration conf) {
        this.configuration = conf;
    }

    @Override
    public Configuration getConf() {
        return configuration;
    }

    static class DoubleDecreasingComparator extends WritableComparator {

        public DoubleDecreasingComparator() {
            super(DoubleWritable.class, true);
        }

        @SuppressWarnings("unchecked")
        @Override
        public int compare(WritableComparable a, WritableComparable b) {
            DoubleWritable d1 = (DoubleWritable) a;
            DoubleWritable d2 = (DoubleWritable) b;

            return d2.compareTo(d1);
        }

        @Override
        public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
            double thisValue = readDouble(b1, s1);
            double thatValue = readDouble(b2, s2);

            return Double.compare(thatValue, thisValue);
        }
    }
}

//package sales.job;
//
//import lombok.extern.slf4j.Slf4j;
//import org.apache.hadoop.conf.Configuration;
//import org.apache.hadoop.fs.Path;
//import org.apache.hadoop.io.*;
//import org.apache.hadoop.mapreduce.Job;
//import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
//import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
//import org.apache.hadoop.mapreduce.lib.partition.InputSampler;
//import org.apache.hadoop.mapreduce.lib.partition.TotalOrderPartitioner;
//import org.apache.hadoop.util.Tool;
//import sales.mapper.SortMapper;
//import sales.reducer.SortReducer;
//
//import java.io.IOException;
//
//@Slf4j
//public class SortJob implements Tool {
//
//    private Configuration configuration;
//
//    @Override
//    public int run(String[] args) throws Exception {
//        if (args.length < 2) {
//            log.error("Usage: SortJob <input> <output> [numReducers]");
//            return 1;
//        }
//
//        String inputPath = args[0];
//        String outputPath = args[1];
//        int numReducers = args.length > 2 ? Integer.parseInt(args[2]) : 1;
//
//        log.info("Запуск SortJob: input={}, output={}, reducers={}", inputPath, outputPath, numReducers);
//
//        Configuration conf = new Configuration(configuration);
//
//        // компаратор для сортировки по убыванию
//        conf.set("mapreduce.sortjob.comparator.class", "sales.job.SortJob$DoubleDecreasingComparator");
//
//        Job job = Job.getInstance(conf, "Sort Sales by Revenue");
//        job.setJarByClass(SortJob.class);
//
//        job.setMapperClass(SortMapper.class);
//        job.setReducerClass(SortReducer.class);
//        job.setNumReduceTasks(numReducers);
//
//        // TotalOrderPartitioner для глобальной сортировки
//        job.setPartitionerClass(TotalOrderPartitioner.class);
//
//        job.setSortComparatorClass(DoubleDecreasingComparator.class);
//
//        job.setMapOutputKeyClass(DoubleWritable.class);
//        job.setMapOutputValueClass(Text.class);
//        job.setOutputKeyClass(Text.class);
//        job.setOutputValueClass(Text.class);
//
//        FileInputFormat.addInputPath(job, new Path(inputPath));
//        FileOutputFormat.setOutputPath(job, new Path(outputPath));
//
//        // это для TotalOrderPartitioner
//        Path partitionFile = new Path(outputPath + "_partitions");
//        TotalOrderPartitioner.setPartitionFile(job.getConfiguration(), partitionFile);
//
//        // сэмплер для определения границ партиций
//        InputSampler.Sampler<DoubleWritable, Text> sampler =
//                new InputSampler.RandomSampler<>(0.1, 1000, 10);
//
//        InputSampler.writePartitionFile(job, sampler);
//
//        boolean success = job.waitForCompletion(true);
//
//        return success ? 0 : 1;
//    }
//
//    @Override
//    public void setConf(Configuration conf) {
//        this.configuration = conf;
//    }
//
//    @Override
//    public Configuration getConf() {
//        return configuration;
//    }
//
//    public static class DoubleDecreasingComparator extends WritableComparator {
//
//        public DoubleDecreasingComparator() {
//            super(DoubleWritable.class, true);
//        }
//
//        @SuppressWarnings("unchecked")
//        @Override
//        public int compare(WritableComparable a, WritableComparable b) {
//            DoubleWritable d1 = (DoubleWritable) a;
//            DoubleWritable d2 = (DoubleWritable) b;
//
//            return Double.compare(d2.get(), d1.get());
//        }
//
//        @Override
//        public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
//            double thisValue = readDouble(b1, s1);
//            double thatValue = readDouble(b2, s2);
//
//            return Double.compare(thatValue, thisValue);
//        }
//    }
//}
