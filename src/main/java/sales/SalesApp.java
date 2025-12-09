//package sales;
//
//import lombok.extern.slf4j.Slf4j;
//import org.apache.hadoop.conf.Configuration;
//import org.apache.hadoop.util.ToolRunner;
//import sales.job.SalesJob;
//import sales.job.SortJob;
//
//import java.util.Arrays;
//
//@Slf4j
//public class SalesApp {
//
//    public static void main(String[] args) throws Exception {
//        log.info("Got arguments: {}", Arrays.toString(args));
//
//        if (args.length < 2) {
//            System.err.println("Usage: SalesApp <input> <output> <reducers> [blockSizeKB]");
//            System.err.println("  input          - input directory in HDFS");
//            System.err.println("  output         - output directory in HDFS");
//            System.err.println("  reducers       - number of reducers");
//            System.err.println("  blockSizeKB    - block size in KB (optional)");
//            System.exit(1);
//        }
//
//        String inputDirectory = args[0];
//        String outputDirectory = args[1];
//
//        int numberOfReducers = 2;
//        int blockSizeKB = 128;
//
//        if (args.length >= 3) numberOfReducers = Integer.parseInt(args[2]);
//        if (args.length >= 4) blockSizeKB = Integer.parseInt(args[3]);
//
//        Configuration configuration = new Configuration();
//
//        configuration.set("mapreduce.input.fileinputformat.split.maxsize", String.valueOf(blockSizeKB * 1024L));
//
//        String tempDirectory = outputDirectory + "-temp";
//
//        long startTime = System.currentTimeMillis();
//
//        log.info("Phase 1: Sales Analysis");
//        String[] salesArguments = {inputDirectory, tempDirectory, String.valueOf(numberOfReducers)};
//        int salesResult = ToolRunner.run(configuration, new SalesJob(), salesArguments);
//
//        if (salesResult != 0) {
//            log.error("Phase 1 completed with error; failed to analyze sales");
//            System.exit(salesResult);
//        }
//
//        log.info("Phase 2: Revenue Sorting");
//        String[] sortArguments = {tempDirectory, outputDirectory, String.valueOf(numberOfReducers)};
//        int sortResult = ToolRunner.run(configuration, new SortJob(), sortArguments);
//
//        if (sortResult != 0) {
//            log.error("Phase 2 completed with error; failed to sort by revenue");
//            System.exit(sortResult);
//        }
//
//        long endTime = System.currentTimeMillis();
//        long duration = endTime - startTime;
//
//        log.info("Execution completed");
//        log.info("Total time: {} ms ({} sec)", duration, duration / 1000.0);
//    }
//}

package sales;

import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;
import sales.job.SalesJob;
import sales.job.SortJob;

import java.util.Arrays;

@Slf4j
public class SalesApp {
    public static void main(String[] args) throws Exception {
        log.info("Got arguments: {}", Arrays.toString(args));

        if (args.length < 2) {
            System.err.println("Usage: SalesApp <input> <output> <reducers> [blockSizeKB]");
            System.err.println("  input          - input directory in HDFS");
            System.err.println("  output         - output directory in HDFS");
            System.err.println("  reducers       - number of reducers (optional)");
            System.err.println("  blockSizeKB    - block size in KB (optional)");
            System.exit(1);
        }

        String inputDirectory = args[0];
        String outputDirectory = args[1];

        int numberOfReducers = 2;
        int blockSizeKB = 128;

        if (args.length >= 3) numberOfReducers = Integer.parseInt(args[2]);
        if (args.length >= 4) blockSizeKB = Integer.parseInt(args[3]);

        Configuration configuration = new Configuration();
        configuration.set("mapreduce.input.fileinputformat.split.maxsize", String.valueOf(blockSizeKB * 1024L));

        String tempDirectory = outputDirectory + "-temp";

        long startTime = System.currentTimeMillis();

        log.info("Phase 1: Sales Analysis");
        String[] salesArguments = {inputDirectory, tempDirectory, String.valueOf(numberOfReducers)};
        int salesResult = ToolRunner.run(configuration, new SalesJob(), salesArguments);

        if (salesResult != 0) {
            log.error("Phase 1 completed with error");
            System.exit(salesResult);
        }

        log.info("Phase 2: Revenue Sorting");
        String[] sortArguments = {tempDirectory, outputDirectory, String.valueOf(numberOfReducers)};
        int sortResult = ToolRunner.run(configuration, new SortJob(), sortArguments);

        if (sortResult != 0) {
            log.error("Phase 2 completed with error");
            System.exit(sortResult);
        }

        long endTime = System.currentTimeMillis();
        long duration = endTime - startTime;

        log.info("Execution completed");
        log.info("Total time: {} ms ({} sec)", duration, duration / 1000.0);
    }
}