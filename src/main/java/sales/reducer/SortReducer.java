package sales.reducer;

import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import sales.model.SortResultsWritable;

import java.io.IOException;


@Slf4j
public class SortReducer extends Reducer<DoubleWritable, SortResultsWritable, Text, Text> {

    @Override
    protected void reduce(DoubleWritable key, Iterable<SortResultsWritable> values, Context context) throws IOException, InterruptedException {

        for (SortResultsWritable value : values) {
            double revenue = key.get();
            String outputValue = String.format("%.2f\t%d", revenue, value.getQuantity());
            context.write(new Text(value.getCategory()), new Text(outputValue));
        }
    }
}

//package sales.reducer;
//
//import lombok.extern.slf4j.Slf4j;
//import org.apache.hadoop.conf.Configuration;
//import org.apache.hadoop.io.DoubleWritable;
//import org.apache.hadoop.io.Text;
//import org.apache.hadoop.mapreduce.Reducer;
//import sales.formatter.OutputFormatter;
//import sales.model.MetricType;
//import sales.model.SalesWritable;
//
//import java.io.IOException;
//
//@Slf4j
//public class SortReducer extends Reducer<DoubleWritable, SalesWritable, Text, Text> {
//    private MetricType metricType = MetricType.QUANTITY;
//
//    @Override
//    protected void setup(Context context) {
//        Configuration conf = context.getConfiguration();
//        String metricCode = conf.get("metric.type", "quantity");
//        metricType = MetricType.fromCode(metricCode);
//    }
//
//    @Override
//    protected void reduce(DoubleWritable key, Iterable<SalesWritable> values, Context context) throws IOException, InterruptedException {
//
//        for (SalesWritable value : values) {
//            String outputValue = OutputFormatter.format(metricType, value);
//            log.info("outputValue = {}", outputValue);
//            context.write(new Text(value.getCategory()), new Text(outputValue));
//        }
//    }
//}