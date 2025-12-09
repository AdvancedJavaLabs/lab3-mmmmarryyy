package sales.reducer;

import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import sales.model.SalesWritable;

import java.io.IOException;


@Slf4j
public class SalesReducer extends Reducer<Text, SalesWritable, Text, SalesWritable> {

    private final SalesWritable result = new SalesWritable();

    @Override
    protected void reduce(Text key, Iterable<SalesWritable> values, Context context) throws IOException, InterruptedException {

        double totalRevenue = 0.0;
        long totalQuantity = 0L;

        for (SalesWritable value : values) {
            totalRevenue += value.getRevenue();
            totalQuantity += value.getQuantity();
        }

        result.setRevenue(totalRevenue);
        result.setQuantity(totalQuantity);

        context.write(key, result);

        log.debug("Обработана категория: {}, выручка: {}, количество: {}", key.toString(), totalRevenue, totalQuantity);
    }
}

//package sales.reducer;
//
//import lombok.extern.slf4j.Slf4j;
//import org.apache.hadoop.conf.Configuration;
//import org.apache.hadoop.io.Text;
//import org.apache.hadoop.mapreduce.Reducer;
//import sales.model.MetricType;
//import sales.model.SalesWritable;
//
//import java.io.IOException;
//
//@Slf4j
//public class SalesReducer extends Reducer<Text, SalesWritable, Text, SalesWritable> {
////    private MetricType metricType = MetricType.QUANTITY;
//
//    @Override
//    protected void setup(Context context) {
//        Configuration conf = context.getConfiguration();
////        String metricCode = conf.get("metric.type", "quantity");
////        metricType = MetricType.fromCode(metricCode);
////        log.info("Using metric type: {}", metricType.getDisplayName());
//    }
//
//    @Override
//    protected void reduce(Text key, Iterable<SalesWritable> values, Context context) throws IOException, InterruptedException {
//        SalesWritable result = new SalesWritable();
//
//        for (SalesWritable value : values) {
//            result.merge(value);
//        }
//
//        context.write(key, result);
//    }
//}