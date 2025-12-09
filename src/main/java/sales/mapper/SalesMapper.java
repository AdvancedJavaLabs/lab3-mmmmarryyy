package sales.mapper;

import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import sales.model.SalesWritable;

import java.io.IOException;


@Slf4j
public class SalesMapper extends Mapper<LongWritable, Text, Text, SalesWritable> {

    private final Text categoryKey = new Text();
    private final SalesWritable salesValue = new SalesWritable();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String line = value.toString();

        if (line.startsWith("transaction_id")) {
            return;
        }

        String[] fields = line.split(",");
        if (fields.length < 5) {
            log.warn("Некорректная строка: {}", line);
            return;
        }

        try {
            String category = fields[2].trim();
            double price = Double.parseDouble(fields[3].trim());
            long quantity = Long.parseLong(fields[4].trim());
            double revenue = price * quantity;

            categoryKey.set(category);
            salesValue.setRevenue(revenue);
            salesValue.setQuantity(quantity);

            context.write(categoryKey, salesValue);

        } catch (NumberFormatException e) {
            log.error("Ошибка парсинга чисел в строке: {}", line, e);
        }
    }
}
//
//package sales.mapper;
//
//import lombok.extern.slf4j.Slf4j;
//import org.apache.hadoop.io.LongWritable;
//import org.apache.hadoop.io.Text;
//import org.apache.hadoop.mapreduce.Mapper;
//import sales.model.SalesWritable;
//
//import java.io.IOException;
//
//@Slf4j
//public class SalesMapper extends Mapper<LongWritable, Text, Text, SalesWritable> {
//    private final Text categoryKey = new Text();
//    private final SalesWritable salesValue = new SalesWritable();
//
//    @Override
//    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
//        String line = value.toString();
//
//        if (line.startsWith("transaction_id")) {
//            return;
//        }
//
//        String[] fields = line.split(",");
//        if (fields.length < 5) {
//            log.warn("Некорректная строка: {}", line);
//            return;
//        }
//
//        try {
//            String category = fields[2].trim();
//            double price = Double.parseDouble(fields[3].trim());
//            long quantity = Long.parseLong(fields[4].trim());
//            double revenue = price * quantity;
//
//            categoryKey.set(category);
//
//            salesValue.setRevenue(revenue);
//            salesValue.setQuantity(quantity);
//            salesValue.setTransactionCount(1);
//            salesValue.setMaxPrice(price);
//            salesValue.setMinPrice(price);
//            salesValue.setSumPrice(price * quantity);
//
//            context.write(categoryKey, salesValue);
//
//        } catch (NumberFormatException e) {
//            log.error("Ошибка парсинга чисел в строке: {}", line, e);
//        }
//    }
//}