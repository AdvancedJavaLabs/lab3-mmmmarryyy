package sales.mapper;

import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import sales.model.SortResultsWritable;

import java.io.IOException;


@Slf4j
public class SortMapper extends Mapper<LongWritable, Text, DoubleWritable, SortResultsWritable> {

    private final DoubleWritable revenueKey = new DoubleWritable();
    private final SortResultsWritable resultValue = new SortResultsWritable();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] parts = line.split("\t");

        if (parts.length >= 3) {
            try {
                String category = parts[0];
                double revenue = Double.parseDouble(parts[1]);
                long quantity = Long.parseLong(parts[2]);

                revenueKey.set(revenue);
                resultValue.setCategory(category);
                resultValue.setRevenue(revenue);
                resultValue.setQuantity(quantity);

                context.write(revenueKey, resultValue);

            } catch (NumberFormatException e) {
                log.error("Ошибка парсинга выручки: {}", line, e);
            }
        }
    }
}