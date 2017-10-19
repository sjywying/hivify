package sm.hive.udf;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

import java.util.Arrays;

/**
 * Created by Sreekanth Mahesala on 7/21/16.
 */

public class ClickstreamRevenueDecoder extends UDF {

    public double evaluate(Text productlist) {
        try{

            if(productlist == null) return 0;
            if(!(productlist.toString().contains(",;"))) return 0;

            String pl = productlist.toString();
            String[] lineItems = pl.substring(1, pl.length()-1).split(",;");
            lineItems = Arrays.copyOf(lineItems,lineItems.length-1);

            double revenue = 0;

            for(String lineItem : lineItems) {
                revenue = revenue + Double.parseDouble(lineItem.split(";")[2]);
            }
            return revenue;

        }
        catch(Exception e){
            System.out.println(e.getMessage());
            return -1;
        }

    }
}
