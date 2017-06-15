package sm.hive.udf;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by Sreekanth Mahesala on 7/21/16.
 */

public class ProductList extends UDF {

    public ArrayList<String> evaluate(Text input, String value ) {

        //Check for input value
        if(input == null) return null;

        //Check for delimiters
        if(!(input.toString().contains(";"))) return null;

        String input_str = input.toString().toLowerCase().substring(1,input.getLength());
        value = value.toLowerCase();

        Map<String,String> pm = ProductListMap();

        ArrayList<String> output = CartMetrics(input_str, pm.get(value), value);

        return output;
    }

    public static ArrayList<String> CartMetrics(String input_str, String location, String value){

        String[] productlist = input_str.split(",;");

        Boolean istax_line = (value.equals("t") || value.equals("sc"));
        Boolean cart_add = value.startsWith("a");

        return ParseProductLineItem(productlist,location,cart_add, istax_line);

    }

    private static ArrayList<String> ParseProductLineItem(String[] line_items, String location, Boolean cart_add, Boolean istax_line){
        ArrayList<String> result = new ArrayList<String>();

        Integer line_item_count;

        if (cart_add) {
            line_item_count=line_items.length;
        }else{
            line_item_count=((line_items.length-1)<1)?1:line_items.length-1;
        }

        String delimiter = ";";
        String[] location_parameters = location.split(",");
        Integer i = Integer.parseInt(location_parameters[0]);

        String label = null;

        if (location_parameters.length >1){
            label=location_parameters[1];
        }

        String label_delimiter = (location_parameters.length==3)?location_parameters[2]:null;

        for(int j=0; j< line_item_count; j++) {

            String line_item_value = line_items[j].split(delimiter)[i];

            if (istax_line) {
                line_item_value = line_items[line_items.length-1].split(delimiter)[i];
            }

            if (label != null && label_delimiter != null) {

                String[] line_item_values = line_item_value.split(label_delimiter);
                line_item_value = null;

                for(int k=0; k<line_item_values.length;k++){
                    if (line_item_values[k].contains(label)){
                        line_item_value = line_item_values[k].replace(label,"");
                    }
                }
            }

            result.add(line_item_value);
        }

        return result;
    }


    private static Map<String,String> ProductListMap(){
        Map<String,String> pm = new HashMap<String, String>();

        pm.put("s", "0");
        pm.put("q", "1");
        pm.put("r", "2");
        pm.put("c", "3,212=,\\|");
        pm.put("si", "4,evar75=,\\|");
        pm.put("sm", "4,evar9=,\\|");
        pm.put("t", "3,208=,\\|");
        pm.put("sc", "3,209=,\\|");
        pm.put("ar", "3,234=,\\|");
        pm.put("aq", "3,235=,\\|");
        pm.put("as", "0");

        return pm;
    }

}
