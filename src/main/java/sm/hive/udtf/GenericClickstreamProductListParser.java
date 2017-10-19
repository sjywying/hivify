package sm.hive.udtf;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;

/**
 * Created by Sreekanth Mahesala on 12/5/16.
 */


@Description(name = "GenericClickstreamProductListParser", value = "_FUNC_(product_list,event_list) - emits (line_item, sku, units, price, product_level_coupon, bopis_units, bopis_price, bopis_oos, shipping_method, store_id, tax, shipping_cost) for each row in the input")
public class GenericClickstreamProductListParser extends GenericUDTF {

    private PrimitiveObjectInspector productListOI = null;
    private PrimitiveObjectInspector eventListOI = null;

    @Override
    public StructObjectInspector initialize(ObjectInspector[] args) throws UDFArgumentException {

        String usage = "GenericClickstreamProductListParser(product_list, event_list) AS line_item, sku, units, price, product_level_coupon, bopis_units, bopis_price, bopis_oos, shipping_method, store_id, tax, shipping_cost";

        if (args.length !=2) {
            throw new UDFArgumentException(usage);
        }

        productListOI = (PrimitiveObjectInspector) args[0];
        eventListOI = (PrimitiveObjectInspector) args[1];

        List<String> fieldNames = new ArrayList<String>(2);
        List<ObjectInspector> fieldOIs = new ArrayList<ObjectInspector>(2);

        fieldNames.add("event_name");
        fieldNames.add("line_item");
        fieldNames.add("sku");
        fieldNames.add("units");
        fieldNames.add("price");
        fieldNames.add("product_level_coupon"); //event=212
        fieldNames.add("bopis_units"); //event=283
        fieldNames.add("bopis_price"); //event=284
        fieldNames.add("bopis_oos"); //event=285
        fieldNames.add("shipping_method"); //event=evar9
        fieldNames.add("store_id"); //event=evar75
        fieldNames.add("tax"); //event=208
        fieldNames.add("shipping_cost"); //event=209
        fieldNames.add("order_level_coupon"); //event=213
        fieldNames.add("coupon_count"); //event=214

        fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector); //event_name
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector); //line_item
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector); //sku
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaFloatObjectInspector); //units
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaFloatObjectInspector); //price
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaFloatObjectInspector); //product_level_coupon
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaFloatObjectInspector); //bopis_units
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaFloatObjectInspector); //bopis_price
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector); //bopis_oos
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector); //shipping_method
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector); //store_id
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaFloatObjectInspector); //tax
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaFloatObjectInspector); //shipping_cost
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaFloatObjectInspector); //order_level_coupon
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaFloatObjectInspector); //coupon_count


        return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, fieldOIs);
    }

    @Override
    public void process(Object[] record) throws HiveException {
        final String productListRow = (String) record[0]; // (String) productListOI.getPrimitiveJavaObject(record[0]);
        final String eventListRow =(String) record[1]; //(String) eventListOI.getPrimitiveJavaObject(record[1]);

        if (productListRow == null) {
            return;
        }

        if (eventListRow == null) {
            return;
        }


        final List<String> events = Arrays.asList(eventListRow.split(","));
        final float defaultFloat = Float.valueOf("0");
        boolean isPurchaseEvent = false;

        final String[] productRows = productListRow.split(",");
        List<Object[]> rows;

        if(events.contains("1") && !events.contains("11")) {
            //Purchase event
            isPurchaseEvent = true;
            rows = parsePurchase(productRows,defaultFloat);
            for(Object[] row: rows) {
                forward(row);
            }
        }

        if(events.contains("2")) {
            //ProductView event
            rows = parseProductView(productRows, defaultFloat);
            for(Object[] row: rows) {
                forward(row);
            }
        }

        if(events.contains("11") && !isPurchaseEvent) {
            //Checkout event
            rows = parseCheckout(productRows,defaultFloat);
            for(Object[] row: rows) {
                forward(row);
            }
        }

        if(events.contains("12")) {
            //Cart add event
            rows = parseCartAdd(productRows,defaultFloat);
            for(Object[] row: rows) {
                forward(row);
            }
        }

        if(events.contains("13")) {
            //Cart remove event
            rows = parseCartRemove(productRows,defaultFloat);
            for(Object[] row: rows) {
                forward(row);
            }
        }


    }

    public List<Object[]> parseProductView(String[] lineItems, Float defaultFloat) throws HiveException {
        List<Object[]> rows = new ArrayList<Object[]>();
        String tokenDelimiter = ";";

        for(String lineItem: lineItems) {
            String[] tokens = lineItem.split(tokenDelimiter);
            if(tokens.length-1 >= 1) {
                rows.add(newRow("product_view"
                        , lineItem
                        , tokens[1]
                        , defaultFloat
                        , defaultFloat
                        , defaultFloat
                        , defaultFloat
                        , defaultFloat
                        ,""
                        ,""
                        ,""
                        ,defaultFloat
                        ,defaultFloat
                        ,defaultFloat
                        ,defaultFloat));
            }
        }

        return rows;
    }

    public List<Object[]> parseCartRemove(String[] lineItems, Float defaultFloat) throws HiveException {
        List<Object[]> rows = new ArrayList<Object[]>();
        String tokenDelimiter = ";";

        for(String lineItem: lineItems) {
            String[] tokens = lineItem.split(tokenDelimiter);
            if(tokens.length-1 >= 1) {
                rows.add(newRow( "cart_remove"
                        , lineItem
                        , tokens[1]
                        , defaultFloat
                        , defaultFloat
                        , defaultFloat
                        , defaultFloat
                        , defaultFloat
                        , ""
                        , ""
                        , ""
                        , defaultFloat
                        , defaultFloat
                        , defaultFloat
                        , defaultFloat));
            }
        }

        return rows;

    }

    public List<Object[]> parseCartAdd(String[] lineItems, Float defaultFloat) throws HiveException {
        List<Object[]> rows = new ArrayList<Object[]>();
        String tokenDelimiter = ";";

        for(String lineItem: lineItems) {
            String[] tokens = lineItem.split(tokenDelimiter);
            Float atcUnits;
            Float atcRevenue;

            try {
                atcUnits = Float.valueOf(getLineDetails(tokens[4],"235",defaultFloat.toString()));
            } catch(Exception e) {
                atcUnits = Float.valueOf(0);
            }
            try {
                atcRevenue = Float.valueOf(getLineDetails(tokens[4],"234",defaultFloat.toString()));
            } catch (Exception e) {
                atcRevenue = Float.valueOf(0);
            }

            if(tokens.length-1 >= 4) {
                rows.add(newRow( "cart_add"
                        , lineItem
                        , tokens[1]
                        , atcUnits
                        , atcRevenue
                        , defaultFloat
                        , defaultFloat
                        , defaultFloat
                        , ""
                        , ""
                        , ""
                        , defaultFloat
                        , defaultFloat
                        , defaultFloat
                        , defaultFloat));
            }
        }

        return rows;

    }

    public List<Object[]> parseCheckout(String[] lineItems, Float defaultFloat) throws HiveException {

        List<Object[]> rows = new ArrayList<Object[]>();
        String tokenDelimiter = ";";

        for(String lineItem: lineItems) {
            String[] tokens = lineItem.split(tokenDelimiter);
            String units = (tokens.length-1>=2) ? tokens[2] : "0";
            String price = (tokens.length-1>=3) ? tokens[3] : "0";
            if(tokens.length-1 >= 1) {
                rows.add(newRow( "checkout"
                        , lineItem
                        , tokens[1]
                        , Float.valueOf(units)
                        , Float.valueOf(price)
                        , defaultFloat
                        , defaultFloat
                        , defaultFloat
                        , ""
                        , ""
                        , ""
                        , defaultFloat
                        , defaultFloat
                        , defaultFloat
                        , defaultFloat));
            }
        }

        return rows;
    }

    public List<Object[]> parsePurchase(String[] lineItems, Float defaultFloat) throws HiveException {

        List<Object[]> rows = new ArrayList<Object[]>();
        String tokenDelimiter = ";";
        Float tax =  Float.valueOf(0);
        Float shippingCost = Float.valueOf(0);
        Float orderLevelCoupon = Float.valueOf(0);
        Float couponCount = Float.valueOf(0);

        for(String token: lineItems[lineItems.length-1].split(tokenDelimiter)) {
              if(token.contains("208")) {
                  tax = Float.valueOf(getLineDetails(token,"208",defaultFloat.toString()));
              }
              if(token.contains("209")) {
                  shippingCost = Float.valueOf(getLineDetails(token,"209",defaultFloat.toString()));
              }
              if(token.contains("213")) {
                  orderLevelCoupon = Float.valueOf(getLineDetails(token,"213",defaultFloat.toString()));
              }
            if(token.contains("214")) {
                couponCount = Float.valueOf(getLineDetails(token,"214",defaultFloat.toString()));
            }
        }

        for(String lineItem: Arrays.copyOf(lineItems, lineItems.length-1)) {
            String[] tokens = lineItem.split(tokenDelimiter);
            if(tokens.length-1>=5) {
                rows.add(newRow("purchase"
                        , lineItem
                        , tokens[1]
                        , Float.valueOf(tokens[2])
                        , Float.valueOf(tokens[3])
                        , Float.valueOf(getLineDetails(tokens[4],"212",defaultFloat.toString()))
                        , Float.valueOf(getLineDetails(tokens[4],"283",defaultFloat.toString()))
                        , Float.valueOf(getLineDetails(tokens[4],"284",defaultFloat.toString()))
                        , getLineDetails(tokens[4],"285","")
                        , getLineDetails(tokens[5],"eVar9","")
                        , getLineDetails(tokens[5],"eVar75","")
                        , tax
                        , shippingCost
                        , orderLevelCoupon
                        , couponCount));
            }
        }

        return rows;
    }


    private Object[] newRow(String eventName, String lineItem,String sku, Float units, Float price, Float productLevelCoupon, Float bopisUnits, Float bopisPrice, String bopisOos, String shippingMethod, String store_id, Float tax, Float shippingCost, Float orderLevelCoupon, Float couponCount) {

        return new Object[] {eventName
                , lineItem
                , sku
                , units
                , price
                , productLevelCoupon
                , bopisUnits
                , bopisPrice
                , bopisOos
                , shippingMethod
                , store_id
                , tax
                , shippingCost
                , orderLevelCoupon
                , couponCount
        };
    }

    private String getLineDetails(String tokenString, String event, String nullString) {

        String result = nullString;
        String tokenDelimiter = "|";
        String eventString = event+"=";


        if(tokenString.contains(tokenDelimiter)) {
            for(String token: tokenString.split(Pattern.quote(tokenDelimiter))) {
                if(token.contains(eventString)) {
                   return token.replace(eventString,"");
                }
            }
        } else {
            if(tokenString.contains(eventString)) {
                return tokenString.replace(eventString,"");
            }
        }

        return result;
    }

    @Override
    public void close() throws HiveException {
        // do nothing
    }
}
