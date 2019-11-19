package aim3.hw1;

/*
 * The base of the code has been taken from  my team's implementation of
 * the Map Reduce Lab Exercise by Sergi Nadal
 * in the MSc Big Data Management course in UPC Barcelona
 * The team members were me (Ioannis Prapas) and Ankush Sharma
 */

import java.util.HashMap;
import java.util.Map;

public class Utils {

    private static final Map<String, Integer> customerAttributes = new HashMap<String, Integer>() {{
        put("custkey", 0) ;
        put("name", 1);
        put("address", 2);
        put("nationkey", 3);
        put("phone", 4);
        put("acctbal", 5);
        put("mktsegment", 6);
        put("comment", 7);
    }};

    private static final Map<String, Integer> orderAttributes = new HashMap<String, Integer>() {{
        put("orderkey", 0);
        put("custkey", 1);
        put("orderstatus", 2);
        put("price", 3);
        put("orderdate", 4);
        put("orderpriority", 5);
        put("clerk", 6);
        put("shippriority", 7);
        put("comment", 8);
    }};

    public static String getCustomerAttribute(String[] row, String attribute) {
        return row[customerAttributes.get(attribute)];
    }

    public static String getOrderAttribute(String[] row, String attribute) {
        if (orderAttributes.containsKey(attribute)) {
            return row[orderAttributes.get(attribute)];
        }
        return null;
    }

}
