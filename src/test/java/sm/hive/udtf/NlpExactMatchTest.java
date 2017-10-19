package sm.hive.udtf;

import junit.framework.TestCase;
import org.apache.hadoop.hive.ql.metadata.HiveException;

import java.util.ArrayList;

/**
 * Created by Sreekanth Mahesala on 2/4/17.
 */
public class NlpExactMatchTest extends TestCase {

    public void testEvaluateMethod() {

        ArrayList<String> tokens = new ArrayList<String>();
        tokens.add("last");
        tokens.add("name");
        tokens.add("record");
        tokens.add("wrong");
        tokens.add("make");
        tokens.add("user");
        tokens.add("long");
        tokens.add("correct");
        tokens.add("get");
        tokens.add("right");
        tokens.add("first");
        tokens.add("time");
        tokens.add("thank");

        String phrase = "thank";

        NlpExactMatch em = new NlpExactMatch();

        Object[] record = {tokens,phrase};
        try {
            em.process(record);
        } catch (HiveException e) {
            System.out.println(e.getMessage());
        }


    }

}
