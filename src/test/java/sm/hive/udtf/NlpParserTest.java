package sm.hive.udtf;

import junit.framework.TestCase;
import org.apache.hadoop.hive.ql.metadata.HiveException;

import java.util.ArrayList;

/**
 * Created by Sreekanth Mahesala on 2/4/17.
 */
public class NlpParserTest extends TestCase {

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

        NlpParser p = new NlpParser();

        Object[] record = {"I need all colors for this machine.  Was wondering if there are any specials before I call another company to get it cheaper. ok I just want to make sure I buiy the right ink!  Thank you for your help :). Nope that's great thanks for your help :). ok no prob. I need to get ink for a brother mfc-j6720dw. Thank you... ok thank you. these would be for the brother  MFC-J6720DW?",tokens};
        try {
            p.process(record);
        } catch (HiveException e) {
            System.out.println(e.getMessage());
        }


    }

}
