package sm.hive.udtf;

import edu.stanford.nlp.ling.CoreAnnotations;
import edu.stanford.nlp.ling.CoreLabel;
import edu.stanford.nlp.pipeline.Annotation;
import edu.stanford.nlp.pipeline.StanfordCoreNLP;
import edu.stanford.nlp.util.CoreMap;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.*;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.Text;

import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * Created by Sreekanth Mahesala on 12/5/16.
 */


@Description(name = "NlpExactMatch", value = "_FUNC_(ArrayList<String> tokens, ) - emits (em_lemmas[], em_tokens[], em_token_count, em_distinct_token_count) for each row in the input")
public class NlpExactMatch extends GenericUDTF {

    private PrimitiveObjectInspector phrase = null;
    private ListObjectInspector tokens = null;
    private ListObjectInspector words = null;
    private StanfordCoreNLP pipeline = null;

    @Override
    public StructObjectInspector initialize(ObjectInspector[] args) throws UDFArgumentException {

        String usage = "NlpExactMatch(tokens[], words[], phrase) AS em_phrase, em_words, em_lemmas, em_word_count, em_distinct_word_count";

        if (args.length !=3) {
            throw new UDFArgumentException(usage);
        }

        //Input
        tokens = (ListObjectInspector) args[0];
        words = (ListObjectInspector) args[1];
        phrase = (PrimitiveObjectInspector) args[2];

        //Output
        List<String> fieldNames = new ArrayList<String>();
        List<ObjectInspector> fieldOIs = new ArrayList<ObjectInspector>();

        fieldNames.add("em_phrase");
        fieldNames.add("em_words");
        fieldNames.add("em_lemmas");
        fieldNames.add("em_word_count");
        fieldNames.add("em_distinct_word_count");

        fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector); //phrase
        fieldOIs.add(ObjectInspectorFactory.getStandardListObjectInspector(PrimitiveObjectInspectorFactory.javaStringObjectInspector)); //words
        fieldOIs.add(ObjectInspectorFactory.getStandardListObjectInspector(PrimitiveObjectInspectorFactory.javaStringObjectInspector)); //lemmas
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaIntObjectInspector); //token_count
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaIntObjectInspector); //distinct_token_count

        return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, fieldOIs);
    }

    @Override
    public void process(Object[] record) throws HiveException {
        final ArrayList<Text> inputTokens = (ArrayList<Text>) this.tokens.getList(record[0]);
        final ArrayList<Text> inputWords = (ArrayList<Text>) this.tokens.getList(record[1]);
        final String phrase = (String) this.phrase.getPrimitiveJavaObject(record[2]);

        if(inputTokens.size() > 0) {
            ArrayList<Text> lemmas = nlpProcess(phrase);

            ArrayList<Text> matchedWords = new ArrayList<Text>();
            Integer totalCount = 0;

            for(Text token: inputTokens) {
                if(lemmas.contains(token)) {
                    totalCount += 1;
                    Text word = inputWords.get(inputTokens.indexOf(token));
                    if(!matchedWords.contains(word)) {
                        matchedWords.add(word);
                    }
                }
            }

            if(matchedWords.size() > 0) {
                forward(newRow(phrase, matchedWords, lemmas, totalCount));
            }
        }


    }

    private StanfordCoreNLP nlpPipeline() {
        if(pipeline == null) {
            Properties props = new Properties();
            props.put("annotators","tokenize, ssplit, pos, lemma");
            pipeline = new StanfordCoreNLP(props);
        }
        return pipeline;
    }

    private ArrayList<Text> nlpProcess(String rawText) {

        ArrayList<Text> aLemmas= new ArrayList<Text>();

        try {
            String text = new String(rawText.getBytes("UTF-8"),"UTF-8");
            Annotation doc = new Annotation(text);
            nlpPipeline().annotate(doc);

            List<CoreMap> sentences = doc.get(CoreAnnotations.SentencesAnnotation.class);

            for(CoreMap sentence: sentences) {
                for(CoreLabel token: sentence.get(CoreAnnotations.TokensAnnotation.class)) {
                    String lemma = token.get(CoreAnnotations.LemmaAnnotation.class);
                    if(lemma.length() > 1) {
                        aLemmas.add(new Text(lemma));
                    }
                }
            }

            return aLemmas;
        } catch(UnsupportedEncodingException e) {
            System.out.println("Encoding to UTF-8 failed, moving to next row");
            return aLemmas;
        }
    }

    private Object[] newRow(String phrase, ArrayList<Text> words, ArrayList<Text> lemmas, Integer match_count) {

        return new Object[] {
                phrase
                , words
                , lemmas
                , match_count
                , lemmas.size()
        };
    }

    @Override
    public void close() throws HiveException {
        // do nothing
    }
}
