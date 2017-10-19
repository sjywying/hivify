package sm.hive.udtf;

import edu.stanford.nlp.ling.CoreAnnotations;
import edu.stanford.nlp.ling.CoreLabel;
import edu.stanford.nlp.pipeline.Annotation;
import edu.stanford.nlp.pipeline.StanfordCoreNLP;
import edu.stanford.nlp.sentiment.SentimentCoreAnnotations;
import edu.stanford.nlp.util.CoreMap;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.*;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

import java.io.UnsupportedEncodingException;
import java.util.*;

/**
 * Created by Sreekanth Mahesala on 12/5/16.
 */


@Description(name = "NlpParser", value = "_FUNC_(evcontent) - emits (document_sentiment, document_sentiment_score, sentences[], sentence_sentiments[], words[], tokens[], ner[], pos[]) for each row in the input")
public class NlpParser extends GenericUDTF {

    private PrimitiveObjectInspector evContentOI = null;
    private ListObjectInspector stopWordOI = null;
    private StanfordCoreNLP pipeline = null;

    @Override
    public StructObjectInspector initialize(ObjectInspector[] args) throws UDFArgumentException {

        String usage = "NlpParser(evcontent, stopWordsArray) AS document_sentiment, document_sentiment_score, sentences, sentence_sentiments, words, tokens, ner, pos";

        if (args.length !=2) {
            throw new UDFArgumentException(usage);
        }

        evContentOI = (PrimitiveObjectInspector) args[0];
        stopWordOI = (ListObjectInspector) args[1];

        List<String> fieldNames = new ArrayList<String>(2);
        List<ObjectInspector> fieldOIs = new ArrayList<ObjectInspector>(2);

        fieldNames.add("document_sentiment");
        fieldNames.add("document_sentiment_score");
        fieldNames.add("sentences");
        fieldNames.add("sentence_sentiments");
        fieldNames.add("words");
        fieldNames.add("tokens");
        fieldNames.add("ner");
        fieldNames.add("pos");

        fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector); //document_sentiment
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaDoubleObjectInspector); //document_sentiment_score
        fieldOIs.add(ObjectInspectorFactory.getStandardListObjectInspector(PrimitiveObjectInspectorFactory.javaStringObjectInspector)); //sentence
        fieldOIs.add(ObjectInspectorFactory.getStandardListObjectInspector(PrimitiveObjectInspectorFactory.javaStringObjectInspector)); //sentence_sentiments
        fieldOIs.add(ObjectInspectorFactory.getStandardListObjectInspector(PrimitiveObjectInspectorFactory.javaStringObjectInspector)); //words
        fieldOIs.add(ObjectInspectorFactory.getStandardListObjectInspector(PrimitiveObjectInspectorFactory.javaStringObjectInspector)); //tokens
        fieldOIs.add(ObjectInspectorFactory.getStandardListObjectInspector(PrimitiveObjectInspectorFactory.javaStringObjectInspector)); //ner
        fieldOIs.add(ObjectInspectorFactory.getStandardListObjectInspector(PrimitiveObjectInspectorFactory.javaStringObjectInspector)); //pos

        return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, fieldOIs);
    }

    @Override
    public void process(Object[] record) throws HiveException {
        final String evContentRow = (String) evContentOI.getPrimitiveJavaObject(record[0]);
        final List<String> stopWords = (List<String>) stopWordOI.getList(record[1]);

        if(evContentRow.length() > 25) {
            Object[] row = nlpProcess(evContentRow, stopWords);
            forward(row);
        }
    }

    private StanfordCoreNLP nlpPipeline() {
        if(pipeline == null) {
            Properties props = new java.util.Properties();
            props.put("annotators","tokenize, ssplit, pos, lemma, ner, parse, sentiment");
            props.put("parse.model", "edu/stanford/nlp/models/srparser/englishSR.ser.gz");
            props.put("parse.maxlen", "70"); //Max limit of 70 words per sentence

            pipeline = new StanfordCoreNLP(props);
        }
        return pipeline;
    }

    private Object[] nlpProcess(String rawText, List<String> stopWords) {
        try {
            String text = new String(rawText.getBytes("UTF-8"),"UTF-8");
            Annotation doc = new Annotation(text);
            nlpPipeline().annotate(doc);

            ArrayList<String> aSentences = new ArrayList<String>();
            ArrayList<String> aSentenceSentiments= new ArrayList<String>();
            ArrayList<String> aLemmas= new ArrayList<String>();
            ArrayList<String> aWords= new ArrayList<String>();
            ArrayList<String> aPoss= new ArrayList<String>();
            ArrayList<String> aNers = new ArrayList<String>();

            List<CoreMap> sentences = doc.get(CoreAnnotations.SentencesAnnotation.class);

            for(CoreMap sentence: sentences) {
                for(CoreLabel token: sentence.get(CoreAnnotations.TokensAnnotation.class)) {
                    String lemma = token.get(CoreAnnotations.LemmaAnnotation.class);
                    String word = token.get(CoreAnnotations.TextAnnotation.class);
                    String pos = token.get(CoreAnnotations.PartOfSpeechAnnotation.class);
                    String ne = token.get(CoreAnnotations.NamedEntityTagAnnotation.class);

                    if(lemma.length() > 1 && !stopWords.contains(lemma)) {
                        aWords.add(word);
                        aLemmas.add(lemma);
                        aPoss.add(pos);
                        aNers.add(ne);
                    }
                }
                aSentences.add(sentence.toString());
                aSentenceSentiments.add(sentence.get(SentimentCoreAnnotations.ClassName.class));
            }

            Object[] docSentimentValues = documentSentiment(aSentenceSentiments);
            Double documentSentimentScore = Double.valueOf(docSentimentValues[0].toString());
            String documentSentimentLabel = docSentimentValues[1].toString();

            return newRow(documentSentimentLabel, documentSentimentScore, aSentences, aSentenceSentiments, aWords,aLemmas,aNers,aPoss);
        } catch(UnsupportedEncodingException e) {
            System.out.println("Encoding to UTF-8 failed, moving to next row");
            return null;
        }
    }

    private Object[] documentSentiment(ArrayList<String> aSentiments) {

        HashMap<String, Integer> sentimentMap = new HashMap<String, Integer>();
        for(String sentiment: aSentiments) {
            if(sentimentMap.containsKey(sentiment)) {
                sentimentMap.put(sentiment, sentimentMap.get(sentiment) + 1);
            } else {
                sentimentMap.put(sentiment,1);
            }
        }

        Integer sentenceCount = aSentiments.size();
        Iterator it = sentimentMap.entrySet().iterator();

        Double weightedScore = 0.0;
        while(it.hasNext()) {
            Map.Entry pair = (Map.Entry) it.next();
            String label = pair.getKey().toString().toLowerCase();
            Double score = Double.valueOf(pair.getValue().toString())/sentenceCount;

            if(label.equals("very positive")) {
                weightedScore += (score * 1);
            } else if (label.equals("positive")) {
                weightedScore += (score * 0.9);
            } else if (label.equals("neutral")) {
                weightedScore += (score * 0.5);
            } else if(label.equals("negative")) {
                weightedScore += (score * 0.1);
            } else if(label.equals("very negative")) {
                weightedScore += (score * 0);
            }

        }

        String docLabel;
        if (weightedScore >= 0.95) docLabel = "very positive";
        else if (weightedScore > 0.6 && weightedScore < 0.95) docLabel =  "positive";
        else if (weightedScore > 0.25 && weightedScore <= 0.6)  docLabel = "neutral";
        else if (weightedScore > 0.1 && weightedScore <= 0.25)  docLabel = "negative";
        else if (weightedScore <= 0.1)  docLabel = "very negative";
        else docLabel = "unknown";

        return new Object[] {weightedScore, docLabel};
    }

    private Object[] newRow(String documentLabel, Double documentScore, ArrayList<String> sentences, ArrayList<String> sentence_sentiments, ArrayList<String> words, ArrayList<String> tokens, ArrayList<String> ner, ArrayList<String> pos) {

        return new Object[] {documentLabel
                , documentScore
                , sentences
                , sentence_sentiments
                , words
                , tokens
                , ner
                , pos
        };
    }

    @Override
    public void close() throws HiveException {
        // do nothing
    }
}
