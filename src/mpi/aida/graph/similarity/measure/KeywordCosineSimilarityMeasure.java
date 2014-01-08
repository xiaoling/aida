package mpi.aida.graph.similarity.measure;

import gnu.trove.TIntCollection;
import gnu.trove.list.linked.TIntLinkedList;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;

import mpi.aida.data.Entity;
import mpi.aida.graph.similarity.context.EntitiesContext;
import mpi.aida.graph.similarity.context.FastWeightedKeyphrasesContext;
import mpi.aida.util.CollectionUtils;
import mpi.experiment.trace.NullEntityEntityTracing;
import mpi.experiment.trace.Tracer;
import mpi.experiment.trace.measures.KeytermEntityEntityMeasureTracer;
import mpi.experiment.trace.measures.TermTracer;

public class KeywordCosineSimilarityMeasure extends EntityEntitySimilarityMeasure {

  public KeywordCosineSimilarityMeasure(Tracer tracer) {
    super(tracer);
  }

  private FastWeightedKeyphrasesContext kwc;

  public double calcSimilarity(Entity a, Entity b, EntitiesContext entitiesContext) {
    // additional stuff for tracing
    Map<Integer, Double> matches = new HashMap<Integer, Double>();

    kwc = (FastWeightedKeyphrasesContext) entitiesContext;
    
    int[] kwA = kwc.getKeywordArray(a);
    int[] kwB = kwc.getKeywordArray(b);
    int[] is = intersect(kwA, kwB);
      
    double dotprod = 0.0;
    
    for (int i : is) {      
      double v1 = kwc.getCombinedKeywordMiIdfWeight(a, i);
      double v2 = kwc.getCombinedKeywordMiIdfWeight(b, i);

      if (v1 > 0 && v2 > 0) {
        double tmp = v1 * v2;
        dotprod += tmp;

        matches.put(i, tmp);
      }
    }

    double norm1 = kwc.getWeightVectorNorm(a);
    double norm2 = kwc.getWeightVectorNorm(b);
    
    double sim = 0.0;
    double denom = norm1 * norm2;

    if (denom != 0) {
      sim = dotprod / denom;
    }

    if (!(tracer.eeTracing() instanceof NullEntityEntityTracing)) {
      Map<Integer, Double> e1keywords = new HashMap<Integer, Double>();     
      for (int i=0;i<kwA.length;i++) {
        double weight = kwc.getCombinedKeywordMiIdfWeight(a, i);
        if (weight > 0.0) {
          e1keywords.put(i, weight);
        }
      }     
      e1keywords = CollectionUtils.sortMapByValue(e1keywords, true);
      Map<Integer, Double> e1top = new LinkedHashMap<Integer, Double>();           
      for (Entry<Integer, Double> e : e1keywords.entrySet()) {       
        e1top.put(e.getKey(), e.getValue());
      }      
      e1keywords = e1top;
      
      
      Map<Integer, Double> e2keywords = new HashMap<Integer, Double>();
      for (int i = 0; i < kwB.length; i++) {
        double weight = kwc.getCombinedKeywordMiIdfWeight(b, i);
        if (weight > 0.0) {
          e2keywords.put(i, weight);
        }
      }  
      e2keywords = CollectionUtils.sortMapByValue(e2keywords, true);
      Map<Integer, Double> e2top = new LinkedHashMap<Integer, Double>();           

      for (Entry<Integer, Double> e : e2keywords.entrySet()) {       
        e2top.put(e.getKey(), e.getValue());
      }      
      e2keywords = e2top;
      
      
      Map<Integer, TermTracer> matchedKeywords = new HashMap<Integer, TermTracer>();
      for (Integer kw : matches.keySet()) {
        TermTracer tt = new TermTracer();
        tt.setTermWeight(matches.get(kw));
        matchedKeywords.put(kw, tt);
      }
      
      tracer.eeTracing().addEntityContext(a.getName(), e1keywords);
      tracer.eeTracing().addEntityContext(b.getName(), e2keywords);

      KeytermEntityEntityMeasureTracer mt = new KeytermEntityEntityMeasureTracer("KeywordCosineSim", 0.0, e2keywords, matchedKeywords);
      mt.setScore(sim);
      tracer.eeTracing().addEntityEntityMeasureTracer(a.getName(), b.getName(), mt);
      
      KeytermEntityEntityMeasureTracer mt2 = new KeytermEntityEntityMeasureTracer("KeywordCosineSim", 0.0, e1keywords, matchedKeywords);
      mt2.setScore(sim);
      tracer.eeTracing().addEntityEntityMeasureTracer(b.getName(), a.getName(), mt2);
    }

    return sim;
  }
  
  private int[] intersect(int[] a, int[] b) {
    if (a == null || a.length == 0 || b == null || b.length == 0) {
      return new int[0];
    }
   
    TIntCollection is = new TIntLinkedList();
    int i = 0, j = 0;
    while (i < a.length && j < b.length) {
      if (a[i] == b[j]) {
        is.add(a[i]);
        ++i;
        ++j;
      } else if (a[i] < b[j]) {
        ++i;
      } else {
        ++j;
      }
    }
    return is.toArray();
  }
}
