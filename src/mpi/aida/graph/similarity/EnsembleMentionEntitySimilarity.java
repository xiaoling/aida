package mpi.aida.graph.similarity;

import gnu.trove.iterator.TIntDoubleIterator;
import gnu.trove.map.hash.TIntDoubleHashMap;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import mpi.aida.data.Context;
import mpi.aida.data.Entities;
import mpi.aida.data.Entity;
import mpi.aida.data.Mention;
import mpi.aida.data.Mentions;
import mpi.aida.graph.similarity.importance.EntityImportance;
import mpi.aida.graph.similarity.util.SimilaritySettings;
import mpi.experiment.trace.Tracer;
import mpi.experiment.trace.measures.EntityImportanceMeasureTracer;
import mpi.experiment.trace.measures.PriorMeasureTracer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class uses learned weights for all MentionEntitySimilarityMeasure X EntityContext
 * combination to create a combined MentionEntitySimilarity measure.
 * 
 * The prior probability of a mention-entity pair can also be used, and should
 * generally improve results.
 * 
 * TODO the constructor should not use the Entities object but build the union of
 * all candidate entities from the Mentions (getCandidateEntities()) themselves.
 */
public class EnsembleMentionEntitySimilarity {
  private static final Logger logger = 
      LoggerFactory.getLogger(EnsembleMentionEntitySimilarity.class);
  
  private List<MentionEntitySimilarity> mes;
  private Map<String, double[]> mesMinMax;
  Map<String, Map<Mention, TIntDoubleHashMap>> precomputedScores;  
  /**
   * EntityImportances need to be in [0, 1].
   */
  private List<EntityImportance> eis;

  private PriorProbability pp = null;

  private SimilaritySettings settings;

  private Tracer tracer = null;

  /**
   * Use this constructor if the context is the same for all mention-entity pairs.
   * 
   * @param mentions
   * @param entities
   * @param context
   * @param settings
   * @param tracer
   * @throws Exception
   */
  public EnsembleMentionEntitySimilarity(Mentions mentions, Entities entities, Context context, SimilaritySettings settings, Tracer tracer) throws Exception {
    Map<Mention, Context> sameContexts = new HashMap<Mention, Context>();
    for (Mention m : mentions.getMentions()) {
      sameContexts.put(m, context);
    }
    init(mentions, entities, sameContexts, settings, tracer);
  }
  
  public EnsembleMentionEntitySimilarity(Mentions mentions, Entities entities, Map<Mention, Context> mentionsContexts, SimilaritySettings settings, Tracer tracer) throws Exception {
    init(mentions, entities, mentionsContexts, settings, tracer);
  }

  private void init(Mentions mentions, Entities entities, Map<Mention, Context> mentionsContexts, SimilaritySettings settings, Tracer tracer) throws Exception {
    this.settings = settings;
    double prior = settings.getPriorWeight();
    Set<String> mentionNames = new HashSet<String>();
    for (Mention m : mentions.getMentions()) {
      mentionNames.add(m.getMention());
    }
    pp = new MaterializedPriorProbability(mentionNames);
    pp.setWeight(prior);
    mes = settings.getMentionEntitySimilarities(entities, tracer);
    // adjust weights when switched
    if (settings.getPriorThreshold() >= 0.0) {
      double[] nonPriorWeights = new double[mes.size() / 2];
      for (int i = 0; i < mes.size() / 2; i++) {
        nonPriorWeights[i] = mes.get(i).getWeight();
      }
      double[] normNonPriorWeights = rescaleArray(nonPriorWeights);
      for (int i = 0; i < mes.size() / 2; i++) {
        mes.get(i).setWeight(normNonPriorWeights[i]);
      }

      double[] withPriorWeights = new double[mes.size() / 2 + 1];

      for (int i = mes.size() / 2; i < mes.size(); i++) {
        withPriorWeights[i - mes.size() / 2] = mes.get(i).getWeight();
      }

      withPriorWeights[withPriorWeights.length - 1] = pp.getWeight();

      double[] normWithPriorWeights = rescaleArray(withPriorWeights);
      for (int i = mes.size() / 2; i < mes.size(); i++) {
        mes.get(i).setWeight(normWithPriorWeights[i - mes.size() / 2]);
      }
      pp.setWeight(normWithPriorWeights[normWithPriorWeights.length - 1]);
    }
    eis = settings.getEntityImportances(entities);
    this.tracer = tracer;
    mesMinMax = precomputeMinMax(mentions, mentionsContexts);
  }
  
  /**
   * Updates precomputedScores. 
   * 
   * @param mentions
   * @param mentionsContexts
   * @return
   * @throws Exception
   */
  private Map<String, double[]> precomputeMinMax(
      Mentions mentions, Map<Mention, Context> mentionsContexts) throws Exception {
    precomputedScores = new HashMap<String, Map<Mention,TIntDoubleHashMap>>();
    //scores stay the same for the switched measures, only weights change
    for (MentionEntitySimilarity s : mes) {
      if(precomputedScores.containsKey(s.getIdentifier())) {
        continue;
      }
      Map<Mention, TIntDoubleHashMap> measureScores =
          new HashMap<Mention, TIntDoubleHashMap>();
      precomputedScores.put(s.getIdentifier(), measureScores);
      for (Mention m : mentions.getMentions()) {
        Context context = mentionsContexts.get(m);
        TIntDoubleHashMap mentionScores = new TIntDoubleHashMap();
        measureScores.put(m, mentionScores);
        for (Entity e : m.getCandidateEntities()) {
          double sim = s.calcSimilarity(m, context, e);
          mentionScores.put(e.getId(), sim);
        }
      }
    }
    Map<String, double[]> measureMinMaxs =
        new HashMap<String, double[]>();
    for (String  s : precomputedScores.keySet()) {
      double[] minMax = new double[] { Double.MAX_VALUE, 0.0 }; 
      measureMinMaxs.put(s, minMax);
      Map<Mention, TIntDoubleHashMap> measureScores = precomputedScores.get(s);
      for (TIntDoubleHashMap mentionScores : measureScores.values()) {
        for (TIntDoubleIterator itr = mentionScores.iterator(); itr.hasNext(); ) {
          itr.advance();
          double score = itr.value();
          minMax[0] = Math.min(minMax[0], score);
          minMax[1] = Math.max(minMax[1], score);
        }
      }
    }
    return measureMinMaxs;
  }

  public static double[] rescaleArray(double[] in) {
    double[] out = new double[in.length];

    double total = 0;

    for (double i : in) {
      total += i;
    }

    // rescale
    for (int i = 0; i < in.length; i++) {
      double norm = in[i] / total;
      out[i] = norm;
    }

    return out;
  }

  public double calcSimilarity(Mention mention, Context context, Entity entity) throws Exception {
    double bestPrior = pp.getBestPrior(mention.getMention());
    boolean shouldSwitch = settings.getPriorThreshold() > 0.0; 
    // If non-switch sim is computed, prior is always used. Otherwise determine based on the threshold and the distribution.
    boolean shouldUsePrior = !shouldSwitch || shouldIncludePrior(bestPrior, settings.getPriorThreshold(), mention);
    MentionEntitySimilarity[] mesToUse = 
        getMentionEntitySimilarities(mes, shouldUsePrior, shouldSwitch);

    double weightedSimilarity = 0.0;

    for (MentionEntitySimilarity s : mesToUse) {
      double singleSimilarity = precomputedScores.get(s.getIdentifier()).get(mention).get(entity.getId());
      double[] minMax = mesMinMax.get(s.getIdentifier());
      singleSimilarity = rescale(singleSimilarity, minMax[0], minMax[1]);
      weightedSimilarity += singleSimilarity * s.getWeight();
    }

    for (EntityImportance ei : eis) {
      double singleImportance = ei.getImportance(entity);
      weightedSimilarity += singleImportance * ei.getWeight();
      
      EntityImportanceMeasureTracer mt = new EntityImportanceMeasureTracer(ei.getIdentifier(), ei.getWeight());
      mt.setScore(singleImportance);
      tracer.addMeasureForMentionEntity(mention, entity.getName(), mt);
    }

    if (shouldUsePrior && pp != null && settings.getPriorWeight() > 0.0) {
      double weightedPrior = pp.getPriorProbability(mention.getMention(), entity);
      weightedSimilarity += weightedPrior * pp.getWeight();

      PriorMeasureTracer mt = new PriorMeasureTracer("Prior", pp.getWeight());
      mt.setScore(weightedPrior);
      tracer.addMeasureForMentionEntity(mention, entity.getName(), mt);
    }
    
    tracer.setMentionEntityTotalSimilarityScore(mention, entity.getName(), weightedSimilarity);

    return weightedSimilarity;
  }

  /**
   * First half of similarity measures MUST BE the switched ones. 
   * All other ones are just used normally
  
   * @param mentionEntitySimilarities
   * @param shouldSwitch 
   * @param shouldUsePrior 
   * @param usePrior
   * @return
   */
  private MentionEntitySimilarity[] getMentionEntitySimilarities(
      List<MentionEntitySimilarity> mentionEntitySimilarities, boolean shouldUsePrior, boolean shouldSwitch) {
    int start = 0;
    int end = mentionEntitySimilarities.size();
    if (shouldSwitch) {
      start = 0;
      end = mentionEntitySimilarities.size() / 2;
      if (shouldUsePrior) {
        start = mentionEntitySimilarities.size() / 2;
        end = mentionEntitySimilarities.size();
      }
    }
    MentionEntitySimilarity[] mesToUse = new MentionEntitySimilarity[end - start];
    for (int i = start; i < end; i++) {
      mesToUse[i - start] = mentionEntitySimilarities.get(i);
    }
    return mesToUse;
  }

  private boolean shouldIncludePrior(double bestPrior, double priorThreshold, Mention mention) {    
    boolean shouldUse = bestPrior > priorThreshold;

    if (!shouldUse) {
      return false;
    } else {
      // make sure that at least 10% of all candidates have a prior to make up for lacking data
      int total = 0;
      int withPrior = 0;

      for (Entity e : mention.getCandidateEntities()) {
        total++;

        if (pp.getPriorProbability(mention.getMention(), e) > 0.0) {
          withPrior++;
        }
      }

      double priorRatio = (double) withPrior / (double) total;

      if (priorRatio >= 0.2) {
        return true;
      } else {
        return false;
      }
    }
  }

  public static double rescale(double value, double min, double max) {
    if (min == max) {
      // No score or only one, return max.
      return 1.0;
    }
    
    if (value < min) {
      logger.debug("Wrong normalization, " + 
                    value + " not in [" + min + "," + max + "], " +
                   "renormalizing to 0.0.");
      return 0.0;
    } else if (value > max) {
      logger.debug("Wrong normalization, " + 
          value + " not in [" + min + "," + max + "], " +
         "renormalizing to 1.0.");
      return 1.0;
    }
    return (value - min) / (max - min);
  }
  
  public void announceMentionAssignment(Mention mention, Entity entity) {
	  for(MentionEntitySimilarity mesInstance: mes) {
		  mesInstance.announceMentionAssignment(mention, entity);
	  }
  }
  public void addExtraContext(Mention mention, Object context) {
	  for(MentionEntitySimilarity mesInstance: mes) {
		  mesInstance.addExtraContext(mention, context);
	  }
  }
}
