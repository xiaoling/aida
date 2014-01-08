package mpi.aida.graph;

import gnu.trove.iterator.TObjectDoubleIterator;
import gnu.trove.map.hash.TObjectDoubleHashMap;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import mpi.aida.AidaManager;
import mpi.aida.config.AidaConfig;
import mpi.aida.config.settings.DisambiguationSettings;
import mpi.aida.data.Context;
import mpi.aida.data.Entities;
import mpi.aida.data.Entity;
import mpi.aida.data.Mention;
import mpi.aida.data.Mentions;
import mpi.aida.graph.extraction.ExtractGraph;
import mpi.aida.graph.similarity.EnsembleEntityEntitySimilarity;
import mpi.aida.graph.similarity.EnsembleMentionEntitySimilarity;
import mpi.aida.graph.similarity.MaterializedPriorProbability;
import mpi.aida.util.CollectionUtils;
import mpi.aida.util.RunningTimer;
import mpi.experiment.trace.GraphTracer;
import mpi.experiment.trace.NullGraphTracer;
import mpi.experiment.trace.Tracer;
import mpi.experiment.trace.data.EntityTracer;
import mpi.experiment.trace.data.MentionTracer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GraphGenerator {
  
  public class MaximumGraphSizeExceededException extends Exception {
    private static final long serialVersionUID = -4159436792558733318L;
    public MaximumGraphSizeExceededException() { super(); }
    public MaximumGraphSizeExceededException(String message) { super(message); }
  }

  private static final Logger logger = 
      LoggerFactory.getLogger(GraphGenerator.class);
  
  private final Mentions mentions;
  
  private final Context context;
  
  private String docId;

  private DisambiguationSettings settings;
  
  private Tracer tracer = null;

  // set this to 20000 for web server so it can still run for web server
  private int maxNumCandidateEntitiesForGraph = 0;

  public GraphGenerator(Mentions mentions, Context context, String docId, DisambiguationSettings settings, Tracer tracer) {
    //		this.storePath = content.getStoreFile();
    this.mentions = mentions;
    this.context = context;
    this.docId = docId;
    this.settings = settings;
    this.tracer = tracer;
    try {
      if (AidaConfig.get(AidaConfig.MAX_NUM_CANDIDATE_ENTITIES_FOR_GRAPH) != null) {
        maxNumCandidateEntitiesForGraph = 
            Integer.parseInt(
                AidaConfig.get(AidaConfig.MAX_NUM_CANDIDATE_ENTITIES_FOR_GRAPH));
      }
    } catch (Exception e) {
      maxNumCandidateEntitiesForGraph = 0;
    }
  }

  public Graph run() throws Exception {
    Graph gData = null;
    gData = generateGraph();
    return gData;
  }

  private Graph generateGraph() throws Exception {
    int timerId = RunningTimer.start("GraphGenerator");
    AidaManager.fillInCandidateEntities(
        mentions, settings.isIncludeNullAsEntityCandidate(),
        settings.isIncludeContextMentions(), settings.getMaxEntityRank());
    Set<String> mentionStrings = new HashSet<String>();
    Entities allEntities = getNewEntities();

    if (settings.isIncludeNullAsEntityCandidate()) {
      allEntities.setIncludesOokbeEntities(true);
    }

    // prepare tracing
    for (Mention mention : mentions.getMentions()) {
      MentionTracer mt = new MentionTracer(mention);
      tracer.addMention(mention, mt);

      for (Entity entity : mention.getCandidateEntities()) {
        EntityTracer et = new EntityTracer(entity.getName());
        tracer.addEntityForMention(mention, entity.getName(), et);
      }
    }

    // gather candidate entities - and prepare tracing
    for (Mention mention : mentions.getMentions()) {
      MentionTracer mt = new MentionTracer(mention);
      tracer.addMention(mention, mt);
      for (Entity entity : mention.getCandidateEntities()) {
        EntityTracer et = new EntityTracer(entity.getName());
        tracer.addEntityForMention(mention, entity.getName(), et);
      }

      String mentionString = mention.getMention();
      mentionStrings.add(mentionString);
      allEntities.addAll(mention.getCandidateEntities());
    }
    
    // Check if the number of candidates exceeds the threshold (for memory
    // issues).
    if (maxNumCandidateEntitiesForGraph != 0 && allEntities.size() > maxNumCandidateEntitiesForGraph) {
      throw new MaximumGraphSizeExceededException(
          "Maximum number of candidate entites for graph exceeded " + allEntities.size());
    }

    RunningTimer.stageStart("GraphGenerator", "LocalSimilarity", timerId);
    logger.debug("Computing the mention-entity similarities...");
    Map<Mention, Double> mentionL1s = null;
    if (settings.getGraphSettings().shouldUseCoherenceRobustnessTest()) {
      mentionL1s = computeMentionPriorSimL1Distances(mentions, allEntities);
    }

    EnsembleMentionEntitySimilarity mentionEntitySimilarity = 
        new EnsembleMentionEntitySimilarity(
            mentions, allEntities, context,
            settings.getSimilaritySettings(), tracer);
    logger.info("Computing the mention-entity similarities...");

    // We might drop entities here, so we have to rebuild the list of unique
    // entities
    allEntities = getNewEntities();
    // Keep the similarities for all mention-entity pairs, as some are
    // dropped later on.
    Map<Mention, TObjectDoubleHashMap<String>> mentionEntityLocalSims =
        new HashMap<Mention, TObjectDoubleHashMap<String>>();
    for (int i = 0; i < mentions.getMentions().size(); i++) {
      Mention currentMention = mentions.getMentions().get(i);
      TObjectDoubleHashMap<String> entityLocalSims =
          new TObjectDoubleHashMap<String>();
      mentionEntityLocalSims.put(currentMention, entityLocalSims);
      Entities candidateEntities = currentMention.getCandidateEntities();
      for (Entity candidate : candidateEntities) {
        // keyphrase-based mention/entity similarity.
        double similarity = mentionEntitySimilarity.calcSimilarity(currentMention, context, candidate);
        candidate.setMentionEntitySimilarity(similarity);
        entityLocalSims.put(candidate.getName(), similarity);
      }
      if (settings.getGraphSettings().shouldUseCoherenceRobustnessTest()) {
         if (mentionL1s.containsKey(currentMention) &&
             mentionL1s.get(currentMention) < 
             settings.getGraphSettings().getCohRobustnessThreshold()) {
          // drop all candidates, fix the mention to the one that has the
          // highest local similarity value
          Entity bestCandidate = getBestCandidate(currentMention);
          if (bestCandidate != null) {
            Entities candidates = new Entities();
            candidates.add(bestCandidate);
            currentMention.setCandidateEntities(candidates);
            allEntities.add(bestCandidate);
            // Update the confusable entities on the candidate,
            // there are none for the mention anymore now that it is fixed.
            Collection<Entity> confusables = bestCandidate.getConfusableEntities();
            Set<Entity> allCandidates = new HashSet<Entity>(candidateEntities.getEntities());
            allCandidates.remove(bestCandidate);
            for (Entity e : allCandidates) {
              confusables.remove(e);
            }
          }
        } else {
          allEntities.addAll(candidateEntities);
        }
      } else {
        allEntities.addAll(candidateEntities);
      }
    }
    RunningTimer.stageEnd("GraphGenerator", "LocalSimilarity", timerId);

    logger.debug("Building the graph...");
    EnsembleEntityEntitySimilarity eeSim = 
        new EnsembleEntityEntitySimilarity(
            allEntities, settings.getSimilaritySettings(), tracer);
    ExtractGraph egraph = 
        new ExtractGraph(
            docId, mentions, allEntities, eeSim, settings.getGraphSettings().getAlpha());
    Graph gData = egraph.generateGraph();
    gData.setMentionEntitySim(mentionEntityLocalSims);
    RunningTimer.end("GraphGenerator", timerId);    
    return gData;
  }
  
  private Map<Mention, Double> computeMentionPriorSimL1Distances(
      Mentions mentions, Entities allEntities) throws Exception {
    // Precompute the l1 distances between each mentions
    // prior and keyphrase based similarity.
    Map<Mention, Double> l1s = new HashMap<Mention, Double>();
    Set<String> mentionStrings = new HashSet<String>();
    for (Mention m : mentions.getMentions()) {
      mentionStrings.add(m.getMention());
    }    
    MaterializedPriorProbability pp = 
        new MaterializedPriorProbability(mentionStrings);
    EnsembleMentionEntitySimilarity keyphraseSimMeasure = 
        new EnsembleMentionEntitySimilarity(
            mentions, allEntities, context,
            settings.getGraphSettings().getCoherenceSimilaritySetting(),
            tracer);

    int solvedByLocal = 0;
    for (Mention mention : mentions.getMentions()) {
      // get prior distribution
      TObjectDoubleHashMap<String> priorDistribution = calcPriorDistribution(mention, pp);

      // get similarity distribution per UnnormCOMB (IDF+MI)
      TObjectDoubleHashMap<String> simDistribution = 
          calcSimDistribution(mention, keyphraseSimMeasure);

      // get L1 norm of both distributions, the graph algorithm can use
      // this for additional information. 
      // SOLVE_BY_LOCAL
      // otherwise, SOLVE_BY_COHERENCE
      double l1 = calcL1(priorDistribution, simDistribution);
      l1s.put(mention, l1);
      
      if (l1 < settings.getGraphSettings().getCohRobustnessThreshold()) {
        ++solvedByLocal;
        GraphTracer.gTracer.addMentionToLocalOnly(docId, mention.getMention(), mention.getCharOffset());
      }
    }
    
    if (!(GraphTracer.gTracer instanceof NullGraphTracer)) {
      gatherL1stats(docId, l1s);
      GraphTracer.gTracer.addStat(docId, "Number of fixed mention by coherence robustness", Integer.toString(solvedByLocal));
    }
    
    return l1s;
  }
  
  private void gatherL1stats(String docId, Map<Mention, Double> l1s) {
    double l1_total = 0.0;
    for (double l1 : l1s.values()) {
      l1_total += l1;
    }
    double l1_mean = l1_total / l1s.size();
    double varTemp = 0.0;
    for (double l1 : l1s.values()) {
      varTemp += Math.pow(Math.abs(l1_mean - l1), 2);
    }
    double variance = 0;
    if (l1s.size() > 1) {
      variance = varTemp / l1s.size();
    }
    GraphTracer.gTracer.addStat(docId, "L1 (prior-sim) Mean", Double.toString(l1_mean));
    GraphTracer.gTracer.addStat(docId, "L1 (prior-sim) StdDev", Double.toString(Math.sqrt(variance)));
  }
  
  private Entities getNewEntities() {
    Entities entities = new Entities();
    if (settings.isIncludeNullAsEntityCandidate()) {
      entities.setIncludesOokbeEntities(true);
    }
    return entities;
  }

  private TObjectDoubleHashMap<String> calcPriorDistribution(
      Mention mention, MaterializedPriorProbability pp) {
    TObjectDoubleHashMap<String >priors = 
        new TObjectDoubleHashMap<String>();

    for (Entity entity : mention.getCandidateEntities()) {
      priors.put(entity.getName(), 
          pp.getPriorProbability(mention.getMention(), entity));
    }

    return priors;
  }

  private TObjectDoubleHashMap<String> calcSimDistribution(
      Mention mention, EnsembleMentionEntitySimilarity combSimMeasure) throws Exception {
    TObjectDoubleHashMap<String> sims = new TObjectDoubleHashMap<String>();
    for (Entity e : mention.getCandidateEntities()) {
      sims.put(e.getName(),
              combSimMeasure.calcSimilarity(mention, context, e));
    }
    return CollectionUtils.normalizeScores(sims);
  }

  private double calcL1(TObjectDoubleHashMap<String> priorDistribution, 
                        TObjectDoubleHashMap<String> simDistribution) {
    double l1 = 0.0;

    for (TObjectDoubleIterator<String> itr = priorDistribution.iterator(); 
        itr.hasNext(); ) {
      itr.advance();
      double prior = itr.value();
      double sim = simDistribution.get(itr.key());
      double diff = Math.abs(prior - sim);
      l1 += diff;
    }

    assert (l1 >= -0.00001 && l1 <= 2.00001) : "This cannot happen, L1 must be in [0,2]. Was: " + l1;
    return l1;
  }
  
  private Entity getBestCandidate(Mention m) {
    double bestSim = Double.NEGATIVE_INFINITY;
    Entity bestCandidate = null;
    for (Entity e : m.getCandidateEntities()) {
      if (e.getMentionEntitySimilarity() > bestSim) {
        bestSim = e.getMentionEntitySimilarity();
        bestCandidate = e;
      }
    }
    return bestCandidate;
  }
}
