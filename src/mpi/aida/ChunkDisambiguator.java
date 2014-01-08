package mpi.aida;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import mpi.aida.config.settings.DisambiguationSettings;
import mpi.aida.data.ChunkDisambiguationResults;
import mpi.aida.data.Entities;
import mpi.aida.data.PreparedInputChunk;
import mpi.aida.data.ResultEntity;
import mpi.aida.data.ResultMention;
import mpi.aida.disambiguationtechnique.LocalDisambiguation;
import mpi.aida.graph.Graph;
import mpi.aida.graph.GraphGenerator;
import mpi.aida.graph.algorithms.CocktailParty;
import mpi.aida.graph.algorithms.CocktailPartySizeConstrained;
import mpi.aida.graph.algorithms.DisambiguationAlgorithm;
import mpi.aida.util.RunningTimer;
import mpi.experiment.trace.GraphTracer;
import mpi.experiment.trace.GraphTracer.TracingTarget;
import mpi.experiment.trace.Tracer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Main class for running the disambiguation. Is thread-safe and can be
 * run in parallel.
 * 
 */
public class ChunkDisambiguator implements Runnable {
  private static final Logger logger = 
      LoggerFactory.getLogger(ChunkDisambiguator.class);
  
  private PreparedInputChunk input;

  private DisambiguationSettings settings;

  private Tracer tracer;
  
  private Map<PreparedInputChunk, ChunkDisambiguationResults> results;
    
  public ChunkDisambiguator(PreparedInputChunk input, DisambiguationSettings settings, 
                      Map<PreparedInputChunk, ChunkDisambiguationResults> results, 
                      Tracer tracer) {
    this.input = input;
    this.settings = settings;
    this.results = results;
    this.tracer = tracer;
  }

  public ChunkDisambiguationResults disambiguate() throws Exception {
    Integer timerId = RunningTimer.start("ChunkDisambiguator");    
    Map<ResultMention, List<ResultEntity>> mentionMappings = null;

    switch (settings.getDisambiguationTechnique()) {
      case LOCAL:
        mentionMappings = runLocalDisambiguation(input, settings, tracer);
        break;
//      case LOCAL_ITERATIVE:
//        mentionMappings = runLocalDisambiguationIterative(input, settings, tracer);
//        break;
      case GRAPH:
        mentionMappings = runGraphDisambiguation(input, settings);
        break;
//      case CHAKRABARTI:
//        mentionMappings = runChakrabartiDisambiguation(input, settings);
      default:
        break;
    }
    RunningTimer.end("ChunkDisambiguator", timerId);   

    // do the tracing
    String tracerHtml = null;  //tracer.getHtmlOutput();
    TracingTarget target = settings.getTracingTarget();

    if (GraphTracer.gTracer.canGenerateHtmlFor(input.getChunkId())) {
      tracerHtml = GraphTracer.gTracer.generateHtml(input.getChunkId(), target);
      GraphTracer.gTracer.removeDocId(input.getChunkId());
    } else if (GraphTracer.gTracer.canGenerateHtmlFor(Integer.toString(input.getChunkId().hashCode()))) {
      tracerHtml = GraphTracer.gTracer.generateHtml(Integer.toString(input.getChunkId().hashCode()), target);
      GraphTracer.gTracer.removeDocId(Integer.toString(input.getChunkId().hashCode()));
    }
    
    ChunkDisambiguationResults disambiguationResults = new ChunkDisambiguationResults(mentionMappings, tracerHtml);

    if (settings.getNullMappingThreshold() >= 0.0) {
      // calc threshold
//      double threshold = settings.getSimilaritySettings().getNormalizedAverageScore() * settings.getNullMappingThresholdFactor();
      double threshold = settings.getNullMappingThreshold();
      logger.info(
          "Dropping all entities below the score threshold of " + threshold);

      // drop anything below the threshold                  
      for (ResultMention rm : disambiguationResults.getResultMentions()) {
        double score = disambiguationResults.getBestEntity(rm).getDisambiguationScore();

        if (score < threshold) {
          List<ResultEntity> nme = new ArrayList<ResultEntity>(1);
          nme.add(ResultEntity.getNoMatchingEntity());
          disambiguationResults.setResultEntities(rm, nme);
        }
      }
    }

    return disambiguationResults;
  }

//  private Map<ResultMention, List<ResultEntity>> runChakrabartiDisambiguation(PreparedInputChunk content, DisambiguationSettings settings) throws Exception {
//    ChakrabartiTechnique chakra = new ChakrabartiTechnique(content, settings.getSimilaritySettings(), 0.0, typeClassifier, tracer);
//    Map<ResultMention, List<ResultEntity>> results = chakra.disambiguate();
//    return results;
//  }

  private Map<ResultMention, List<ResultEntity>> runGraphDisambiguation(
      final PreparedInputChunk content, final DisambiguationSettings settings) throws Exception {

    GraphGenerator gg = new GraphGenerator(content.getMentions(), 
        content.getContext(), content.getChunkId(), settings, tracer);
    Graph gData = gg.run();

    // Run the algorithm.
    DisambiguationAlgorithm da = null;
    switch (settings.getDisambiguationAlgorithm()) {
      case COCKTAIL_PARTY:
        da = new CocktailParty(gData, settings.getGraphSettings(),
            settings.shouldComputeConfidence(),
            settings.getConfidenceSettings());
        break;
      case COCKTAIL_PARTY_SIZE_CONSTRAINED:
        da = new CocktailPartySizeConstrained(gData, 
            settings.getGraphSettings(),
            settings.shouldComputeConfidence(),
            settings.getConfidenceSettings());
        break;
//      case RANDOM_WALK:
//        da = new RandomWalk(gData, 0.15, 10000);
      default:
        break;
    }

    Map<ResultMention, List<ResultEntity>> results = da.disambiguate();

    // Replace name-OOKBE placeholders by plain OOKBE placeholders. 
    if (settings.isIncludeNullAsEntityCandidate()) {
      Map<ResultMention, List<ResultEntity>> nmeCleanedResults = new HashMap<ResultMention, List<ResultEntity>>();

      for (Entry<ResultMention, List<ResultEntity>> e : results.entrySet()) {
        if (Entities.isOokbeName(e.getValue().get(0).getEntity())) {
          List<ResultEntity> nme = new ArrayList<ResultEntity>(1);
          nme.add(ResultEntity.getNoMatchingEntity());
          nmeCleanedResults.put(e.getKey(), nme);
        } else {
          nmeCleanedResults.put(e.getKey(), e.getValue());
        }
      }
      results = nmeCleanedResults;
    }

    return results;
  }

  private Map<ResultMention, List<ResultEntity>> runLocalDisambiguation(PreparedInputChunk content, DisambiguationSettings settings, Tracer tracer) throws SQLException {
    Map<String, Map<ResultMention, List<ResultEntity>>> solutions = Collections.synchronizedMap(new HashMap<String, Map<ResultMention, List<ResultEntity>>>());
    LocalDisambiguation ld = 
        new LocalDisambiguation(
            content, settings.getSimilaritySettings(), 
            settings.isIncludeNullAsEntityCandidate(), 
            settings.isIncludeContextMentions(), 
            settings.shouldComputeConfidence(),
            settings.getMaxEntityRank(),
            content.getChunkId(), solutions, tracer);
    ld.run();
    return solutions.get(content.getChunkId());
  }

//  private Map<ResultMention, List<ResultEntity>> runLocalDisambiguationIterative(PreparedInputChunk content, DisambiguationSettings settings, Tracer tracer) throws SQLException {
//    Map<String, Map<ResultMention, List<ResultEntity>>> solutions = Collections.synchronizedMap(new HashMap<String, Map<ResultMention, List<ResultEntity>>>());
//    LocalDisambiguation ld = new LocalDisambiguationIterative(content, settings.getSimilaritySettings(), settings.isIncludeNullAsEntityCandidate(), settings.isIncludeContextMentions(), content.getDocId(), solutions, tracer);
//    ld.run();
//    return solutions.get(content.getDocId());
//  }

  @Override
  public void run() {
    try {
      ChunkDisambiguationResults result = disambiguate();
      results.put(input, result);
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}