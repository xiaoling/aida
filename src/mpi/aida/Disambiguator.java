package mpi.aida;

import java.text.NumberFormat;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import mpi.aida.config.settings.DisambiguationSettings;
import mpi.aida.data.ChunkDisambiguationResults;
import mpi.aida.data.DisambiguationResults;
import mpi.aida.data.PreparedInput;
import mpi.aida.data.PreparedInputChunk;
import mpi.aida.data.ResultEntity;
import mpi.aida.data.ResultMention;
import mpi.aida.util.DocumentCounter;
import mpi.aida.util.RunningTimer;
import mpi.experiment.trace.NullTracer;
import mpi.experiment.trace.Tracer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class Disambiguator implements Runnable {
  
  private Logger logger_ = LoggerFactory.getLogger(Disambiguator.class);
  
  private PreparedInput preparedInput_;
  private DisambiguationSettings settings_;
  private DocumentCounter documentCounter_;
  private Map<String, DisambiguationResults> resultsMap_;
  private Tracer tracer_;
 
  private NumberFormat nf;
  
  /** 
   * Common init.
   */
  private void init(PreparedInput input, DisambiguationSettings settings,
                  Tracer tracer) {
    preparedInput_ = input;
    settings_ = settings;
    tracer_ = tracer;
    nf = NumberFormat.getNumberInstance(Locale.ENGLISH);
    nf.setMaximumFractionDigits(2);
  }
  
  /**
   * Use this when calling Disambiguator in parallel.
   * 
   * @param input
   * @param settings
   * @param resultsMap
   * @param tracer
   * @param dc 
   */
  public Disambiguator(PreparedInput input, DisambiguationSettings settings, 
      Map<String, DisambiguationResults> resultsMap, Tracer tracer, DocumentCounter dc) {
    this(input, settings, tracer);
    resultsMap_ = resultsMap;
    documentCounter_ = dc;
  }
    
  /**
   * tracer is set to NullTracer();
   * @param input
   * @param settings
   */
  public Disambiguator(PreparedInput input, DisambiguationSettings settings) {
    init(input, settings, new NullTracer());
  }

  public Disambiguator(PreparedInput input, DisambiguationSettings settings, Tracer tracer) {
    init(input, settings, tracer);
  }

  
  public DisambiguationResults disambiguate() throws InterruptedException {
    logger_.info("Disambiguating '" + preparedInput_.getDocId() + "' with " + 
        preparedInput_.getChunksCount() + " chunks and " +
        preparedInput_.getMentionSize() + " mentions."); 
    Integer runningId = RunningTimer.start("Disambiguator");
    Map<PreparedInputChunk, ChunkDisambiguationResults> chunkResults =
        disambiguateChunks(preparedInput_);
    DisambiguationResults results = 
        aggregateChunks(preparedInput_, chunkResults);
    Long runTimeInMs = RunningTimer.end("Disambiguator", runningId);
    double runTime = runTimeInMs / (double) 1000;
    logger_.info("Document '" + preparedInput_.getDocId() + "' done in " + 
                nf.format(runTime) + "s");
    return results;
  }

  private Map<PreparedInputChunk, ChunkDisambiguationResults> disambiguateChunks(
      PreparedInput preparedInput) throws InterruptedException {
    Map<PreparedInputChunk, ChunkDisambiguationResults> chunkResults =
        new HashMap<PreparedInputChunk, ChunkDisambiguationResults>();
    ExecutorService es = Executors.newFixedThreadPool(settings_.getNumChunkThreads());
    for (PreparedInputChunk c : preparedInput_) {
      ChunkDisambiguator cd = new ChunkDisambiguator(c, settings_, chunkResults, tracer_);
      es.execute(cd);
    }
    es.shutdown();
    es.awaitTermination(1, TimeUnit.DAYS);
    return chunkResults;
  }
  
  /**
   * For the time being, just put everything together.
   * It should take into account potential conflicts across chunks (e.g.
   * the same mention string pointing to different entities, which is unlikely
   * in the same document).
   * 
   * @param preparedInput
   * @param chunkResults
   * @return
   */
  private DisambiguationResults aggregateChunks(PreparedInput preparedInput,
      Map<PreparedInputChunk, ChunkDisambiguationResults> chunkResults) {
    Map<ResultMention, List<ResultEntity>> mentionMappings = 
        new HashMap<ResultMention, List<ResultEntity>>();
    
    StringBuilder gtracerHtml = new StringBuilder(); 
    for (Entry<PreparedInputChunk,ChunkDisambiguationResults> e : chunkResults.entrySet()) {
      PreparedInputChunk p = e.getKey();
      ChunkDisambiguationResults cdr = e.getValue();
      gtracerHtml.append("<div>");
      gtracerHtml.append(cdr.getgTracerHtml());
      gtracerHtml.append("</div>");
      gtracerHtml.append("<div style='font-size:8pt;color:#DDDDDD'>chunkid: ").append(p.getChunkId()).append("</p>");
      for (ResultMention rm : cdr.getResultMentions()) {
        List<ResultEntity> res = cdr.getResultEntities(rm);
        rm.setDocId(preparedInput.getDocId());
        mentionMappings.put(rm, res);
      }
    }
    
    DisambiguationResults results = 
        new DisambiguationResults(mentionMappings, gtracerHtml.toString());
    return results;
  }
  
  @Override
  public void run() {
    try {
      DisambiguationResults result = disambiguate();
      result.setTracer(tracer_);
      if (documentCounter_ != null) {
        // This provides a means of knowing where we are
        // and how long it took until now.
        documentCounter_.oneDone();
      }
      resultsMap_.put(preparedInput_.getDocId(), result);
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}
