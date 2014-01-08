package mpi.aida.disambiguationtechnique;

import java.sql.SQLException;
import java.text.NumberFormat;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;

import mpi.aida.AidaManager;
import mpi.aida.data.Entities;
import mpi.aida.data.Entity;
import mpi.aida.data.Mention;
import mpi.aida.data.PreparedInputChunk;
import mpi.aida.data.ResultEntity;
import mpi.aida.data.ResultMention;
import mpi.aida.graph.similarity.EnsembleMentionEntitySimilarity;
import mpi.aida.graph.similarity.util.SimilaritySettings;
import mpi.aida.util.CollectionUtils;
import mpi.experiment.trace.Tracer;
import mpi.experiment.trace.data.EntityTracer;
import mpi.experiment.trace.data.MentionTracer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LocalDisambiguation implements Runnable {
  private static final Logger logger = 
      LoggerFactory.getLogger(LocalDisambiguation.class);
  
	protected SimilaritySettings ss;

	protected PreparedInputChunk input;

	protected String chunkId;

	protected Map<String, Map<ResultMention, List<ResultEntity>>> solutions;

	protected boolean includeNullAsEntityCandidate;
	
	protected boolean includeContextMentions;
	
	protected boolean computeConfidence;
	
	protected double maxEntityRank;

	protected Tracer tracer = null;

	private NumberFormat nf;
	
	public LocalDisambiguation(PreparedInputChunk content, SimilaritySettings settings,
			boolean includeNullAsEntityCandidate,
			boolean includeContextMentions, boolean computeConfidence, 
			double maxEntityRank, String docId,
			Map<String, Map<ResultMention, List<ResultEntity>>> solutions, 
			Tracer tracer)
			throws SQLException {
	  nf = NumberFormat.getNumberInstance(Locale.ENGLISH);
	  nf.setMaximumFractionDigits(2);
	  logger.debug("Preparing '" + docId + "' (" + content.getMentions().getMentions().size() + " mentions)");

		this.ss = settings;
		this.chunkId = docId;
		this.solutions = solutions;
		this.input = content;
		this.includeNullAsEntityCandidate = includeNullAsEntityCandidate;
		this.includeContextMentions = includeContextMentions;
		this.computeConfidence = computeConfidence;
		this.maxEntityRank = maxEntityRank;
		this.tracer = tracer;
		
		logger.debug("Finished preparing '" + docId + "'");
	}

	@Override
	public void run() {	  
	  try {
      AidaManager.fillInCandidateEntities(input.getMentions(),
          includeNullAsEntityCandidate, includeContextMentions, maxEntityRank);
    } catch (SQLException e) {
      logger.error("SQLException when getting candidates: " + 
                   e.getLocalizedMessage());
    }
		EnsembleMentionEntitySimilarity mes = prepapreMES();
		try {
      disambiguate(mes);
    } catch (Exception e) {
      logger.error("Error: " + e.getLocalizedMessage());
      e.printStackTrace();
    }
	}
	
	private EnsembleMentionEntitySimilarity prepapreMES() {
		Entities entities = new Entities();
		for (Mention mention : input.getMentions().getMentions()) {
			MentionTracer mt = new MentionTracer(mention);
			tracer.addMention(mention, mt);
			for (Entity entity : mention.getCandidateEntities()) {
				EntityTracer et = new EntityTracer(entity.getName());
				tracer.addEntityForMention(mention, entity.getName(), et);
			}
			entities.addAll(mention.getCandidateEntities());
		}
		
		if (includeNullAsEntityCandidate) {
			entities.setIncludesOokbeEntities(true);
		}

		EnsembleMentionEntitySimilarity mes = null;
		try {
			mes = new EnsembleMentionEntitySimilarity(input.getMentions(), entities, 
			    input.getContext(), ss, tracer);
			return mes;
		} catch (Exception e) {
			e.printStackTrace();
			return null;
		}
	}
	
	protected void disambiguate(EnsembleMentionEntitySimilarity mes) throws Exception {
		for (Mention mention : input.getMentions().getMentions()) {		
			List<ResultEntity> entities = new LinkedList<ResultEntity>();
			
			// Compute all scores.
			Map<String, Double> entityScores = new HashMap<String, Double>();
			for (Entity entity : mention.getCandidateEntities()) { 
			  double sim = mes.calcSimilarity(mention, input.getContext(), entity);
			  // After disambiguation, replace OOKBE suffixed name with placeholder.
			  String name = entity.getName();
        if (entity.isOOKBentity()) {
          name = Entity.OOKBE;
        }
			  entityScores.put(name, sim);
			}
			
			if (computeConfidence) {
  	    // Normalize similarities so that they sum up to one. The mass of the
        // score that the best entity accumulates will also be a measure of the
        // confidence that the mapping is correct.
			  entityScores = CollectionUtils.normalizeScores(entityScores);
			}
			
			// Create ResultEntities.
			for (Entry<String, Double> e : entityScores.entrySet()) {
			  entities.add(new ResultEntity(e.getKey(), e.getValue()));
			}
  			
	     // Distinguish a the cases of empty, unambiguous, and ambiguous mentions.
      if (entities.isEmpty()) {
        // Assume a 95% confidence, as the coverage of names of the dictionary
        // is quite good.
        ResultEntity re = ResultEntity.getNoMatchingEntity();
        if (computeConfidence) {
          re.setDisambiguationScore(0.95);
        }
        entities.add(re);
      } else if (entities.size() == 1 && computeConfidence) {
        // Do not give full confidence to unambiguous mentions, as there might
        // be meanings missing.
        entities.get(0).setDisambiguationScore(0.95);
      } 
			
			// Sort the candidates by their score.
			Collections.sort(entities);

			// Fill solutions.
			Map<ResultMention, List<ResultEntity>> docSolutions = solutions.get(chunkId);
			if (docSolutions == null) {
				docSolutions = new HashMap<ResultMention, List<ResultEntity>>();
				solutions.put(chunkId, docSolutions);
			}
			ResultMention rm = new ResultMention(chunkId, mention.getMention(), mention.getCharOffset(), mention.getCharLength());
			docSolutions.put(rm, entities);
		}
	}
}
