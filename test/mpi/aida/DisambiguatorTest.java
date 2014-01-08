package mpi.aida;

import static org.junit.Assert.assertEquals;

import java.util.HashMap;
import java.util.Map;

import mpi.aida.config.AidaConfig;
import mpi.aida.config.settings.DisambiguationSettings;
import mpi.aida.config.settings.PreparationSettings;
import mpi.aida.config.settings.disambiguation.CocktailPartyDisambiguationSettings;
import mpi.aida.data.DisambiguationResults;
import mpi.aida.data.Entity;
import mpi.aida.data.PreparedInput;
import mpi.aida.data.ResultMention;
import mpi.aida.preparation.mentionrecognition.FilterMentions.FilterType;

import org.junit.Test;

/**
 * Testing against the predefined DataAccessForTesting.
 * 
 */
public class DisambiguatorTest {
  public static final double DEFAULT_ALPHA = 0.6;
  public static final double DEFAULT_COH_ROBUSTNESS = 0.9;
  public static final int DEFAULT_SIZE = 5;
  
  public DisambiguatorTest() {
    AidaConfig.set("dataAccess", "testing");
    AidaConfig.set(AidaConfig.CACHE_WORD_DATA, "false");
    AidaManager.init();
  }
  
  @Test
  public void testPageKashmir() throws Exception {
    Preparator p = new Preparator();

    String docId = "testPageKashmir1";
    String content = "When [[Page]] played Kashmir at Knebworth, his Les Paul was uniquely tuned.";
    PreparationSettings prepSettings = new PreparationSettings();
    prepSettings.setMentionsFilter(FilterType.STANFORD_NER);
    prepSettings.setUseHybridMentionDetection(true);

    PreparedInput preparedInput = p.prepare(docId, content, new PreparationSettings());

    DisambiguationSettings settings = new CocktailPartyDisambiguationSettings();
    settings.getGraphSettings().setAlpha(DEFAULT_ALPHA);
    settings.getGraphSettings().setCohRobustnessThreshold(DEFAULT_COH_ROBUSTNESS);
    settings.getGraphSettings().setEntitiesPerMentionConstraint(DEFAULT_SIZE);

    Disambiguator d = new Disambiguator(preparedInput, settings);

    DisambiguationResults results = d.disambiguate();

    Map<String, String> mappings = repackageMappings(results);

    String mapped = mappings.get("Page");
    assertEquals("Jimmy_Page", mapped);

    mapped = mappings.get("Kashmir");
    assertEquals("Kashmir_(song)", mapped);

    mapped = mappings.get("Knebworth");
    assertEquals("Knebworth_Festival", mapped);

    mapped = mappings.get("Les Paul");
    assertEquals(Entity.OOKBE, mapped);
  }
  
  @Test
  public void testNoMaxEntityRank() throws Exception {
    Preparator p = new Preparator();

    String docId = "testPageKashmir2";
    String content = "When [[Page]] played Kashmir at Knebworth, his Les Paul was uniquely tuned.";
    PreparationSettings prepSettings = new PreparationSettings();
    prepSettings.setMentionsFilter(FilterType.STANFORD_NER);
    prepSettings.setUseHybridMentionDetection(true);

    PreparedInput preparedInput = p.prepare(docId, content, new PreparationSettings());

    DisambiguationSettings settings = new CocktailPartyDisambiguationSettings();
    settings.getGraphSettings().setAlpha(DEFAULT_ALPHA);
    settings.getGraphSettings().setCohRobustnessThreshold(DEFAULT_COH_ROBUSTNESS);
    settings.getGraphSettings().setEntitiesPerMentionConstraint(DEFAULT_SIZE);
    settings.setMaxEntityRank(-0.1);

    Disambiguator d = new Disambiguator(preparedInput, settings);

    DisambiguationResults results = d.disambiguate();

    Map<String, String> mappings = repackageMappings(results);

    String mapped = mappings.get("Page");
    assertEquals(Entity.OOKBE, mapped);

    mapped = mappings.get("Kashmir");
    assertEquals(Entity.OOKBE, mapped);

    mapped = mappings.get("Knebworth");
    assertEquals(Entity.OOKBE, mapped);

    mapped = mappings.get("Les Paul");
    assertEquals(Entity.OOKBE, mapped);
  }
  
  @Test
  public void testTopMaxEntityRank() throws Exception {
    Preparator p = new Preparator();

    String docId = "testPageKashmir3";
    String content = "When [[Page]] played Kashmir at Knebworth, his Les Paul was uniquely tuned.";
    PreparationSettings prepSettings = new PreparationSettings();
    prepSettings.setMentionsFilter(FilterType.STANFORD_NER);
    prepSettings.setUseHybridMentionDetection(true);

    PreparedInput preparedInput = p.prepare(docId, content, new PreparationSettings());

    DisambiguationSettings settings = new CocktailPartyDisambiguationSettings();
    settings.getGraphSettings().setAlpha(DEFAULT_ALPHA);
    settings.getGraphSettings().setCohRobustnessThreshold(DEFAULT_COH_ROBUSTNESS);
    settings.getGraphSettings().setEntitiesPerMentionConstraint(DEFAULT_SIZE);
    settings.setMaxEntityRank(0.8);

    Disambiguator d = new Disambiguator(preparedInput, settings);

    DisambiguationResults results = d.disambiguate();

    Map<String, String> mappings = repackageMappings(results);

    String mapped = mappings.get("Page");
    assertEquals("Jimmy_Page", mapped);

    mapped = mappings.get("Kashmir");
    assertEquals("Kashmir_(song)", mapped);

    mapped = mappings.get("Knebworth");
    assertEquals(Entity.OOKBE, mapped);

    mapped = mappings.get("Les Paul");
    assertEquals(Entity.OOKBE, mapped);
  }

  private Map<String, String> repackageMappings(DisambiguationResults results) {
    Map<String, String> repack = new HashMap<String, String>();

    for (ResultMention rm : results.getResultMentions()) {
      repack.put(rm.getMention(), results.getBestEntity(rm).getEntity());
    }

    return repack;
  }
}
