package mpi.aida;

import static org.junit.Assert.*;
import mpi.aida.config.AidaConfig;
import mpi.aida.config.settings.PreparationSettings;
import mpi.aida.config.settings.preparation.StanfordManualPreparationSettings;
import mpi.aida.data.PreparedInput;

import org.junit.Test;


public class AidaManagerTest {
  public AidaManagerTest() {
    AidaConfig.set("dataAccess", "testing");
    AidaConfig.set(AidaConfig.CACHE_WORD_DATA, "false");
    AidaManager.init();
  }
  
  @Test
  public void dropMentionsBelowOccurrenceCount() {
    String text = "[[one]] and [[two]] and [[two]] and [[three]] and [[three]] and [[three]]";
    PreparationSettings settings = new StanfordManualPreparationSettings();
    Preparator p = new Preparator();
    PreparedInput in = p.prepare(text, settings);
    assertEquals(6, in.getMentionSize());
    
    settings.setMinMentionOccurrenceCount(1);
    in = p.prepare(text, settings);
    assertEquals(6, in.getMentionSize());
    
    settings.setMinMentionOccurrenceCount(1);
    in = p.prepare(text, settings);
    assertEquals(6, in.getMentionSize());
    
    settings.setMinMentionOccurrenceCount(1);
    in = p.prepare(text, settings);
    assertEquals(6, in.getMentionSize());
    
    settings.setMinMentionOccurrenceCount(2);
    in = p.prepare(text, settings);
    assertEquals(5, in.getMentionSize());
    
    settings.setMinMentionOccurrenceCount(3);
    in = p.prepare(text, settings);
    assertEquals(3, in.getMentionSize());
    
    settings.setMinMentionOccurrenceCount(4);
    in = p.prepare(text, settings);
    assertEquals(0, in.getMentionSize());
  }
}
