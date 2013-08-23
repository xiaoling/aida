package mpi.aida;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import mpi.aida.access.DataAccess;
import mpi.aida.config.settings.DisambiguationSettings;
import mpi.aida.config.settings.PreparationSettings;
import mpi.aida.config.settings.disambiguation.CocktailPartyDisambiguationSettings;
import mpi.aida.config.settings.disambiguation.CocktailPartyKOREDisambiguationSettings;
import mpi.aida.config.settings.disambiguation.CocktailPartyKOREIDFDisambiguationSettings;
import mpi.aida.config.settings.disambiguation.CocktailPartyKORELSHDisambiguationSettings;
import mpi.aida.config.settings.disambiguation.LocalDisambiguationIDFSettings;
import mpi.aida.config.settings.disambiguation.LocalDisambiguationSettings;
import mpi.aida.config.settings.disambiguation.PriorOnlyDisambiguationSettings;
import mpi.aida.config.settings.preparation.StanfordHybridPreparationSettings;
import mpi.aida.data.DisambiguationResults;
import mpi.aida.data.EntityMetaData;
import mpi.aida.data.PreparedInput;
import mpi.aida.data.ResultMention;
import mpi.aida.data.ResultProcessor;
import mpi.aida.util.htmloutput.HtmlGenerator;

/**
 * Disambiguates a document using the default PRIOR, LOCAL or GRAPH settings,
 * callable from the Command Line.
 *
 */
public class CommandLineDisambiguator {

  public static void main(String[] args) throws Exception {
    if (args.length != 2) {
      printUsage();
      System.exit(1);
    }

    String disambiguationTechniqueSetting = args[0];  
    String inputFile = args[1];
    File text = new File(inputFile);

    BufferedReader reader = 
        new BufferedReader(
            new InputStreamReader(new FileInputStream(text), "UTF-8"));
    StringBuilder content = new StringBuilder();

    for (String line = reader.readLine(); line != null; 
        line = reader.readLine()) {
      content.append(line).append('\n');
    }
    reader.close();

    PreparationSettings prepSettings = new StanfordHybridPreparationSettings();
    Preparator p = new Preparator();
    PreparedInput input = 
        p.prepare(content.toString(), prepSettings);

    DisambiguationSettings disSettings = null;   
    if (disambiguationTechniqueSetting.equals("PRIOR")) {
      disSettings = new PriorOnlyDisambiguationSettings();
    } else if (disambiguationTechniqueSetting.equals("LOCAL")) {
      disSettings = new LocalDisambiguationSettings();
    } else if (disambiguationTechniqueSetting.equals("LOCAL-IDF")) {
      disSettings = new LocalDisambiguationIDFSettings();
    } else if (disambiguationTechniqueSetting.equals("GRAPH")) {
      disSettings = new CocktailPartyDisambiguationSettings();
    } else if (disambiguationTechniqueSetting.equals("GRAPH-IDF")) {
      disSettings = new CocktailPartyKOREIDFDisambiguationSettings();
    } else if (disambiguationTechniqueSetting.equals("GRAPH-KORE")) {
      disSettings = new CocktailPartyKOREDisambiguationSettings();
    } else if (disambiguationTechniqueSetting.equals("GRAPH-KORELSH")) {
      disSettings = new CocktailPartyKORELSHDisambiguationSettings();
    } else {
      System.err.println(
          "disambiguation-technique can be either: " +
          "'PRIOR', 'LOCAL', 'LOCAL-IDF', 'GRAPH', 'GRAPH-IDF' or 'GRAPH-KORE");
      System.exit(2);
    }    
    
    Disambiguator d = new Disambiguator(input, disSettings);
    DisambiguationResults results = d.disambiguate();
 
    
    // retrieve JSON representation of Disambiguated results
    ResultProcessor rp = new ResultProcessor(content.toString(), results, inputFile, input);
    String jsonStr = rp.process();
    System.out.println(jsonStr);
    // generate HTML from Disambiguated Results
    HtmlGenerator gen = new HtmlGenerator(content.toString(), jsonStr, inputFile, input);
    String htmlContent = gen.constructFromJson(jsonStr);
    String resultFile = inputFile+".html";
    BufferedWriter writer = 
        new BufferedWriter(new OutputStreamWriter(
            new FileOutputStream(resultFile), "UTF-8"));
    writer.write(htmlContent);
    writer.flush();
    writer.close();
    
    System.out.println("Disambiguation for '" + inputFile + "' done, " +
    		"result written to '" + resultFile + '"');
    
    System.out.println("Mentions and Entities found:");
    System.out.println("\tMention\tEntity_id\tEntity\tEntity Name\tURL");
    
    Set<String> entities = new HashSet<String>();
    for (ResultMention rm : results.getResultMentions()) {
      entities.add(results.getBestEntity(rm).getEntity());
    }
    Map<String, EntityMetaData> entitiesMetaData = DataAccess.getEntitiesMetaData(entities);
    
    for (ResultMention rm : results.getResultMentions()) {
      String entity = results.getBestEntity(rm).getEntity();
      EntityMetaData entityMetaData = entitiesMetaData.get(entity);

      System.out.println("\t" + rm + "\t" + entityMetaData.getId() + "\t" + entity + "\t" + entityMetaData.getHumanReadableRepresentation() + "\t"
          + entityMetaData.getUrl());
    }
  }

  private static void printUsage() {
    System.out.println("Usage:\n CommandLineDisambiguator " +
    		"<disambiguation-technique> <input-file.txt>");
    System.out.println("\tdisambiguation-technique: " +
    		"PRIOR, LOCAL, LOCAL-IDF, GRAPH, GRAPH-IDF, or GRAPH-KORE");
  }
}
