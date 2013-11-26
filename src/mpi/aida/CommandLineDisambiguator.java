package mpi.aida;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import mpi.aida.access.DataAccess;
import mpi.aida.config.settings.DisambiguationSettings;
import mpi.aida.config.settings.JsonSettings.JSONTYPE;
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
import mpi.aida.data.Entities;
import mpi.aida.data.EntityMetaData;
import mpi.aida.data.PreparedInput;
import mpi.aida.data.ResultMention;
import mpi.aida.data.ResultProcessor;
import mpi.aida.util.htmloutput.HtmlGenerator;
import mpi.tools.javatools.util.FileUtils;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.MissingOptionException;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;

/**
 * Disambiguates a document from the command line.
 *
 */
public class CommandLineDisambiguator {
  
  private Options commandLineOptions;
  
  public static void main(String[] args) throws Exception {
    new CommandLineDisambiguator().run(args);
  }

  public void run(String args[]) throws Exception {
    commandLineOptions = buildCommandLineOptions();
    CommandLineParser parser = new PosixParser();
    CommandLine cmd = null;
    try {
      cmd = parser.parse(commandLineOptions, args); 
    } catch (MissingOptionException e) {
      System.out.println("\n\n" + e + "\n\n");
      printHelp(commandLineOptions);
    }
    if (cmd.hasOption("h")) {
      printHelp(commandLineOptions);
    }
    
    String disambiguationTechniqueSetting = "GRAPH";
    if (cmd.hasOption("t")) {
      disambiguationTechniqueSetting = cmd.getOptionValue("t");
    }
    String input = cmd.getOptionValue("i");
    File inputFile = new File(input); 
    List<File> files = new ArrayList<File>();
    if (cmd.hasOption("d")) {
      if (!inputFile.isDirectory()) {
        System.out.println("\n\nError: expected " + input + " to be a directory.");
        printHelp(commandLineOptions);
      }
      for (File f : FileUtils.getAllFiles(inputFile)) {
        if (f.getName().endsWith(".txt")) {
          files.add(f);
        }
      }
    } else {
      if (inputFile.isDirectory()) {
        System.out.println("\n\nError: expected " + input + " to be a file.");
        printHelp(commandLineOptions);
      }
      files.add(inputFile);
    }
    String outputFormat = "HTML";
    if (cmd.hasOption("o")) {
      outputFormat = cmd.getOptionValue("o");
    }
    PreparationSettings prepSettings = new StanfordHybridPreparationSettings();
    if (cmd.hasOption('m')) {
      int minCount = Integer.parseInt(cmd.getOptionValue('m'));
      prepSettings.setMinMentionOccurrenceCount(minCount);
      System.out.println("Dropping mentions with less than " + minCount + " occurrences in the text.");
    }
    Preparator p = new Preparator();
    
    int threadCount = 1;
    if (cmd.hasOption("c")) {
      threadCount = Integer.parseInt(cmd.getOptionValue("c"));
    }    
    int resultCount = 10;
    if (cmd.hasOption("e")) {
      resultCount = Integer.parseInt(cmd.getOptionValue("e"));
    }    
    ExecutorService es = Executors.newFixedThreadPool(threadCount);
    System.out.println("Processing " + files.size() + " documents with " +
        threadCount + " threads.");
    for (File f : files) {
      Processor proc = new Processor(f.getAbsolutePath(), 
          disambiguationTechniqueSetting, p, prepSettings, outputFormat, resultCount,
          !inputFile.isDirectory());   
      es.execute(proc);
    }
    es.shutdown();
    es.awaitTermination(1, TimeUnit.DAYS);
  } 

  @SuppressWarnings("static-access")
  private Options buildCommandLineOptions() throws ParseException {
    Options options = new Options();
    options
        .addOption(OptionBuilder
            .withLongOpt("input")
            .withDescription(
                "Input, assumed to be a UTF-8 encoded text file. "
                + "Set -d to treat  the parameter as directory.")
            .hasArg()
            .withArgName("FILE")
            .isRequired()
            .create("i"));
    options
    .addOption(OptionBuilder
        .withLongOpt("directory")
        .withDescription(
            "Set to treat the -i input as directory. Will recursively process"
            + "all .txt files in the directory.")
        .create("d"));
    options
        .addOption(OptionBuilder
            .withLongOpt("technique")
            .withDescription(
                "Set the disambiguation-technique to be used: PRIOR, LOCAL, LOCAL-IDF, GRAPH, GRAPH-IDF, or GRAPH-KORE. Default is GRAPH.")
            .hasArg()
            .withArgName("TECHNIQUE")
            .create("t"));
    options
    .addOption(OptionBuilder
        .withLongOpt("outputformat")
        .withDescription(
            "Set the output-format to be used: HTML or JSON. Default is HTML.")
        .hasArg()
        .withArgName("FORMAT")
        .create("o"));    
    options
    .addOption(OptionBuilder
        .withLongOpt("threadcount")
        .withDescription(
            "Set the number of documents to be processed in parallel.")
        .hasArg()
        .withArgName("COUNT")
        .create("c"));    
    options
    .addOption(OptionBuilder
        .withLongOpt("minmentioncount")
        .withDescription(
            "Set the minimum occurrence count of a mention to be considered for disambiguation. Default is 1.")
        .hasArg()
        .withArgName("COUNT")
        .create("m"));  
    options
    .addOption(OptionBuilder
        .withLongOpt("maximumresultspermention")
        .withDescription(
            "Set the number of entities returned per mention in the output. Default is 10.")
        .hasArg()
        .withArgName("COUNT")
        .create("e"));  
    options.addOption(OptionBuilder.withLongOpt("help").create('h'));
    return options;
  }
  
  private void printHelp(Options commandLineOptions) {
    String header = "\n\nRun AIDA on a UTF-8 encoded input:\n\n";
    HelpFormatter formatter = new HelpFormatter();
    formatter.printHelp("CommandLineDisambiguator", header, 
        commandLineOptions, "", true);
    System.exit(0);
  }
  
  class Processor implements Runnable {
    private String inputFile; 
    private String disambiguationTechniqueSetting; 
    private Preparator p; 
    private PreparationSettings prepSettings;
    private String outputFormat;
    private int resultCount;
    private boolean logResults;
    
    public Processor(String inputFile, String disambiguationTechniqueSetting,
        Preparator p, PreparationSettings prepSettings, String outputFormat,
        int resultCount, boolean logResults) {
      super();
      this.inputFile = inputFile;
      this.disambiguationTechniqueSetting = disambiguationTechniqueSetting;
      this.p = p;
      this.prepSettings = prepSettings;
      this.outputFormat = outputFormat;
      this.resultCount = resultCount;
      this.logResults = logResults;
    }    
    
    @Override
    public void run() {
      try {
        BufferedReader reader = new BufferedReader(new InputStreamReader(
            new FileInputStream(inputFile), "UTF-8"));
        StringBuilder content = new StringBuilder();

        for (String line = reader.readLine(); line != null; line = reader
            .readLine()) {
          content.append(line).append('\n');
        }
        reader.close();
        
        PreparedInput input = p.prepare(inputFile, content.toString(), prepSettings);
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
          System.err
              .println("disambiguation-technique can be either: "
                  + "'PRIOR', 'LOCAL', 'LOCAL-IDF', 'GRAPH', 'GRAPH-IDF' or 'GRAPH-KORE");
          System.exit(2);
        }

        Disambiguator d = new Disambiguator(input, disSettings);
        DisambiguationResults results = d.disambiguate();

        // retrieve JSON representation of Disambiguated results
        ResultProcessor rp = new ResultProcessor(content.toString(), results,
            inputFile, input, resultCount);
        //String jsonStr = rp.process(false);
        String jsonStr = rp.process(JSONTYPE.EXTENDED);
        String resultFile = null;
        String resultContent = null;
        if (outputFormat.equals("JSON")) {
          resultFile = inputFile + ".json";
          resultContent = jsonStr;
        } else if (outputFormat.equals("HTML")) {
          resultFile = inputFile + ".html";
          // generate HTML from Disambiguated Results
          HtmlGenerator gen = new HtmlGenerator(content.toString(), jsonStr,
              inputFile, input);
          resultContent = gen.constructFromJson(jsonStr);
        } else {
          System.out.println("Unrecognized output format.");
          printHelp(commandLineOptions);
        }
        BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(
            new FileOutputStream(resultFile), "UTF-8"));
        writer.write(resultContent);
        writer.flush();
        writer.close();
        
        if (logResults) {
          System.out.println("Disambiguation for '" + inputFile + "' done, "
              + "result written to '" + resultFile + "' in " + outputFormat);
          
          System.out.println("Mentions and Entities found:");
          System.out.println("\tMention\tEntity_id\tEntity\tEntity Name\tURL");
  
          Set<String> entities = new HashSet<String>();
          for (ResultMention rm : results.getResultMentions()) {
            entities.add(results.getBestEntity(rm).getEntity());
          }
          Map<String, EntityMetaData> entitiesMetaData = DataAccess
              .getEntitiesMetaData(entities);
  
          for (ResultMention rm : results.getResultMentions()) {
            String entity = results.getBestEntity(rm).getEntity();
            EntityMetaData entityMetaData = entitiesMetaData.get(entity);
  
            if (Entities.isOokbEntity(entity)) {
              System.out.println("\t" + rm + "\t NO MATCHING ENTITY");
            } else {
              System.out.println("\t" + rm + "\t" + entityMetaData.getId() + "\t"
                + entity + "\t" + entityMetaData.getHumanReadableRepresentation()
                + "\t" + entityMetaData.getUrl());
            }
          }  
        }
      } catch (Exception e) {
        System.err.println("Error while processing '" + inputFile + "': " + 
            e.getLocalizedMessage());
        e.printStackTrace();
      }
    }    
  }
}
