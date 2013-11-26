package mpi.experiment.reader;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import mpi.aida.data.Context;
import mpi.aida.data.Mentions;
import mpi.aida.data.PreparedInput;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class AidaSingleFormatCollectionReader extends CollectionReader {
  private Logger logger_ = 
      LoggerFactory.getLogger(AidaSingleFormatCollectionReader.class);
    
  protected List<String> docIds_ = new ArrayList<String>();

  protected Map<String, PreparedInput> documents_ = new HashMap<String, PreparedInput>();
  
  public AidaSingleFormatCollectionReader(String collectionPath, CollectionPart cp, CollectionReaderSettings settings) throws IOException {
    super(collectionPath, cp, settings);
    init();
  }

  public AidaSingleFormatCollectionReader(String collectionPath, int from, int to, CollectionReaderSettings settings) throws IOException {
    super(collectionPath, from, to, settings);
    init();
  }
  
  private void init() throws IOException {
    logger_.info("Reading AIDA SingleFormat dataset from: " + collectionPath);
    int docs = 0;
    List<File> textDirectories = getAllTextDirectories();
    Collections.sort(textDirectories, new FileComparator(false));
    List<File> aidaFiles = new ArrayList<File>();
    for (File textDirectory : textDirectories) {
      for (File f : textDirectory.listFiles(new FilenameFilter() {        
        @Override public boolean accept(File f, String s) {
          return s.endsWith(".tsv");
        }})) {
        aidaFiles.add(f);
      }
    }
    Collections.sort(aidaFiles, new FileComparator(usesIntegerDocIds()));
    for (File aidaFile : aidaFiles)  {
      ++docs;
      if ((from > 0) && (docs < from)) {
        // Skip until from.
        continue;
      }
      if ((to > 0) && (docs > to)) {
        // Break after all docs have been read.
        break;
      }
      PreparedInput doc = getPreparedInputForFile(aidaFile);
      if (docs % 1000 == 0) {
        logger_.debug("Read " + docs + " docs.");
      }
      docIds_.add(doc.getDocId());
      documents_.put(doc.getDocId(), doc);
    }
    logger_.info("Read " + docIds_.size() + " docs.");
  }
  
  protected List<File> getAllTextDirectories() {
    List<File> textDir = new ArrayList<File>();
    textDir.add(new File(collectionPath + File.separator + "text"));
    return textDir;
  }

  protected PreparedInput getPreparedInputForFile(File aidaFile) {
    return new PreparedInput(aidaFile, settings.getMinMentionOccurrence(), settings.isIncludeOutOfDictionaryMentions());
  }

  @Override
  public Iterator<PreparedInput> iterator() {
    if (preparedInputs == null) {
      preparedInputs = new ArrayList<PreparedInput>(docIds_.size());
      for (String docId : docIds_) {
        preparedInputs.add(documents_.get(docId));
      }
    }
    return preparedInputs.iterator();
  }

  @Override
  public Mentions getDocumentMentions(String docId) {
    return documents_.get(docId).getMentions();
  }

  @Override
  public Context getDocumentContext(String docId) {
    return documents_.get(docId).getContext();
  }

  @Override
  public int collectionSize() {
    return docIds_.size();
  }

  @Override
  public String getText(String docId) {
    return documents_.get(docId).getContext().toString();
  }
  
  public boolean usesIntegerDocIds() {
    return false;
  }  

  @Override
  protected int[] getCollectionPartFromTo(CollectionPart cp) {
    return new int[] {0, docIds_.size() - 1 };
  }

  public Comparator<File> getComparator(boolean convertToInt) {
    return new FileComparator(convertToInt);
  }
  
  public static void main(String[] args) throws IOException {
    AidaSingleFormatCollectionReader r = new AidaSingleFormatCollectionReader("../aidaannotation/data/corpora/DNB/paragraphs_6_NE/Mono", 0, 10, new CollectionReaderSettings());
    for (PreparedInput p : r) {
      System.out.println(p.getDocId());
      System.out.println(p.getTokens().toText());
    }
  }
}
