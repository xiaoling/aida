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
import java.util.Map.Entry;

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
  
  private String[] docIds = null;
  
  public AidaSingleFormatCollectionReader(String collectionPath) throws IOException {
    super(collectionPath, CollectionPart.ALL, new CollectionReaderSettings());
    init();
  }
  
  public AidaSingleFormatCollectionReader(String collectionPath, CollectionPart cp, CollectionReaderSettings settings) throws IOException {
    super(collectionPath, cp, settings);
    init();
  }

  public AidaSingleFormatCollectionReader(String collectionPath, int from, int to, CollectionReaderSettings settings) throws IOException {
    super(collectionPath, from, to, settings);
    init();
  }
  
  public AidaSingleFormatCollectionReader(String collectionPath, String docIds, CollectionReaderSettings settings) throws IOException {
    super(collectionPath, 0, 0, settings);
    this.docIds = docIds.split(",");
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
    if(docIds == null){
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
    }else{
      for(File aidaFile : aidaFiles){
        String fName = aidaFile.getName();
        fName = fName.substring(0, fName.indexOf(".tsv"));
        for(String docId : docIds){
          if(docId.equalsIgnoreCase(fName)){
            PreparedInput doc = getPreparedInputForFile(aidaFile);
            if (docs % 1000 == 0) {
              logger_.debug("Read " + docs + " docs.");
            }
            docIds_.add(doc.getDocId());
            documents_.put(doc.getDocId(), doc);
            break;
          }
        }
      }
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
    PreparedInput p = documents_.get(docId);    
    assert p.getChunksCount() == 1;
    return documents_.get(docId).iterator().next().getContext();
  }

  @Override
  public int collectionSize() {
    return docIds_.size();
  }

  @Override
  public String getText(String docId) {
    return getDocumentContext(docId).toString();
  }
  
  public boolean usesIntegerDocIds() {
    return false;
  }  

  @Override
  protected int[] getCollectionPartFromTo(CollectionPart cp) {
    return new int[] {0, 0};
  }

  public Comparator<File> getComparator(boolean convertToInt) {
    return new FileComparator(convertToInt);
  }
  
  public Map<String, String> getTextMap() {
    Map<String, String> texts = new HashMap<String, String>();
    for (Entry<String, PreparedInput> e : documents_.entrySet()) {
      texts.put(e.getKey(), e.getValue().getTokens().toText());
    }
    return texts;
  }
}
