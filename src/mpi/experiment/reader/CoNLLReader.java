package mpi.experiment.reader;

import java.io.File;

public class CoNLLReader extends AidaFormatCollectionReader {

  public final static String finalFileName = File.separator + "CoNLL-YAGO.tsv";

  public final static String finalCollectionPath = "./data/experiment/CONLL";

  public CoNLLReader() {
    super(finalCollectionPath, finalFileName);
  }
  
  public CoNLLReader(String collectionPath) {
    super(collectionPath, finalFileName);
  }
  
  public CoNLLReader(CollectionPart cp) {
    super(finalCollectionPath, finalFileName, cp, new CollectionReaderSettings());
  }
  
  public CoNLLReader(String collectionPath, CollectionPart cp) {
    super(collectionPath, finalFileName, cp, new CollectionReaderSettings());
  }
  
  public CoNLLReader(CollectionPart cp, CollectionReaderSettings settings) {
    super(finalCollectionPath, finalFileName, cp, settings);
  }
  
  public CoNLLReader(String collectionPath, int from, int to) {
    this(collectionPath, from, to, new CollectionReaderSettings());
  }

  public CoNLLReader(String collectionPath, int from, int to, CollectionReaderSettings settings) {
    super(collectionPath, finalFileName, from, to, settings);
  }

  public CoNLLReader(String collectionPath, CollectionPart cp, CollectionReaderSettings settings) {
    super(collectionPath, finalFileName, cp, settings);
  }

  public CoNLLReader(String collectionPath, String docNums, CollectionReaderSettings settings) {
    super(collectionPath, finalFileName, docNums, settings);
  }

  @Override
  protected int[] getCollectionPartFromTo(CollectionPart cp) {
    int[] ft = new int[] { 1, 1393 };
    switch (cp) {
      case TRAIN:
        ft = new int[] { 1, 946 };
        break;
      case DEV:
        ft = new int[] { 947, 1162 };
        break;
      case DEV_SMALL:
        ft = new int[] { 947, 1046 };
        break;
      case TEST:
        ft = new int[] { 1163, 1393 };
        break;
      default:
        break;
    }
    return ft;
  }
}
