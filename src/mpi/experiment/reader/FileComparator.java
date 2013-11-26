package mpi.experiment.reader;

import java.io.File;
import java.util.Comparator;


public class FileComparator implements Comparator<File> {
  boolean convertToInt_;
  public FileComparator(boolean convertToInt) {
    convertToInt_ = convertToInt;
  }
  
  @Override
  public int compare(File a, File b) {
    if (convertToInt_) {
      Integer aInt = Integer.parseInt(a.getName().replace(".tsv", ""));
      Integer bInt = Integer.parseInt(b.getName().replace(".tsv", ""));
      return aInt.compareTo(bInt);
    } else {
      return a.getName().compareTo(b.getName());
    }
  }
}
