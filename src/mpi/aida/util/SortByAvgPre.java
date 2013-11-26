package mpi.aida.util;

import java.util.Comparator;
import java.util.HashMap;

public class SortByAvgPre implements Comparator<String> {

  private HashMap<String, String> idsAvgPrec = null;

  public SortByAvgPre(HashMap<String, String> idsAvgPrec) {
    this.idsAvgPrec = idsAvgPrec;
  }

  @Override
  public int compare(String o1, String o2) {
    if (idsAvgPrec.get(o1) == null && idsAvgPrec.get(o2) == null) {
      return 0;
    } else if (idsAvgPrec.get(o1) == null) {
      return 1;
    } else if (idsAvgPrec.get(o2) == null) {
      return -1;
    }
    Double first = Double.parseDouble(idsAvgPrec.get(o1));
    Double second = Double.parseDouble(idsAvgPrec.get(o2));
    return second.compareTo(first);
  }
}
