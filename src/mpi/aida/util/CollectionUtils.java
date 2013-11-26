package mpi.aida.util;

import gnu.trove.iterator.TIntDoubleIterator;
import gnu.trove.iterator.TObjectDoubleIterator;
import gnu.trove.map.hash.TIntDoubleHashMap;
import gnu.trove.map.hash.TObjectDoubleHashMap;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;

import org.apache.commons.lang.StringUtils;


public class CollectionUtils {
  public static <K, V extends Comparable<? super V>> LinkedHashMap<K, V> sortMapByValue(Map<K, V> map) {
    return sortMapByValue(map, false);
  }
  
  public static <K, V extends Comparable<? super V>> LinkedHashMap<K, V> sortMapByValue(Map<K, V> map, final boolean descending) {
    List<Map.Entry<K, V>> list = new LinkedList<Map.Entry<K, V>>(map.entrySet());
    Collections.sort(list, new Comparator<Map.Entry<K, V>>() {

      public int compare(Map.Entry<K, V> o1, Map.Entry<K, V> o2) {
        int comp = (o1.getValue()).compareTo(o2.getValue());
        
        if (descending) {
          comp = comp * (-1);
        }
        
        return comp;
      }
    });

    LinkedHashMap<K, V> result = new LinkedHashMap<K, V>();
    for (Map.Entry<K, V> entry : list) {
      result.put(entry.getKey(), entry.getValue());
    }
    return result;
  }
  
  public static Map<String, Double> getWeightStringsAsMap(List<String[]> weightStrings) {
    Map<String, Double> weights = new HashMap<String, Double>();
    for (String[] s : weightStrings) {
      String name = s[0];
      Double weight = Double.parseDouble(s[1]);
      weights.put(name, weight);
    }
    return weights;
  }
  
   /**
   * Normalizes scores by summing up. First shifts so that the min is 0 if
   * it was negative.
   * 
   * @param scores
   * @return
   */
  public static <T> Map<T, Double> shiftAndNormalizeScores(Map<T, Double> scores) {
    Double min = Double.MAX_VALUE;
    for (Double s : scores.values()) {
      if (s < min) {
        min = s;
      }
    }
    Map<T, Double> shiftScores = new HashMap<T, Double>(scores);
    if (min < 0) {
      for (T key : scores.keySet()) {
        Double prevScore = scores.get(key);
        Double shiftScore = prevScore - min;
        shiftScores.put(key, shiftScore);
      }
    }    
    return normalizeScores(shiftScores);
  }
  
  public static <T> Map<T, Double> normalizeScores(Map<T, Double> scores) {
    Map<T, Double> normalizedScores = new HashMap<T, Double>();
    double total = 0;
    for (Double d : scores.values()) {
      total += d;
    }
    if (total == 0) {
      return scores;
    }
    for (Entry<T, Double> entry : scores.entrySet()) {
      Double normalizedScore = entry.getValue() / total;
      normalizedScores.put(entry.getKey(), normalizedScore);
    }
    return normalizedScores;
  }
  
  public static TIntDoubleHashMap normalizeScores(TIntDoubleHashMap scores) {
    TIntDoubleHashMap normalizedScores = new TIntDoubleHashMap();
    double total = 0;
    for (TIntDoubleIterator itr = scores.iterator(); itr.hasNext(); ) {
      itr.advance();
      total += itr.value();
    }
    if (total == 0) {
      return scores;
    }
    for (TIntDoubleIterator itr = scores.iterator(); itr.hasNext(); ) {
      itr.advance();
      Double normalizedScore = itr.value() / total;
      normalizedScores.put(itr.key(), normalizedScore);
    }
    return normalizedScores;
  }
  
  public static <T> TObjectDoubleHashMap<T> normalizeScores(TObjectDoubleHashMap<T> scores) {
    TObjectDoubleHashMap<T> normalizedScores = new TObjectDoubleHashMap<T>();
    double total = 0;
    for (TObjectDoubleIterator<T> itr = scores.iterator(); itr.hasNext(); ) {
      itr.advance();
      total += itr.value();
    }
    if (total == 0) {
      return scores;
    }
    for (TObjectDoubleIterator<T> itr = scores.iterator(); itr.hasNext(); ) {
      itr.advance();
      Double normalizedScore = itr.value() / total;
      normalizedScores.put(itr.key(), normalizedScore);
    }
    return normalizedScores;
  }
  
  /**
   * Groups the input data by the specified column. Will first sort then
   * group.
   * 
   * @param input Rows of data.
   * @param groupingRange Position of data to group by.
   * @return  Data grouped by position. Key is <TAB> joined data.
   */
  public static Map<String, List<String[]>> groupData(
      List<String[]> input, final int[] groupingRange) {
    Map<String, List<String[]>> grouped = new HashMap<String, List<String[]>>();
    if (input.size() == 0) {
      return grouped;
    }
    
    // Sort the input by the grouping position.
    Collections.sort(input, new StringArrayComparator(groupingRange));
    
    // Iterate and group.
    String[] first = input.get(0);
    String[] group = TsvUtils.getElementsInRange(first, groupingRange);
    List<String[]> part = new ArrayList<String[]>();
    grouped.put(StringUtils.join(group, "\t"), part);
    StringArrayComparator comp = new StringArrayComparator(new int[] { 0, group.length - 1 });
    for (String[] row : input) {
      String[] currentGroup = TsvUtils.getElementsInRange(row, groupingRange);
      String[] currentData = TsvUtils.getElementsNotInRange(row, groupingRange);
      if (comp.compare(group, currentGroup) != 0) {
        part = new ArrayList<String[]>();
        grouped.put(StringUtils.join(currentGroup, "\t"), part);
        group = currentGroup;
      }
      part.add(currentData);
    }    
    return grouped;
  }
  
  static class StringArrayComparator implements Comparator<String[]> {
    private int[] groupingRange_;
    
    public StringArrayComparator(int[] groupingRange) {
      groupingRange_ = groupingRange;
    }
    
    public int compare(String[] a, String[] b) {
      String[] aComp = TsvUtils.getElementsInRange(a, groupingRange_);
      String[] bComp = TsvUtils.getElementsInRange(b, groupingRange_);
      for (int i = 0; i < aComp.length; ++i) {
        int comp = aComp[i].compareTo(bComp[i]);
        if (comp != 0) {
          return comp;
        }
      }
      // Everythin is equal.
      return 0;
    }
  }
  
  /**
   * Selects an element from elements according to the probability distribution
   * given by upperBounds. 
   * 
   * @param elements  Elements to choose from.
   * @param upperBounds Probability distribution given by the upper bounds of 
   *                    the interval [0.0, 1.0]. 1.0 must be included.
   * @param rand  Random object
   * @return  Randomly selected element.
   */
  public static <T> T getConditionalElement(
      T[] elements, double[] upperBounds, Random rand) {
    assert elements.length == upperBounds.length;
    double r = rand.nextDouble();
    // Do binary search to get the bound
    int i = bisectSearch(upperBounds, r);
    return elements[i];
  }
  
  public static int bisectSearch(double[] bounds, double x) {
    int l = 0;
    int r = bounds.length - 1;
    while (l < r) {
      int mid = (l + r) / 2;
      if (x < bounds[mid]) {
        r = mid;
      } else {
        l = mid + 1;
      }
    }
    return l;
  }

  /**
   * Convenience method for the call above.
   * 
   * @param elementProbabilities Map with elements as keys and their 
   *  probabilities as values. Values are expected to sum up to 1.
   * @param rand  Random generator to use.
   * @return  Randomly selected element according to probabilities.
   */
  public static Integer getConditionalElement(
      TIntDoubleHashMap elementProbabilities, Random rand) {
    Integer[] elements = new Integer[elementProbabilities.size()];
    double[] probs = new double[elementProbabilities.size()];
    double currentProb = 0.0;
    int i = 0;
    for (TIntDoubleIterator itr = elementProbabilities.iterator(); itr.hasNext(); ) {
      itr.advance();
      elements[i] = itr.key();
      currentProb += itr.value();
      probs[i] = currentProb;
      ++i;
    }
    return getConditionalElement(elements, probs, rand);
  }
}
