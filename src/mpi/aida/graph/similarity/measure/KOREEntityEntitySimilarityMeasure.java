package mpi.aida.graph.similarity.measure;

import gnu.trove.TIntCollection;
import gnu.trove.list.linked.TIntLinkedList;
import gnu.trove.map.hash.TIntObjectHashMap;
import gnu.trove.set.hash.TIntHashSet;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import mpi.aida.data.Entity;
import mpi.aida.graph.similarity.context.EntitiesContext;
import mpi.aida.graph.similarity.context.FastWeightedKeyphrasesContext;
import mpi.aida.util.CollectionUtils;
import mpi.experiment.trace.NullEntityEntityTracing;
import mpi.experiment.trace.Tracer;
import mpi.experiment.trace.measures.KeytermEntityEntityMeasureTracer;
import mpi.experiment.trace.measures.TermTracer;

public class KOREEntityEntitySimilarityMeasure
		extends EntityEntitySimilarityMeasure {

	public KOREEntityEntitySimilarityMeasure(
			Tracer tracer) {
		super(tracer);
	}

	@Override
  public double calcSimilarity(Entity a, Entity b, EntitiesContext context) {	  
    FastWeightedKeyphrasesContext kwc = (FastWeightedKeyphrasesContext) context;

    // generate keyphrase pairs that intersect
    TIntObjectHashMap<TIntHashSet> overlapping = new TIntObjectHashMap<TIntHashSet>();
    for (int t : intersect(kwc.getKeywordArray(a),
        kwc.getKeywordArray(b))) {
      for (int kpA : kwc.getKeyphrasesForKeyword(a, t)) {
        for (int kpB : kwc.getKeyphrasesForKeyword(b, t)) {
          if (!overlapping.contains(kpA)) {
            overlapping.put(kpA, new TIntHashSet());
          }
          overlapping.get(kpA).add(kpB);
        }
      }
    }

    double n = .0;
    
    // tracing
    Map<Integer, TermTracer> matchesA = new HashMap<Integer, TermTracer>();
    Map<Integer, TermTracer> matchesB = new HashMap<Integer, TermTracer>();

    // iterate over overlapping phrase pairs
    for (int kpA : overlapping.keys()) {
      for (int kpB : overlapping.get(kpA).toArray()) {
        double psimn = .0; //, psimd = .0;
        TermTracer tt = new TermTracer();
        for (int t : intersect(kwc.getKeyphraseTokenIds(kpA, true),
            kwc.getKeyphraseTokenIds(kpB, true))) {
          double kwWeight = Math.min(
              kwc.getCombinedKeywordMiIdfWeight(a, t),
              kwc.getCombinedKeywordMiIdfWeight(b, t));
          psimn += kwWeight;
          // Requires too much main memory, enable if needed.
          // if (kwWeight > 0.0) {
          //  tt.addInnerMatch(t, kwWeight);
          //}
        }
        
        double kpASourceWeight = kwc.getKeyphraseSourceWeight(a, kpA);
        double kpBSourceWeight = kwc.getKeyphraseSourceWeight(b, kpB);
        double kpWeight = 
            Math.min(kpASourceWeight * kwc.getCombinedKeyphraseMiIdfWeight(a, kpA),
                     kpBSourceWeight * kwc.getCombinedKeyphraseMiIdfWeight(b, kpB));
        double psimd = kwc.getKeywordWeightSum(a, kpA) + kwc.getKeywordWeightSum(b, kpB) - psimn;
        if (psimd != 0.0) {
          double kpJaccardSim = (psimn / psimd);        
          double matchWeight = kpWeight * Math.pow(kpJaccardSim, 2);   
          
          n += matchWeight;
          
          // Tracing.
          if (matchWeight > 0) {
            matchesA.put(kpA, tt);
            matchesB.put(kpB, tt);
            tt.setTermWeight(matchWeight);
          }
        }
      }
    }
    
    double denom = 0.0;
    int[] kpsA = kwc.getEntityKeyphraseIds(a);
    for (int kp : kpsA) {
      denom += kwc.getKeyphraseSourceWeight(a, kp) * kwc.getCombinedKeyphraseMiIdfWeight(a, kp);
    }
    
    int[] kpsB = kwc.getEntityKeyphraseIds(b);
    for (int kp : kpsB) {
      denom += kwc.getKeyphraseSourceWeight(b, kp) * kwc.getCombinedKeyphraseMiIdfWeight(b, kp);
    }
    
    if (!(tracer.eeTracing() instanceof NullEntityEntityTracing)) {
      collectTracingInfo(a, b, 
          kpsA, kpsB, n / denom, matchesA, matchesB, kwc);
    }
        
    double sim = 0.0;    
    if (denom > 0) {
      sim = n / denom;   
    }
    return sim;
  }
	
	protected double getOuterDenominator(double[] kpwA, double[] kpwB, double n) {
      return zipmax(kpwA, kpwB);
  }

  @SuppressWarnings("unused")
	private void printArray(int[] arr) {
		if (arr.length > 0) {
			System.out.print("[" + arr[0]);
			for (int i = 1; i < arr.length; ++i) {
				System.out.print(", " + arr[i]);
			}
			System.out.println("]");
		} else {
			System.out.println("[]");
		}
	}
	
	@SuppressWarnings("unused")
	private double sum(double[] a) {
		double s = .0;
		for (double d : a) {
			s += d;
		}
		return s;
	}

	protected int[] intersect(int[] aOrig, int[] bOrig) {
	  if (aOrig == null || aOrig.length == 0 || bOrig == null || bOrig.length == 0) {
	    return new int[0];
	  }
	 
    // TODO(jhoffart) make sure all arrays are duplicate-free and
    // sorted when passed, then drop the following steps.
	  TIntHashSet aSet = new TIntHashSet(aOrig);
	  TIntHashSet bSet = new TIntHashSet(bOrig);
	  int[] a = aSet.toArray();
	  int[] b = bSet.toArray();
    Arrays.sort(a);
    Arrays.sort(b);
    // Drop until above.
   
		TIntCollection is = new TIntLinkedList();
		int i = 0, j = 0;
		while (i < a.length && j < b.length) {
			if (a[i] == b[j]) {
				is.add(a[i]);
				++i;
				++j;
			} else if (a[i] < b[j]) {
				++i;
			} else {
				++j;
			}
		}
		return is.toArray();
	}

	@SuppressWarnings("unused")
	private int[] union(int[] a, int[] b) {
		TIntCollection u = new TIntLinkedList();
		int i = 0, j = 0, item = -1;
		while (i < a.length && j < b.length) {
			if (a[i] == b[j]) {
				item = a[i];
				while (i < a.length && a[i] == item) {
					++i;
				}
				while (j < b.length && b[j] == item) {
					++j;
				}
			} else if (a[i] < b[j]) {
				item = a[i++];
			} else {
				item = b[j++];
			}
			u.add(item);
		}
		while (i < a.length) {
			u.add(a[i++]);
		}
		while (j < b.length) {
			u.add(b[j++]);
		}
		return u.toArray();
	}

	private double zipmax(double[] a, double[] b) {
		double s = .0;
		int i = a.length - 1, j = b.length - 1;
		while (i >= 0 && j >= 0) {
			if (a[i] >= b[j]) {
				s += a[i--] * (j + 1);
			} else {
				s += b[j--] * (i + 1);
			}
		}
		return s;
	}

	@SuppressWarnings("unused")
	private double zipmin(double[] a, double[] b) {
		double s = .0;
		int i = 0, j = 0;
		while (i < a.length && j < b.length) {
			if (a[i] <= b[j]) {
				s += a[i++] * (b.length - j);
			} else {
				s += b[j++] * (a.length - i);
			}
		}
		return s;
	}
	
  private void collectTracingInfo(Entity a, Entity b, int[] kpsA, int[] kpsB, double sim, Map<Integer, TermTracer> matchesA, Map<Integer, TermTracer> matchesB, FastWeightedKeyphrasesContext kwc) {
    Map<Integer, Double> e1keyphrases = new HashMap<Integer, Double>();     
    for (int kp : kpsA) {
//      if (kwc.getCombinedKeyphraseMiIdfWeight(a, kp) > 0.0) {
        e1keyphrases.put(kp, kwc.getCombinedKeyphraseMiIdfWeight(a, kp));
//      }
    }     
    e1keyphrases = CollectionUtils.sortMapByValue(e1keyphrases, true);    

    Map<Integer, Double> e2keyphrases = new HashMap<Integer, Double>();
    for (int kp : kpsB) {
//      if (kwc.getCombinedKeyphraseMiIdfWeight(b, kp) > 0.0) {
        e2keyphrases.put(kp, kwc.getCombinedKeyphraseMiIdfWeight(b, kp));
//      }
    }  
    e2keyphrases = CollectionUtils.sortMapByValue(e2keyphrases, true);

    tracer.eeTracing().addEntityContext(a.getName(), e1keyphrases);
    tracer.eeTracing().addEntityContext(b.getName(), e2keyphrases);

    KeytermEntityEntityMeasureTracer mt = new KeytermEntityEntityMeasureTracer("PartialKeyphraseSim", 0.0, e2keyphrases, matchesB, kwc.getAllKeyphraseTokens());
    mt.setScore(sim);
    tracer.eeTracing().addEntityEntityMeasureTracer(a.getName(), b.getName(), mt);

    KeytermEntityEntityMeasureTracer mt2 = new KeytermEntityEntityMeasureTracer("PartialKeyphraseSim", 0.0, e1keyphrases, matchesA, kwc.getAllKeyphraseTokens());
    mt2.setScore(sim);
    tracer.eeTracing().addEntityEntityMeasureTracer(b.getName(), a.getName(), mt2);
  }
}