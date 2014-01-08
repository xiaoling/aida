/**
 * 
 */
package mpi.aida.graph;

import gnu.trove.iterator.TIntDoubleIterator;
import gnu.trove.iterator.TObjectIntIterator;
import gnu.trove.list.TIntList;
import gnu.trove.list.array.TIntArrayList;
import gnu.trove.map.hash.TIntDoubleHashMap;
import gnu.trove.map.hash.TObjectDoubleHashMap;
import gnu.trove.map.hash.TObjectIntHashMap;
import gnu.trove.set.TIntSet;
import gnu.trove.set.hash.TIntHashSet;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import mpi.aida.data.Mention;

public class Graph {
	private String name;
	protected int nodesCount;
	private double alpha;
	
	private int edgesCount = 0;
	private int nextNodeId = 0;

	private GraphNode[] nodes;
	private TObjectIntHashMap<Mention> mentionNodesIds;
	private TObjectIntHashMap<String> entityNodesIds;
	/**
	 * Storage of all local similarities. This is usually the same as the weights
	 * on all mention-entity edges. However, when mention-entity edges are 
	 * dropped, e.g. when running the coherence robustness test, some weights will
	 * be missing. They are kept here.
	 */
	private Map<Mention, TObjectDoubleHashMap<String>> localSimilarities;
	private TIntDoubleHashMap mentionPriorSimL1;
	
	protected double averageMEweight = 1.0;
	protected double averageEEweight = 1.0;
	
	protected boolean[] isRemoved;
	protected int[] nodesOutdegrees;
	protected double[] nodesWeightedDegrees;
	
	/** 
	 * After the processing is finished, this will contain all removal steps
	 * from the very first (in the last position) to the last (in the first
	 * position). 
	 */
	protected TIntList removalSteps;	
	
	public Graph(String name, int nodesCount, double alpha) {
		this.name = name;
		this.nodesCount = nodesCount;
		this.alpha = alpha;
		
		nodes = new GraphNode[nodesCount];
		mentionNodesIds = new TObjectIntHashMap<Mention>();
		entityNodesIds = new TObjectIntHashMap<String>();
		localSimilarities = new HashMap<Mention, TObjectDoubleHashMap<String>>();
		mentionPriorSimL1 = new TIntDoubleHashMap(50, 0.5f, -1, -1.0);

		isRemoved = new boolean[nodesCount];
		Arrays.fill(isRemoved, false);
		nodesOutdegrees = new int[nodesCount];
		Arrays.fill(nodesOutdegrees, 0);
		nodesWeightedDegrees = new double[nodesCount];
		Arrays.fill(nodesWeightedDegrees, 0);
		
		removalSteps = new TIntArrayList();
	}
	
	public int getNodeOutdegree(int id) {
		return nodesOutdegrees[id];
	}
	
	public double getNodeWeightedDegrees(int id) {
		return nodesWeightedDegrees[id];
	}
	
	public boolean isRemoved(int id) {
		return isRemoved[id];
	}
	
	public void setRemoved(int id) {
		isRemoved[id] = true;
		removalSteps.add(id);
	}
	
	public TIntList getRemovalSteps() {
	  return removalSteps;
	}
	
	public void setIsRemovedFlags(boolean[] isRemoved) {
		this.isRemoved = isRemoved;
	}
	
	
	public void addEdge(Mention mention, String entity, double sim) {
		int id1 = mentionNodesIds.get(mention);
		int id2 = entityNodesIds.get(entity);
		addEdge(id1, id2, sim);
	}
	
	public void addEdge(String entity1, String entity2, double coh) {
		int id1 = entityNodesIds.get(entity1);
		int id2 = entityNodesIds.get(entity2);
		addEdge(id1, id2, coh);
	}
	
	
	public void addMentionNode(Mention mention) {
		GraphNode node = new GraphNode();
		node.setId(nextNodeId);
		node.setType(GraphNodeTypes.MENTION);
		node.setNodeData(mention);
		mentionNodesIds.put(mention, nextNodeId);
		nodes[nextNodeId] = node;
		
		nextNodeId++;
	}
	
	public void addEntityNode(String entity) {
		GraphNode node = new GraphNode();
		node.setId(nextNodeId);
		node.setType(GraphNodeTypes.ENTITY);
		node.setNodeData(entity);
		entityNodesIds.put(entity, nextNodeId);
		nodes[nextNodeId] = node;
		
		nextNodeId++;
	}
	
	public void addMentionPriorSimL1(Mention mention, double l1) {
	  if (!getMentionNodesIds().containsKey(mention)) {
	    throw new IllegalArgumentException(
	        "Mention '" + mention + "' does not exist in graph, make sure " +
	        "to add before adding L1.");
	  } else {
	    int mentionId = getMentionNodesIds().get(mention);
	    mentionPriorSimL1.put(mentionId, l1);
	  }	  
  }
	
	public double getMentionPriorSimL1(int mentionId) {
	  return mentionPriorSimL1.get(mentionId);
	}

  private void addEdge(int node1Id, int node2Id, double weight) {
		if(isEntityNode(node1Id) && isEntityNode(node2Id))
			weight = weight * (1-alpha);
		else if ((isMentionNode(node1Id) && isEntityNode(node2Id))
				|| (isEntityNode(node1Id) && isMentionNode(node2Id)))
			weight = weight * alpha;
				
		edgesCount++;
		
		GraphNode node1 = nodes[node1Id];
		GraphNode node2 = nodes[node2Id];
		
		node1.getSuccessors().put(node2Id, weight);
		nodesOutdegrees[node1Id]++;
		
		node2.getSuccessors().put(node1Id, weight);
		nodesOutdegrees[node2Id]++;
		
		nodesWeightedDegrees[node1Id] += weight;
		nodesWeightedDegrees[node2Id] += weight;		
	}
	
	public boolean isEntityNode(int nodeId) {
		if(nodes[nodeId].getType() == GraphNodeTypes.ENTITY)
			return true;
		else
			return false;
	}
	
	public boolean isMentionNode(int nodeId) {
		if(nodes[nodeId].getType() == GraphNodeTypes.MENTION)
			return true;
		else
			return false;
	}
	
	public int getNodesCount() {
		return nodesCount;
	}

	public long getEdgesCount() {
		return edgesCount;
	}
	
	public GraphNode[] getNodes() {
		return nodes;
	}
	
	public GraphNode getNode(int id) {
		return nodes[id];
	}

	
	public double getAverageMEweight() {
		return averageMEweight;
	}

	public void setAverageMEweight(double averageMEweight) {
		this.averageMEweight = averageMEweight;
	}

	public double getAverageEEweight() {
		return averageEEweight;
	}

	public void setAverageEEweight(double averageEEweight) {
		this.averageEEweight = averageEEweight;
	}

	public String getName() {
		return name;
	}

	public TObjectIntHashMap<Mention> getMentionNodesIds() {
		return mentionNodesIds;
	}
	
	public TObjectIntHashMap<String> getEntityNodesIds() {
	  return entityNodesIds;
  }
	
  public boolean isLocalMention(int mentionId) {
    for (int candidate : getNode(mentionId).getSuccessors().keys()) {
      for (int neighbor : getNode(candidate).getSuccessors().keys()) {
        if (getNode(neighbor).getType().equals(GraphNodeTypes.ENTITY)) {
          return false;
        }
      }
    }
    return true;
  }

  public void setMentionEntitySim(Map<Mention, TObjectDoubleHashMap<String>> localSimiliarites) {
    this.localSimilarities = localSimiliarites;
  }
  
  public void addMentionEntitySim(Mention mention, String entityName, double sim) {
    TObjectDoubleHashMap<String> sims = localSimilarities.get(mention);
    if (sims == null) {
      sims = new TObjectDoubleHashMap<String>();
      localSimilarities.put(mention, sims);
    }
    sims.put(entityName, sim);
  }
  
  public TObjectDoubleHashMap<String> getMentionEntitySims(Mention mention) {
    return localSimilarities.get(mention);
  }
  
  public Graph copyRemovingNodesAndEdges(
    final TIntSet nodesToRemove, 
    final Set<Edge> edgesToRemove) {
    Graph pruned = new Graph(
    getName(), getNodesCount() - nodesToRemove.size(), alpha);
    pruned.setAverageEEweight(getAverageEEweight());
    pruned.setAverageMEweight(getAverageMEweight());
    // Add all non-removed nodes plus mention-entity edges.
    TIntSet addedEntities = new TIntHashSet();
    for (TObjectIntIterator<Mention> itr = getMentionNodesIds().iterator(); 
        itr.hasNext(); ) {
      itr.advance();
      int mentionId = itr.value();
      if (!nodesToRemove.contains(mentionId)) {
        Mention m = itr.key();
        pruned.addMentionNode(m);
        GraphNode mentionNode = getNode(mentionId);     
        for (TIntDoubleIterator entityItr = mentionNode.getSuccessors().iterator(); 
            entityItr.hasNext(); ) {
          entityItr.advance();
          int entityId = entityItr.key();
          if (!nodesToRemove.contains(entityId)) {
            GraphNode entityNode = getNode(entityId);
            String entityName = (String) entityNode.getNodeData();
            if (!addedEntities.contains(entityId)) {
              pruned.addEntityNode(entityName);
              addedEntities.add(entityId);
            }
            Edge toAdd = new Edge(mentionId, entityId);
            if (!edgesToRemove.contains(toAdd)) {
              pruned.addEdge(m, entityName, entityItr.value());
              pruned.addMentionEntitySim(
                  m, entityName, 
                  getMentionEntitySims(m).get(entityName));
            }
          }
        }
      }
    }
    // Add all edges between entities which have not been removed.
    Set<Edge> addedEdges = new HashSet<Edge>();
    for (TObjectIntIterator<String> itr = getEntityNodesIds().iterator(); 
        itr.hasNext(); ) {
      itr.advance();
      int entityId = itr.value();
      if (!nodesToRemove.contains(entityId)) {
        String entityName = itr.key();
        GraphNode entityNode = getNode(entityId);
        for (TIntDoubleIterator entityItr = entityNode.getSuccessors().iterator(); 
            entityItr.hasNext(); ) {
          entityItr.advance();
          int neighborId = entityItr.key();
          GraphNode neighborNode = getNode(neighborId);
          if (neighborNode.getType().equals(GraphNodeTypes.ENTITY) &&
              !nodesToRemove.contains(neighborId)) {
            String neighborName = (String) neighborNode.getNodeData();
            Edge toAdd = new Edge(entityId, neighborId);
            if (!addedEdges.contains(toAdd) && 
                !edgesToRemove.contains(toAdd)) {
              pruned.addEdge(entityName, neighborName, entityItr.value());
              addedEdges.add(toAdd);
            }
          }
        }
      }
    }
    return pruned;
  }
  
  public class Edge {
    int source;
    int target;

    public Edge(int x, int y) {
      source = Math.min(x, y);
      target = Math.max(x, y);
    }

    @Override
    public boolean equals(Object o) {
      if (o instanceof Edge) {
        Edge e = (Edge) o;
        return source == e.source && target == e.target;
      }
      return false;
    }

    @Override
    public int hashCode() {
      return source * target;
    }
  }
}
