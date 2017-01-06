package edu.berkeley.cs186.database.concurrency;

import java.util.*;

/**
 * A waits for graph for the lock manager (used to detect if
 * deadlock will occur and throw a DeadlockException if it does).
 */
public class WaitsForGraph {

  // We store the directed graph as an adjacency list where each node (transaction) is
  // mapped to a list of the nodes it has an edge to.
  private Map<Long, ArrayList<Long>> graph;
  private Map<Long, Boolean> visited=new HashMap<Long, Boolean>();
  private Map<Long, Boolean> stack=new HashMap<Long, Boolean>();

  public WaitsForGraph() {
    graph = new HashMap<Long, ArrayList<Long>>();
  }

  public boolean containsNode(long transNum) {
    return graph.containsKey(transNum);
  }

  protected void addNode(long transNum) {
    if (!graph.containsKey(transNum)) {
      graph.put(transNum, new ArrayList<Long>());
    }
  }

  protected void addEdge(long from, long to) {
    if (!this.edgeExists(from, to)) {
      ArrayList<Long> edges = graph.get(from);
      edges.add(to);
    }
  }

  protected void removeEdge(long from, long to) {
    if (this.edgeExists(from, to)) {
      ArrayList<Long> edges = graph.get(from); //reference the same object.
      edges.remove(to);
    }
  }

  protected boolean edgeExists(long from, long to) {
    if (!graph.containsKey(from)) {
      return false;
    }
    ArrayList<Long> edges = graph.get(from);
    return edges.contains(to);
  }

  /**
   * Checks if adding the edge specified by to and from would cause a cycle in this
   * WaitsForGraph. Does not actually modify the graph in any way.
   * @param from the transNum from which the edge points
   * @param to the transNum to which the edge points
   * @return
   */

  protected boolean edgeCausesCycle(long from, long to) {
    //TODO: Implement Me!!
    visited.put(from, true);
    stack.put(from, true);
    visited.put(to, true);
    stack.put(to, true);
    boolean hasCycle=DFS(to);
    return hasCycle;
  }
//如果两个node指向了同一个node怎么办。这也许并不是cycle啊

  protected boolean DFS(long start)
  {
    for(int i=0; i<graph.get(start).size();i++)
    {
      long sta=graph.get(start).get(i);
      if(visited.containsKey(sta) && stack.get(sta)==true)
      {
        return true;
      }else{
        visited.put(sta, true);
        stack.put(sta, true);
        if (DFS(sta)==true)
        {
          return true;
        }
      }
    }
    stack.put(start, false);
    return false;
  }
}
