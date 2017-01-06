package edu.berkeley.cs186.database.concurrency;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * The LockManager provides allows for table-level locking by keeping
 * track of which transactions own and are waiting for locks on specific tables.
 *
 * THIS CODE IS FOR PROJECT 3 .
 */
public class LockManager {
  //有两种lockType一个是SHARED,一个是EXCLUSIVE
  public enum LockType {SHARED, EXCLUSIVE};
  private ConcurrentHashMap<String, Lock> tableNameToLock;
  private WaitsForGraph waitsForGraph;
  
  public LockManager() {
    //tableNameToLock是一个hashmap, key为table名字, value为lock.
    tableNameToLock = new ConcurrentHashMap<String, Lock>();
    waitsForGraph = new WaitsForGraph();
  }

  /**
   * Acquires a lock on tableName of type lockType for transaction transNum.
   *
   * @param tableName the database to lock on
   * @param transNum the transactions id
   * @param lockType the type of lock
   */
  public void acquireLock(String tableName, long transNum, LockType lockType) {
    //如果manager里没有对这个table的信息，就给这个table加一个新的lock。如果有这个table的话，handle potential deadlock,然后acquire一个lock。
    if (!this.tableNameToLock.containsKey(tableName)) {
      this.tableNameToLock.put(tableName, new Lock(lockType)); //每个table都规定好了一个指定的最外层的一个lock。
    }
    Lock lock = this.tableNameToLock.get(tableName);

    handlePotentialDeadlock(lock, transNum, lockType);

    lock.acquire(transNum, lockType);

  }

  /**
   * Adds any nodes/edges caused the by the specified LockRequest to  ！！！什么时候加nodes(当graph里一个node都没有的时候)
   * this LockManager's WaitsForGraph
   * @param lock the lock on the table that the LockRequest is for
   * @param transNum the transNum of the lock request
   * @param lockType the lockType of the lcok request
   */
  private void handlePotentialDeadlock(Lock lock, long transNum, LockType lockType) {
    //TODO: Implement Me!!
    List<Long> list=new ArrayList<Long> (lock.getOwners());
    waitsForGraph.addNode(transNum);
    if(lockType==null)
    {
      throw new NullPointerException("lock type is null");
    }
    if(lock.getType().equals(LockType.SHARED) && lockType.equals(lockType.EXCLUSIVE)) //为何
    {
      //因为当前hold share的可能不止一个。
      for(int i=0; i< list.size(); i++)
      {
        if(waitsForGraph.edgeCausesCycle(transNum, list.get(i)))
        {
          throw new DeadlockException("error");
        }
          waitsForGraph.addEdge(transNum, list.get(i));
      }

    }else if(lock.getType().equals(lockType.EXCLUSIVE)){
        for (int i=0; i<list.size(); i++)
        {
          if(waitsForGraph.edgeCausesCycle(transNum, list.get(i)) && transNum!=list.get(i)) //self loop don't count
          {
            throw new DeadlockException("error");
          }
          waitsForGraph.addEdge(transNum, list.get(i));
        }
    }
    //if currently, there's no holder? nothing happen?
  }


  /**
   * Releases transNum's lock on tableName.
   *
   * @param tableName the table that was locked
   * @param transNum the transaction that held the lock
   */
  public void releaseLock(String tableName, long transNum) {
    if (this.tableNameToLock.containsKey(tableName)) {
      Lock lock = this.tableNameToLock.get(tableName);
      lock.release(transNum);
    }
  }

  /**
   * Returns a boolean indicating whether or not transNum holds a lock of type lt on tableName.
   *
   * @param tableName the table that we're checking
   * @param transNum the transaction that we're checking for
   * @param lockType the lock type
   * @return whether the lock is held or not
   */
  public boolean holdsLock(String tableName, long transNum, LockType lockType) {
    if (!this.tableNameToLock.containsKey(tableName)) {
      return false;
    }

    Lock lock = this.tableNameToLock.get(tableName);
    return lock.holds(transNum, lockType);
  }
}
