package edu.berkeley.cs186.database.concurrency;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * Each table will have a lock object associated with it in order
 * to implement table-level locking. The lock will keep track of its
 * transaction owners, type, and the waiting queue.
 */
public class Lock {


  private Set<Long> transactionOwners;
  private ConcurrentLinkedQueue<LockRequest> transactionQueue;
  private LockManager.LockType type;
  private long just_Released_Trans;

  public Lock(LockManager.LockType type) {
    this.transactionOwners = new HashSet<Long>(); //是一个set，里面是持有当前table lock的Transcation的number。
    this.transactionQueue = new ConcurrentLinkedQueue<LockRequest>();
    this.type = type;
    this.just_Released_Trans=-99;
  }

  //
  protected Set<Long> getOwners() {
    return this.transactionOwners;
  }

  public LockManager.LockType getType() {
    return this.type;
  }

  private void setType(LockManager.LockType newType) {
    this.type = newType;
  }

  public int getSize() {
    return this.transactionOwners.size();
  }

  public boolean isEmpty() {
    return this.transactionOwners.isEmpty();
  }

  private boolean containsTransaction(long transNum) {
    return this.transactionOwners.contains(transNum);
  }

  private void addToQueue(long transNum, LockManager.LockType lockType) {
    LockRequest lockRequest = new LockRequest(transNum, lockType);
    this.transactionQueue.add(lockRequest);
  }

  private void removeFromQueue(long transNum, LockManager.LockType lockType) {
    LockRequest lockRequest = new LockRequest(transNum, lockType);
    this.transactionQueue.remove(lockRequest);
  }

  private void addOwner(long transNum) {
    this.transactionOwners.add(transNum);
  }

  private void removeOwner(long transNum) {
    this.transactionOwners.remove(transNum);
  }

  /**
   * Attempts to resolve the specified lockRequest. Adds the request to the queue
   * and calls wait() until the request can be promoted and removed from the queue.
   * It then modifies this lock's owners/type as necessary.
   * @param transNum transNum of the lock request
   * @param lockType lockType of the lock request
   */
//key: 每个thread控制着一个acquire.
  protected synchronized void acquire(long transNum, LockManager.LockType lockType) {
    //TODO: Implement Me!!
    if(this.isEmpty() ) {
        addOwner(transNum);
        setType(lockType);
    }else if(this.containsTransaction(transNum) && this.getType().equals(LockManager.LockType.EXCLUSIVE)
            && lockType.equals(LockManager.LockType.SHARED))
    {
    }else if(this.containsTransaction(transNum) && this.getType().equals(LockManager.LockType.EXCLUSIVE) &&
            lockType.equals(LockManager.LockType.EXCLUSIVE))
    {
    }
    else{
      LockRequest checkQueue=new LockRequest(transNum, LockManager.LockType.SHARED);
      LockRequest checkQueue_2=new LockRequest(transNum, LockManager.LockType.EXCLUSIVE);
      if(!transactionQueue.contains(checkQueue) && !transactionQueue.contains(checkQueue_2)) {
        addToQueue(transNum, lockType);
        while (!notifyLocks(transNum, lockType)) {
          try {
            this.wait();
          } catch (InterruptedException e) {
          }
        }
      //需要一个prioritize update 的办法。不一定是peek。
        addOwner(transNum);
        this.setType(lockType);
        removeFromQueue(transNum, lockType);
      }
    }
  }

  //helper method  这里也写wait 等release 来call吗   需要return一个number回去告诉他哪个Transaction吗??????????????????????????
  // 为什么是notifyAll ???? 需要synchronized 吗
  protected boolean notifyLocks(long transNum, LockManager.LockType lockType ){
    if(this.containsTransaction(transNum) && this.getSize()==1 &&this.getType().equals(LockManager.LockType.SHARED)
            && lockType.equals(LockManager.LockType.EXCLUSIVE))
    {
        return true;
    }
    if(!this.isEmpty() && this.getType().equals(LockManager.LockType.SHARED) && lockType.equals(LockManager.LockType.SHARED)
            && this.transactionQueue.peek().equals( new LockRequest(transNum, lockType)))  //....problem
    {
        return true;
    }

    if(this.isEmpty() && this.transactionQueue.peek().equals( new LockRequest(transNum, lockType)))
    {
      return  true;
    }
    if(this.getType().equals(LockManager.LockType.SHARED)&& lockType.equals(LockManager.LockType.SHARED )){
      ConcurrentLinkedQueue<LockRequest> copyQueue=new ConcurrentLinkedQueue<LockRequest>(transactionQueue);
      Iterator<LockRequest> iter=copyQueue.iterator();
      while(iter.hasNext())
      {
        LockRequest temp=iter.next();
        if(temp.equals(new LockRequest(transNum, lockType)))
        {
          return true;
        }
        //在它前面如果有一个不是shared
        if(!temp.lockType.equals(LockManager.LockType.SHARED))
        {
         return false;
        }
      }
    }
    return false;
  }


  /**
   * transNum releases ownership of this lock
   * @param transNum transNum of transaction that is releasing ownership of this lock
   *                 当一个lock release时候 notify会notify到哪里
   */
  protected synchronized void release(long transNum) {
    //TODO: Implement Me!!
    removeOwner(transNum);
    setType(LockManager.LockType.SHARED);
    this.just_Released_Trans=transNum;
    notifyAll();
  }

  /**
   * Checks if the specified transNum holds a lock of lockType on this lock object
   * @param transNum transNum of lock request
   * @param lockType lock type of lock request
   * @return true if transNum holds the lock of type lockType
   */
  protected synchronized boolean holds(long transNum, LockManager.LockType lockType) {
    //TODO: Implement Me!!
    if(!containsTransaction(transNum))
    {
      return false;
    }
    if(this.getType().equals(lockType))
    {
      return true;
    }else {
      return false;
    }
  }

  /**
   * LockRequest objects keeps track of the transNum and lockType.
   * Two LockRequests are equal if they have the same transNum and lockType.
   */
  private class LockRequest {
      private long transNum;
      private LockManager.LockType lockType;
      private LockRequest(long transNum, LockManager.LockType lockType) {
        this.transNum = transNum;
        this.lockType = lockType;
      }

      @Override
      public int hashCode() {
        return (int) transNum;
      }

      @Override
      public boolean equals(Object obj) {
        if (!(obj instanceof LockRequest))
          return false;
        if (obj == this)
          return true;

        LockRequest rhs = (LockRequest) obj;
        return (this.transNum == rhs.transNum) && (this.lockType == rhs.lockType);
      }

  }

}
