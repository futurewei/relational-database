package edu.berkeley.cs186.database.query;

import java.util.*;

import edu.berkeley.cs186.database.DatabaseException;
import edu.berkeley.cs186.database.Database;
import edu.berkeley.cs186.database.datatypes.DataType;
import edu.berkeley.cs186.database.table.Record;
import edu.berkeley.cs186.database.table.stats.TableStats;


import java.util.ArrayList;
import java.util.HashMap;


public class GraceHashOperator extends JoinOperator {

  private int numBuffers;

  public GraceHashOperator(QueryOperator leftSource,
                      QueryOperator rightSource,
                      String leftColumnName,
                      String rightColumnName,
                      Database.Transaction transaction) throws QueryPlanException, DatabaseException {
    super(leftSource,
          rightSource,
          leftColumnName,
          rightColumnName,
          transaction,
          JoinType.GRACEHASH);

    this.numBuffers = transaction.getNumMemoryPages();
    this.stats = this.estimateStats();
    this.cost = this.estimateIOCost();
  }

  public Iterator<Record> iterator() throws QueryPlanException, DatabaseException {
    return new GraceHashIterator();
  }

  public int estimateIOCost() throws QueryPlanException {
    // TODO: implement me!
      int leftNumPages=this.getLeftSource().getStats().getNumPages();
      int rightNumPages=this.getRightSource().getStats().getNumPages();
      int estimate=3*leftNumPages+3*rightNumPages;
      return estimate;
  }

  /**
   * An implementation of Iterator that provides an iterator interface for this operator.
   */
  private class GraceHashIterator implements Iterator<Record> {
    private Iterator<Record> leftIterator;
    private Iterator<Record> rightIterator;
    private Iterator<Record> partitionIteratorL;
    private Iterator<Record> partitionIteratorR;
    private Record leftRecord;
    private Record rightRecord;
    private Record nextRecord;
    private String[] leftPartitions;
    private String[] rightPartitions;
    private int currentPartition=0;
    private Map<DataType, ArrayList<Record>> inMemoryHashTable;
    private int position=0;
    private Record currentProbeRecord;
    private boolean finishedJoin=true;

    public GraceHashIterator() throws QueryPlanException, DatabaseException {
      this.leftIterator = getLeftSource().iterator();
      this.rightIterator = getRightSource().iterator();
      leftPartitions = new String[numBuffers - 1];
      rightPartitions = new String[numBuffers - 1];
      String leftTableName;
      String rightTableName;
      //b-1个？？
      for (int i = 0; i < numBuffers - 1; i++) {
        leftTableName = "Temp HashJoin Left Partition " + Integer.toString(i);
        rightTableName = "Temp HashJoin Right Partition " + Integer.toString(i);
        GraceHashOperator.this.createTempTable(getLeftSource().getOutputSchema(), leftTableName);
        GraceHashOperator.this.createTempTable(getRightSource().getOutputSchema(), rightTableName);
        leftPartitions[i] = leftTableName; //only contains name of the table
        rightPartitions[i] = rightTableName;
      }
      //phase 1 partition stage  
      while(this.leftIterator.hasNext()){
         leftRecord=this.leftIterator.next();
         DataType leftCol = this.leftRecord.getValues().get(GraceHashOperator.this.getLeftColumnIndex());
         int partitionNumL= (leftCol.hashCode()) % (numBuffers-1); //momd (n-1) 会有num from 0 to ...n-2, 一共n-1个数。
         GraceHashOperator.this.addRecord(leftPartitions[partitionNumL],leftRecord.getValues()); //不确定加进去没有。奇怪的写法。
      }
     
     while(this.rightIterator.hasNext())
     {
      rightRecord=this.rightIterator.next();
      DataType rightCol= this.rightRecord.getValues().get(GraceHashOperator.this.getRightColumnIndex());
      int partitionNumR=(rightCol.hashCode()) %(numBuffers-1);
      GraceHashOperator.this.addRecord(rightPartitions[partitionNumR],rightRecord.getValues());
     }
      
      //phase 2: hash left parttion into hash table, using hashMap.
     inMemoryHashTable=new HashMap<>();
     partitionIteratorL=GraceHashOperator.this.getTableIterator(leftPartitions[currentPartition]);
     partitionIteratorR=GraceHashOperator.this.getTableIterator(rightPartitions[currentPartition]);
    }

    /**
     * Checks if there are more record(s) to yield
     *
     * @return true if this iterator has another record to yield, otherwise false
     */
    public boolean hasNext() {
      //iterate 整个partition,把同样col-val的装进hash-table.
        //System.out.println("调用hasNext()");
        //System.out.println("hasNext.joined?"+finishedJoin);
        if(this.nextRecord!=null)
        {
            return true;
        }
        //bug here:
     if(partitionIteratorR.hasNext()==false && finishedJoin==true)
       {
           //System.out.println("右边Iterator没东西了");
          currentPartition+=1;
           //System.out.println("当前partition类别: "+ currentPartition);
          if(currentPartition<= numBuffers-2){
              try {
                  partitionIteratorL = GraceHashOperator.this.getTableIterator(leftPartitions[currentPartition]);
                  partitionIteratorR = GraceHashOperator.this.getTableIterator(rightPartitions[currentPartition]);
                  position=0;
                  finishedJoin=true;
                  //System.out.println("更新了一下iterator");
              }catch (Exception e)
              {
                  return false;
              }
          }else{
            return false;
          }
       }
      while(partitionIteratorL.hasNext())
      {
          //System.out.println("load memory table");
          Record keyRecord=partitionIteratorL.next();
          DataType keyCol=keyRecord.getValues().get(GraceHashOperator.this.getLeftColumnIndex());
          if(!this.inMemoryHashTable.containsKey(keyCol))
          {
              ArrayList<Record> hashRecord=new ArrayList<>();
              hashRecord.add(keyRecord);
              this.inMemoryHashTable.put(keyCol, hashRecord); 
          }else{
            ArrayList temp=inMemoryHashTable.get(keyCol);
            temp.add(keyRecord);
            this.inMemoryHashTable.put(keyCol, temp);
          }
      }

      if(partitionIteratorR.hasNext() && finishedJoin)
      {
        //System.out.println("还有 right record");
        Record probRecord=partitionIteratorR.next(); //gg
        currentProbeRecord=probRecord;
        DataType probKey=probRecord.getValues().get(GraceHashOperator.this.getRightColumnIndex()); //这里还是getLeftColumnIndex吗
         if(this.inMemoryHashTable.containsKey(probKey))   //if it doesn't contain the key???????!!!! 那么rightPartition到此为止，load下一个left_partition.
          {
              finishedJoin=false;
              ArrayList probTemp=this.inMemoryHashTable.get(probKey);
              int size=probTemp.size();
              if(position<size)
              {
                  //System.out.println("当前position："+position);
                  List<DataType> leftValues= ((Record) probTemp.get(position)).getValues(); //一个record
                  List<DataType> tempLeft=new ArrayList(leftValues);
                  position++;
                  List<DataType> rightValues = probRecord.getValues(); 
                  tempLeft.addAll(rightValues);
                  this.nextRecord=new Record(tempLeft);
                  //System.out.println("检查：" + probTemp.get(position-1));
                  //System.out.println("hasNext() return true");
                  return true;
              }else{
                //如果所有的type A的都被join完了，那么把这个Type从hashtable里remove。
                  //System.out.println("nima");
                  position=0; //set position back to 0.
                  return this.hasNext(); //当前hashtable里没有符合条件的Type,左边partition不要了，一起跳到下一个partition.
              }
          }else{
            return this.hasNext();
          }
      }else if(finishedJoin==false)
      {
          //System.out.println("finisihedJoin: "+finishedJoin);
          Record probRecord=currentProbeRecord;
          DataType probKey=probRecord.getValues().get(GraceHashOperator.this.getRightColumnIndex()); //这里还是getLeftColumnIndex吗
          if(this.inMemoryHashTable.containsKey(probKey))   //if it doesn't contain the key???????!!!! 那么rightPartition到此为止，load下一个left_partition.
          {
              ArrayList probTemp=this.inMemoryHashTable.get(probKey);
              int size=probTemp.size();
              if(position<size)
              {
                  //System.out.println("当前position："+position);
                  List<DataType> leftValues= ((Record) probTemp.get(position)).getValues(); //一个record
                  List<DataType> tempLeft=new ArrayList(leftValues);
                  position++;
                  List<DataType> rightValues = probRecord.getValues();
                  tempLeft.addAll(rightValues);
                  this.nextRecord=new Record(tempLeft);
                  //System.out.println("测试："+probTemp.get(position-1));
                  //System.out.println("hasNext() return true");
                  return true;
              }else{
                  //System.out.println("join 完了");
                  //如果所有的type A的都被join完了，那么把这个Type从hashtable里remove。
                  position=0; //set position back to 0.
                  finishedJoin=true;
                  return this.hasNext(); //当前hashtable里没有符合条件的Type,左边partition不要了，一起跳到下一个partition.
              }
          }else{
              return this.hasNext();
          }

      }
      else{
          //System.out.print("当前右边partition 没record,recursive call this.hasNext()");
          return this.hasNext();
      }
    }

    /**
     * Yields the next record of this iterator.
     *
     * @return the next Record
     * @throws NoSuchElementException if there are no more Records to yield
     */
    public Record next() {
      // TODO: implement me!
        //System.out.print("Next()开始执行");
    if (this.hasNext()) {
        //System.out.println("Next() 成功返回record");
        Record r = this.nextRecord;
        this.nextRecord = null;  //这个要用到吧。。。
        return r;
      }
      throw new NoSuchElementException();
    }

    public void remove() {
      throw new UnsupportedOperationException();
    }
  }
}
