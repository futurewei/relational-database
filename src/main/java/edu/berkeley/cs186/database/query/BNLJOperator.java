package edu.berkeley.cs186.database.query;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import edu.berkeley.cs186.database.DatabaseException;
import edu.berkeley.cs186.database.datatypes.DataType;
import edu.berkeley.cs186.database.io.Page;
import edu.berkeley.cs186.database.Database;
import edu.berkeley.cs186.database.table.Record;
import edu.berkeley.cs186.database.table.stats.TableStats;
import edu.berkeley.cs186.database.table.Schema;

public class BNLJOperator extends JoinOperator {

  private int numBuffers;

  public BNLJOperator(QueryOperator leftSource,
                      QueryOperator rightSource,
                      String leftColumnName,
                      String rightColumnName,
                      Database.Transaction transaction) throws QueryPlanException, DatabaseException {
    super(leftSource, rightSource, leftColumnName, rightColumnName, transaction, JoinType.BNLJ);

    this.numBuffers = transaction.getNumMemoryPages();
    this.stats = this.estimateStats();
    this.cost = this.estimateIOCost();
  }

  public Iterator<Record> iterator() throws QueryPlanException, DatabaseException {
    return new BNLJIterator();
  }

  public int estimateIOCost() throws QueryPlanException {
    // TODO: implement me!
    int numPagesPerLeft=this.getLeftSource().getStats().getNumPages();
    int numPagesPerRight=this.getRightSource().getStats().getNumPages();
    int buffer_min_2=numBuffers-2;
    double div=(double) numPagesPerLeft/ buffer_min_2;
    int up=(int) Math.ceil(div);
    int estimate=up*numPagesPerRight+numPagesPerLeft;
    return estimate;
  }

  /**
   * An implementation of Iterator that provides an iterator interface for this operator.
   */
  private class BNLJIterator implements Iterator<Record> {
    private String leftTableName;
    private String rightTableName;
    private Iterator<Page> leftIterator;
    private Iterator<Page> rightIterator;
    private Record leftRecord;
    private Record nextRecord;
    private Record rightRecord;
    private Page leftPage;
    private Page rightPage;
    private Page[] block;
    private int left_index;
    private int right_index;
    private Schema left_table_schema;
    private Schema right_table_schema;
    private int left_table_page_headerSize;
    private int right_table_page_headerSize;
    private int left_bit;
    private int next_left_index;
    private int next_left_bit;
    private int right_bit;
    private int i=0;
    private int limit=0;

    public BNLJIterator() throws QueryPlanException, DatabaseException {
      if (BNLJOperator.this.getLeftSource().isSequentialScan()) {
        this.leftTableName = ((SequentialScanOperator)BNLJOperator.this.getLeftSource()).getTableName();
      } else {
        this.leftTableName = "Temp" + BNLJOperator.this.getJoinType().toString() + "Operator" + BNLJOperator.this.getLeftColumnName() + "Left";
        BNLJOperator.this.createTempTable(BNLJOperator.this.getLeftSource().getOutputSchema(), leftTableName);
        Iterator<Record> leftIter = BNLJOperator.this.getLeftSource().iterator();
        while (leftIter.hasNext()) {
          BNLJOperator.this.addRecord(leftTableName, leftIter.next().getValues());
        }
      }
      if (BNLJOperator.this.getRightSource().isSequentialScan()) {
        this.rightTableName = ((SequentialScanOperator)BNLJOperator.this.getRightSource()).getTableName();
      } else {
        this.rightTableName = "Temp" + BNLJOperator.this.getJoinType().toString() + "Operator" + BNLJOperator.this.getRightColumnName() + "Right";
        BNLJOperator.this.createTempTable(BNLJOperator.this.getRightSource().getOutputSchema(), rightTableName);
        Iterator<Record> rightIter = BNLJOperator.this.getRightSource().iterator();
        while (rightIter.hasNext()) {
          BNLJOperator.this.addRecord(rightTableName, rightIter.next().getValues());
        }
      }


      // TODO: implement me!
        block=new Page[numBuffers-2];

                //set up schema
        left_table_schema=BNLJOperator.this.getLeftSource().getOutputSchema();
        right_table_schema=BNLJOperator.this.getRightSource().getOutputSchema();

          //set up page Header size
        left_table_page_headerSize=BNLJOperator.this.getHeaderSize(leftTableName);
        right_table_page_headerSize=BNLJOperator.this.getHeaderSize(rightTableName);

          //set up pageIterator
        this.leftIterator=BNLJOperator.this.getPageIterator(leftTableName);
        this.leftIterator.next();//skip the header page

        this.rightIterator=BNLJOperator.this.getPageIterator(rightTableName);
        this.rightIterator.next(); //skip the header page

        loadBlock();
    }


      public Record nextRecordOnPage(Page current_page, int pageHeaderSize, Schema schema, int index, int bit, String signal){
          //also need to check if a record in header bit is 1.
        byte[] slot= current_page.readBytes(0, pageHeaderSize);
        while(index < pageHeaderSize)
        {
            //System.out.println("start index: "+index + " start bit "+bit );
            if(bit==8)
            {
              bit=0;
              index++;
              continue;
            }
          for(int j=bit; j<8; j++)
          {
            //System.out.println(slot[index]);
            if( (byte) (slot[index] & (1<<(7-j))) !=0)
            {
              //System.out.println("hit");
              if(signal.equals("right"))
              {
                right_index=index;
                right_bit=j+1;
              }else{
                next_left_index=index;
                next_left_bit=bit+1;
              }
              int starting = pageHeaderSize + (schema.getEntrySize() * (index*8+j)); //entry number
              return schema.decode(current_page.readBytes(starting, schema.getEntrySize()));  //syntax
            }
          }
          index++;
        }
          return null;
      }



      public void loadBlock()
      {
        limit=0;
         while(limit<numBuffers-2)
        {
          if(this.leftIterator.hasNext()){
              block[limit]=this.leftIterator.next();
              limit++;
          }else{
            //whatever limit is, is our current limit
            break;
          }
        }
      }

    /**
     * Checks if there are more record(s) to yield
     *
     * @return true if this iterator has another record to yield, otherwise false
     */
    public boolean hasNext() {
      // TODO: implement me!
      if(this.nextRecord != null)
      {
        return true;
      }

  //every time here, we move to a new left-page.
      while (true)
      {

          if(this.leftPage == null )
          {
              if(i<limit)
              {
                this.leftPage=block[i];
                i=i+1;
                this.left_index=0;
                left_bit=0;
              }else if(this.leftIterator.hasNext()) 
              {
                i=0;
                loadBlock();
                return hasNext();
                //reload the block.
              }
              else {
                return false; //if there's no next left page, return false.
              }
          }

          if(this.rightPage== null)
          {
            // 
           if(this.rightIterator.hasNext())
            {
              this.rightPage=this.rightIterator.next();
              this.right_index=0;
              this.right_bit=0;
              this.leftPage=null;
              if(i==limit){
                i=0;
              }
            }else{
               //re-initialize right table page Iterator
              this.leftPage=null;
              try{
                  this.rightIterator=BNLJOperator.this.getPageIterator(rightTableName);
                }catch(Exception e)
              {
                return false;
              }
              this.rightIterator.next(); //skip the first header page
            }
            continue;
        }
          while(true)  
          {
              //System.out.println("current left Record "+left_index);
              this.leftRecord=nextRecordOnPage(this.leftPage, left_table_page_headerSize, left_table_schema,left_index, left_bit, "left");
              if(this.leftRecord==null) //问题： block里的都run完，upadate right page to be next right page.
              {
                //System.out.println("no more record in this left page");
                left_index=0;
                left_bit=0;
                this.rightPage=null; //make change!
                break;
              } 
              //for each record in left page, check all records in right page.  
              while(true)
              {
                  //System.out.println("current right Record "+right_index);
                  this.rightRecord=nextRecordOnPage(this.rightPage, right_table_page_headerSize, right_table_schema ,right_index, right_bit, "right");
                  //if no more right record on current right table page
                  if(this.rightRecord==null)
                  {
                    //System.out.println("right enter");
                    right_index=0;
                    right_bit=0;
                    left_index=next_left_index;
                    left_bit=next_left_bit;
                    break; //then left record move one more, iterate all records in this right page again
                  }
                  DataType leftJoinValue = this.leftRecord.getValues().get(BNLJOperator.this.getLeftColumnIndex());
                  DataType rightJoinValue =this.rightRecord.getValues().get(BNLJOperator.this.getRightColumnIndex());
                  if(leftJoinValue.equals(rightJoinValue))
                {
                    List<DataType> leftValues = new ArrayList<DataType>(this.leftRecord.getValues()); //each record is essentially a list of values
                    List<DataType> rightValues = new ArrayList<DataType>(rightRecord.getValues());

                    leftValues.addAll(rightValues);
                    this.nextRecord = new Record(leftValues); //this.nextRecord is the next record for new joined table.
                    return true;
                }
              }
          }
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
      if(this.hasNext()){
        Record r=this.nextRecord;
        this.nextRecord = null;
        return r;
      }
      throw new NoSuchElementException();
    }

    public void remove() {
      throw new UnsupportedOperationException();
    }
  }
}
