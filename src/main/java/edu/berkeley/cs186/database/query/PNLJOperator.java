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
import edu.berkeley.cs186.database.table.Table;
import edu.berkeley.cs186.database.table.stats.TableStats;
import edu.berkeley.cs186.database.table.Schema;

public class PNLJOperator extends JoinOperator {

  public PNLJOperator(QueryOperator leftSource,
                      QueryOperator rightSource,
                      String leftColumnName,
                      String rightColumnName,
                      Database.Transaction transaction) throws QueryPlanException, DatabaseException {
    super(leftSource,
          rightSource,
          leftColumnName,
          rightColumnName,
          transaction,
          JoinType.PNLJ);

    this.stats = this.estimateStats();
    this.cost = this.estimateIOCost();
  }

  public Iterator<Record> iterator() throws QueryPlanException, DatabaseException {
    return new PNLJIterator();
  }

  public int estimateIOCost() throws QueryPlanException {
    // TODO: implement me!
      int estimateCost=0;
      int leftPages = this.getLeftSource().getStats().getNumPages();
      int rightPages =this.getRightSource().getStats().getNumPages();
      estimateCost=leftPages*rightPages+leftPages;
      return estimateCost;
  }

  /**
   * An implementation of Iterator that provides an iterator interface for this operator.
   */
    private class PNLJIterator implements Iterator<Record> {
      private String leftTableName;
      private String rightTableName;
      private Iterator<Page> leftIterator;
      private Iterator<Page> rightIterator;
      private Record leftRecord;
      private Record nextRecord;
      private Record rightRecord;
      private Page leftPage;
      private Page rightPage;
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

      public PNLJIterator() throws QueryPlanException, DatabaseException {
          if (PNLJOperator.this.getLeftSource().isSequentialScan()) {
              this.leftTableName = ((SequentialScanOperator) PNLJOperator.this.getLeftSource()).getTableName(); //this one is for sequentialScan.
          } else {
              //for other joinType.
              this.leftTableName = "Temp" + PNLJOperator.this.getJoinType().toString() + "Operator" + PNLJOperator.this.getLeftColumnName() + "Left";
              PNLJOperator.this.createTempTable(PNLJOperator.this.getLeftSource().getOutputSchema(), leftTableName); //create a temp leftTable
              Iterator<Record> leftIter = PNLJOperator.this.getLeftSource().iterator(); 
              while (leftIter.hasNext()) {
                  //As long as leftTable still has elements, add values to leftTemp Table.
                  PNLJOperator.this.addRecord(leftTableName, leftIter.next().getValues());
              }
          }

          if (PNLJOperator.this.getRightSource().isSequentialScan()) {
              this.rightTableName = ((SequentialScanOperator) PNLJOperator.this.getRightSource()).getTableName();
          } else {
              this.rightTableName = "Temp" + PNLJOperator.this.getJoinType().toString() + "Operator" + PNLJOperator.this.getRightColumnName() + "Right";
              PNLJOperator.this.createTempTable(PNLJOperator.this.getRightSource().getOutputSchema(), rightTableName);
              Iterator<Record> rightIter = PNLJOperator.this.getRightSource().iterator();
              while (rightIter.hasNext()) {
                  PNLJOperator.this.addRecord(rightTableName, rightIter.next().getValues());
              }
          }

          //set up schema
          left_table_schema=PNLJOperator.this.getLeftSource().getOutputSchema();
          right_table_schema=PNLJOperator.this.getRightSource().getOutputSchema();

          //set up page Header size
          left_table_page_headerSize=PNLJOperator.this.getHeaderSize(leftTableName);
          right_table_page_headerSize=PNLJOperator.this.getHeaderSize(rightTableName);

          //set up pageIterator
          this.leftIterator=PNLJOperator.this.getPageIterator(leftTableName);
          this.leftIterator.next();//skip the header page

          this.rightIterator=PNLJOperator.this.getPageIterator(rightTableName);
          this.rightIterator.next(); //skip the header page
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
            if( (byte) (slot[index] & (1<<(7-j))) !=0)
            {
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
            //System.out.println("next left "+this.leftIterator.hasNext());
              if(this.leftIterator.hasNext())
              {
                this.leftPage=this.leftIterator.next();  //problem
                this.left_index=0;
                left_bit=0;
              } else {
                return false; //if there's no next left page, return false.
              }
          }

          if(this.rightPage== null)
          {
            //System.out.println("new right page");
            if(this.rightIterator.hasNext())
            {
              this.rightPage=this.rightIterator.next();
              this.right_index=0;
              this.right_bit=0;
            }else{
               //re-initialize right table page Iterator, proceed to another left-table-page.
              this.leftPage=null;
              try{
                  this.rightIterator=PNLJOperator.this.getPageIterator(rightTableName);
                }catch(Exception e)
              {
                return false;
              }
              this.rightIterator.next(); //skip the first header page
              continue;
            }
        }
          while(true)  
          {
              //System.out.println("current left Record "+left_index);
              this.leftRecord=nextRecordOnPage(this.leftPage, left_table_page_headerSize, left_table_schema,left_index, left_bit, "left");
              if(this.leftRecord==null) //problem
              {
                //System.out.println("no more record in this left page");
                left_index=0;
                left_bit=0;
                this.rightPage=null;
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
                  DataType leftJoinValue = this.leftRecord.getValues().get(PNLJOperator.this.getLeftColumnIndex());
                  DataType rightJoinValue =this.rightRecord.getValues().get(PNLJOperator.this.getRightColumnIndex());
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
