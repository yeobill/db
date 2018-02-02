package edu.berkeley.cs186.database.query;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import edu.berkeley.cs186.database.Database;
import edu.berkeley.cs186.database.DatabaseException;
import edu.berkeley.cs186.database.common.BacktrackingIterator;
import edu.berkeley.cs186.database.databox.DataBox;
import edu.berkeley.cs186.database.io.Page;
import edu.berkeley.cs186.database.table.Record;
import edu.berkeley.cs186.database.table.Schema;

public class BNLJOperator extends JoinOperator {

  protected int numBuffers;

  public BNLJOperator(QueryOperator leftSource,
                      QueryOperator rightSource,
                      String leftColumnName,
                      String rightColumnName,
                      Database.Transaction transaction) throws QueryPlanException, DatabaseException {
    super(leftSource,
            rightSource,
            leftColumnName,
            rightColumnName,
            transaction,
            JoinType.BNLJ);

    this.numBuffers = transaction.getNumMemoryPages();
  }

  public Iterator<Record> iterator() throws QueryPlanException, DatabaseException {
    return new BNLJIterator();
  }


  /**
   * An implementation of Iterator that provides an iterator interface for this operator.
   */
  private class BNLJIterator extends JoinIterator {
    // add any member variables here
    private String leftTableName;
    private String rightTableName;
    private BacktrackingIterator<Page> leftPageIterator;
    private BacktrackingIterator<Page> rightPageIterator;
    private BacktrackingIterator<Record> leftRidIter;
    private BacktrackingIterator<Record> rightRidIter;
    private Record leftRecord;
    private Record rightRecord;
    private Record nextRecord;
    private Page leftPage;
    private Page rightPage;
    private Page[] leftPageList = new Page[numBuffers-2];
    private Page[] rightPageList = new Page[1];

    public BNLJIterator() throws QueryPlanException, DatabaseException {
      super();
      this.leftTableName = this.getLeftTableName();
      this.rightTableName = this.getRightTableName();
      this.leftPageIterator = BNLJOperator.this.getPageIterator(leftTableName);

      this.rightPageIterator = BNLJOperator.this.getPageIterator(rightTableName);
      this.leftPageIterator.next();
      this.rightPageIterator.next();
      if (this.rightPageIterator.hasNext()) {
        this.rightPageIterator.next();
        this.rightPageIterator.mark();
      }
      this.leftPage = null;
      this.rightPage = null;
      this.leftRidIter = null;
      this.rightRidIter = null;
      this.leftRecord = null;
      this.rightRecord = null;
    }

    public boolean hasNext() {

      if (this.nextRecord != null) {
        return true;
      }
      while (true) {
        if (this.leftPage == null) {
          if (this.leftPageIterator.hasNext()) {
            int count = 1;
            this.leftPage = this.leftPageIterator.next();
            this.leftPageList[0] = this.leftPage;
            for (int i=1; i < numBuffers; i++) {
              if (this.leftPageIterator.hasNext()) {
                this.leftPage = this.leftPageIterator.next();
                this.leftPageList[i] = this.leftPage;
                count++;
              }
              else {
                Page[] temp = new Page[count];
                System.arraycopy(this.leftPageList, 0, temp, 0, count);
                this.leftPageList = new Page[this.leftPageList.length];
                this.leftPageList = temp;
                break;
              }
            }
            try {
              this.leftRidIter = BNLJOperator.this.getBlockIterator(this.leftTableName, this.leftPageList);
              this.rightPageIterator.reset();
            } catch (DatabaseException d) {
              return false;
            }
          }
          else {
            return false;
          }
        }

        if (this.rightPage == null) {
          if (this.rightPageIterator.hasNext()) {
            this.rightPage = this.rightPageIterator.next();
            this.rightPageList[0] = this.rightPage;
            try {
              this.rightRidIter = BNLJOperator.this.getBlockIterator(this.rightTableName, this.rightPageList);
              this.leftRidIter = BNLJOperator.this.getBlockIterator(this.leftTableName, this.leftPageList);
            } catch (DatabaseException d) {
              return false;
            }
          } else {
            this.leftPage = null;
          }
        }
        if (this.leftRecord == null) {
          if (this.leftRidIter.hasNext()) {
            this.leftRecord = this.leftRidIter.next();
            try {
              this.rightRidIter = BNLJOperator.this.getBlockIterator(this.rightTableName, this.rightPageList);
            } catch (DatabaseException d) {
              return false;
            }
          } else {
            this.rightPage = null;
          }
        }
        while (this.rightRidIter.hasNext()) {
          this.rightRecord = this.rightRidIter.next();
          DataBox leftJoinValue = this.leftRecord.getValues().get(BNLJOperator.this.getLeftColumnIndex());
          DataBox rightJoinValue = rightRecord.getValues().get(BNLJOperator.this.getRightColumnIndex());
          if (leftJoinValue.equals(rightJoinValue)) {
            List<DataBox> leftValues = new ArrayList<DataBox>(this.leftRecord.getValues());
            List<DataBox> rightValues = new ArrayList<DataBox>(rightRecord.getValues());
            leftValues.addAll(rightValues);
            this.nextRecord = new Record(leftValues);
            return true;
          }
        }
        this.leftRecord = null;
      }

    }

    /**
     * Yields the next record of this iterator.
     *
     * @return the next Record
     * @throws NoSuchElementException if there are no more Records to yield
     */
    public Record next() {
      if (this.hasNext()) {
        Record r = this.nextRecord;
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
