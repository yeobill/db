package edu.berkeley.cs186.database.query;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import edu.berkeley.cs186.database.Database;
import edu.berkeley.cs186.database.DatabaseException;
import edu.berkeley.cs186.database.common.BacktrackingIterator;
import edu.berkeley.cs186.database.databox.DataBox;
import edu.berkeley.cs186.database.io.Page;
import edu.berkeley.cs186.database.table.Record;
import edu.berkeley.cs186.database.table.RecordIterator;
import edu.berkeley.cs186.database.table.Schema;

public class SortMergeOperator extends JoinOperator {

  public SortMergeOperator(QueryOperator leftSource,
           QueryOperator rightSource,
           String leftColumnName,
           String rightColumnName,
           Database.Transaction transaction) throws QueryPlanException, DatabaseException {
    super(leftSource, rightSource, leftColumnName, rightColumnName, transaction, JoinType.SORTMERGE);

  }

  public Iterator<Record> iterator() throws QueryPlanException, DatabaseException {
    return new SortMergeOperator.SortMergeIterator();
  }


  /**
  * An implementation of Iterator that provides an iterator interface for this operator.
  */
  private class SortMergeIterator extends JoinIterator {
    private Iterator<Record> leftIter;
    private BacktrackingIterator<Record> rightIter;
    private Record leftRecord;
    private Record rightRecord;
    private Record prevLeftRecord;
    private Record prevRightRecord;
    private String leftTableName;
    private String rightTableName;
    private Record nextRecord;
    private DataBox leftData;
    private DataBox rightData;
    private DataBox prevRightData;
    private DataBox prevLeftData;
    private Record markedR;
    private DataBox marked;
    private boolean isMark;

    public SortMergeIterator() throws QueryPlanException, DatabaseException {
      super();
      SortOperator leftSortOp = new SortOperator(SortMergeOperator.this.getTransaction(), this.getLeftTableName(), new LeftRecordComparator());
      SortOperator rightSortOp = new SortOperator(SortMergeOperator.this.getTransaction(), this.getRightTableName(), new RightRecordComparator());
      this.leftTableName = leftSortOp.sort();
      this.rightTableName = rightSortOp.sort();

      this.leftIter = SortMergeOperator.this.getRecordIterator(this.leftTableName);
      this.rightIter = SortMergeOperator.this.getRecordIterator(this.rightTableName);

      if (this.rightIter.hasNext()) {
        this.markedR = this.rightIter.next();
        this.rightIter.mark();
        this.marked = this.markedR.getValues().get(SortMergeOperator.this.getRightColumnIndex());
        this.rightIter.reset();
      } else {
        this.rightIter = null;
      }

      this.leftRecord = null;
      this.rightRecord = null;
      this.nextRecord = null;
      this.isMark = true;

    }

    /**
     * Checks if there are more record(s) to yield
     *
     * @return true if this iterator has another record to yield, otherwise false
     */
    public boolean hasNext() {
      if (this.rightIter == null || this.leftIter == null) {
        return false;
      }
      if (this.nextRecord != null) {
        return true;
      }
      while (true) {
        if (this.leftRecord == null || this.rightRecord == null) {
          if (this.leftIter.hasNext() && this.rightIter.hasNext()) {
            this.leftRecord = this.leftIter.next();
            this.rightRecord = this.rightIter.next();
          } else {
            return false;
          }
        }
        this.leftData = this.leftRecord.getValues().get(SortMergeOperator.this.getLeftColumnIndex());
        this.rightData = this.rightRecord.getValues().get(SortMergeOperator.this.getRightColumnIndex());


        while (leftData.compareTo(rightData) < 0) {
            if (this.leftIter.hasNext()) {
              this.leftRecord = this.leftIter.next();
              this.leftData = this.leftRecord.getValues().get(SortMergeOperator.this.getLeftColumnIndex());
            } else {
              return false;
            }
          }

        while (leftData.compareTo(rightData) > 0) {
          if (this.rightIter.hasNext()) {
            this.rightRecord = this.rightIter.next();
            this.rightData = this.rightRecord.getValues().get(SortMergeOperator.this.getRightColumnIndex());
          } else {
            return false;
          }
        }

        while (this.leftData.compareTo(this.rightData) == 0) {
          if (this.marked.compareTo(rightData) != 0) {
            this.rightIter.mark();
            this.marked = this.rightData;
          }
          List<DataBox> leftValues = new ArrayList<DataBox>(this.leftRecord.getValues());
          List<DataBox> rightValues = new ArrayList<DataBox>(rightRecord.getValues());
          leftValues.addAll(rightValues);
          this.nextRecord = new Record(leftValues);

          if (this.rightIter.hasNext()) {
            this.rightRecord = this.rightIter.next();
            this.rightData = this.rightRecord.getValues().get(SortMergeOperator.this.getRightColumnIndex());

            if (this.rightData.compareTo(this.leftData) != 0) {
              this.rightIter.reset();
              this.rightRecord = this.rightIter.next();
              this.rightData = this.rightRecord.getValues().get(SortMergeOperator.this.getRightColumnIndex());

              if (this.leftIter.hasNext()) {
                this.leftRecord = this.leftIter.next();
                this.leftData = this.leftRecord.getValues().get(SortMergeOperator.this.getLeftColumnIndex());
              } else {
                this.leftRecord = null;
              }
            }
          } else {
            if (this.leftIter.hasNext()) {
              this.leftRecord = this.leftIter.next();
              this.leftData = this.leftRecord.getValues().get(SortMergeOperator.this.getLeftColumnIndex());
              this.rightIter.reset();
              this.rightRecord = this.rightIter.next();
              this.rightData = this.rightRecord.getValues().get(SortMergeOperator.this.getRightColumnIndex());
            }
            else {
              this.leftRecord = null;
              this.rightRecord = null;
            }
          }
          return true;
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


    private class LeftRecordComparator implements Comparator<Record> {
      public int compare(Record o1, Record o2) {
        return o1.getValues().get(SortMergeOperator.this.getLeftColumnIndex()).compareTo(
                o2.getValues().get(SortMergeOperator.this.getLeftColumnIndex()));
      }
    }

    private class RightRecordComparator implements Comparator<Record> {
      public int compare(Record o1, Record o2) {
        return o1.getValues().get(SortMergeOperator.this.getRightColumnIndex()).compareTo(
                o2.getValues().get(SortMergeOperator.this.getRightColumnIndex()));
      }
    }
  }
}
