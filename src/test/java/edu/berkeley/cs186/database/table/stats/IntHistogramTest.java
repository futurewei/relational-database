package edu.berkeley.cs186.database.table.stats;

import edu.berkeley.cs186.database.StudentTest;
import edu.berkeley.cs186.database.StudentTestP2;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.junit.Assert.*;

import edu.berkeley.cs186.database.datatypes.IntDataType;
import edu.berkeley.cs186.database.query.QueryPlan.PredicateOperator;

public class IntHistogramTest {

  @Test(timeout=1000)
  public void testSimpleHistogram() {
    IntHistogram histogram = new IntHistogram();

    for (int i = 0; i < 10; i++) {
      histogram.addValue(i);
    }

    assertEquals(10, histogram.getEntriesInRange(0, 10));
  }

  @Test(timeout=1000)
  public void testComplexHistogram() {
    IntHistogram histogram = new IntHistogram();

    for (int i = 0; i < 40; i++) {
      histogram.addValue(i);
    }

    assertEquals(10, histogram.getEntriesInRange(0, 10));
    assertEquals(10, histogram.getEntriesInRange(10, 20));
    assertEquals(10, histogram.getEntriesInRange(20, 30));
    assertEquals(10, histogram.getEntriesInRange(30, 40));
    assertEquals(20, histogram.getEntriesInRange(20, 40));
    assertEquals(10, histogram.getEntriesInRange(15, 25));
    assertEquals(5, histogram.getEntriesInRange(25, 30));
  }

  @Test(timeout=1000)
  public void testHistogramExpand() {
    IntHistogram histogram = new IntHistogram();

    for (int i = 0; i < 10; i++) {
      histogram.addValue(i);
    }

    histogram.addValue(99);

    assertEquals(10, histogram.getAllBuckets().get(5).getCount());
    assertEquals(1, histogram.getAllBuckets().get(9).getCount());
  }



  @Test
  @Category(StudentTestP2.class)
  public void testMaxMIN() {
    IntHistogram histogram = new IntHistogram();

    for (int i = 0; i < 50; i++) {
      histogram.addValue(i);
      histogram.addValue(i);
    }

    assertEquals(50, histogram.getMaxValue());
    assertEquals(0, histogram.getMinValue());
  }


  @Test(timeout=1000)
  public void testComputeReductionFactor() {
    IntHistogram histogram = new IntHistogram();

    for (int i = 0; i < 50; i++) {
      histogram.addValue(i);
      histogram.addValue(i);
    }

    assertEquals(50, histogram.getNumDistinct());

    IntDataType equalsValue = new IntDataType(3);
    assertEquals(0.02f,
            histogram.computeReductionFactor(PredicateOperator.EQUALS,
                    equalsValue),
            0.001f);

    IntDataType lessThanValue = new IntDataType(25);
    assertEquals(0.5,
            histogram.computeReductionFactor(PredicateOperator.LESS_THAN,
                    lessThanValue),
            0.001f);

    IntDataType lessThanEqualsValue = new IntDataType(25);
    assertEquals(0.52,
            histogram.computeReductionFactor(PredicateOperator.LESS_THAN_EQUALS,
                    lessThanEqualsValue),
            0.001f);

    IntDataType greaterThanValue = new IntDataType(9);
    assertEquals(0.82,
            histogram.computeReductionFactor(PredicateOperator.GREATER_THAN,
                    greaterThanValue),
            0.001f);

    IntDataType greaterThanEqualsValue = new IntDataType(10);
    assertEquals(0.82,
            histogram.computeReductionFactor(PredicateOperator.GREATER_THAN_EQUALS,
                    greaterThanEqualsValue),
            0.001f);
  }

  @Test(timeout=1000)
  public void testCopyWithReduction() {
    IntHistogram histogram = new IntHistogram();

    for (int i = 0; i < 100; i++) {
      histogram.addValue(i);
    }

    assertEquals(100, histogram.getNumDistinct());

    IntHistogram copyHistogram = histogram.copyWithReduction(0.7f);

    assertEquals(70, copyHistogram.getEntriesInRange(0, 100));
    assertEquals(70, copyHistogram.getNumDistinct());
  }

  @Test(timeout=1000)
  public void testCopyWithPredicate() {
    IntHistogram histogram = new IntHistogram();

    for (int i = 0; i < 500; i++) {
      histogram.addValue(i);
    }

    assertEquals(500, histogram.getNumDistinct());

    IntDataType value = new IntDataType(320);
    IntHistogram copyHistogram = histogram.copyWithPredicate(PredicateOperator.LESS_THAN,
            value);

    assertEquals(320, copyHistogram.getEntriesInRange(0, 500));
    assertEquals(250, copyHistogram.getNumDistinct());
  }
}