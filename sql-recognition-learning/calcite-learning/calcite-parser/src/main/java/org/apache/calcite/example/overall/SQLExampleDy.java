package org.apache.calcite.example.overall;

import org.apache.calcite.linq4j.*;
import org.apache.calcite.schema.*;
import org.apache.calcite.linq4j.function.*;
import org.apache.calcite.adapter.enumerable.*;
import org.apache.calcite.runtime.*;

public class SQLExampleDy {

    public static class Record1_0 implements java.io.Serializable {
        public java.math.BigDecimal f0;

        public Record1_0() {
        }

        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof Record1_0)) {
                return false;
            }
            return java.util.Objects.equals(this.f0, ((Record1_0) o).f0);
        }

        public int hashCode() {
            int h = 0;
            h = Utilities.hash(h, this.f0);
            return h;
        }

        public int compareTo(Record1_0 that) {
            final int c;
            c = Utilities.compareNullsLast(this.f0, that.f0);
            if (c != 0) {
                return c;
            }
            return 0;
        }

        public String toString() {
            return "{f0=" + this.f0 + "}";
        }

    }

    public Enumerable bind(final org.apache.calcite.DataContext root) {
        final Enumerable _inputEnumerable = Schemas.enumerable((ScannableTable) root.getRootSchema().getSubSchema("s").getTable("users"), root);
        final AbstractEnumerable left = new AbstractEnumerable() {
            public Enumerator enumerator() {
                return new Enumerator() {
                    public final Enumerator inputEnumerator = _inputEnumerable.enumerator();

                    public void reset() {
                        inputEnumerator.reset();
                    }

                    public boolean moveNext() {
                        while (inputEnumerator.moveNext()) {
                            final int input_value = SqlFunctions.toInt(((Object[]) inputEnumerator.current())[2]);
                            final Integer value_dynamic_param = (Integer) root.get("?0");
                            final Boolean binary_call_value = value_dynamic_param == null ? (Boolean) null : Boolean.valueOf(input_value >= value_dynamic_param.intValue());
                            final boolean binary_call_isNull = binary_call_value == null;
                            final Boolean logical_and_value = (binary_call_isNull ? true : binary_call_value) && input_value <= 30 ? (binary_call_isNull ? (Boolean) null : Boolean.TRUE) : Boolean.FALSE;
                            if (logical_and_value != null && SqlFunctions.toBoolean(logical_and_value)) {
                                return true;
                            }
                        }
                        return false;
                    }

                    public void close() {
                        inputEnumerator.close();
                    }

                    public Object current() {
                        final Object[] current = (Object[]) inputEnumerator.current();
                        final Object input_value = current[0];
                        final Object input_value0 = current[1];
                        final Object input_value1 = current[2];
                        return new Object[]{
                                input_value,
                                input_value0,
                                input_value1};
                    }

                };
            }

        };
        final Enumerable _inputEnumerable0 = left.hashJoin(Schemas.enumerable((ScannableTable) root.getRootSchema()
                        .getSubSchema("s")
                        .getTable("orders"), root), new Function1() {
                    public String apply(Object[] v1) {
                        return v1[0] == null ? (String) null : v1[0].toString();
                    }

                    public Object apply(Object v1) {
                        return apply(
                                (Object[]) v1);
                    }
                }
                , new Function1() {
                    public String apply(Object[] v1) {
                        return v1[1] == null ? (String) null : v1[1].toString();
                    }

                    public Object apply(Object v1) {
                        return apply(
                                (Object[]) v1);
                    }
                }
                , new Function2() {
                    public Object[] apply(Object[] left, Object[] right) {
                        return new Object[]{
                                left[0],
                                left[1],
                                left[2],
                                right[0],
                                right[1],
                                right[2],
                                right[3]};
                    }

                    public Object[] apply(Object left, Object right) {
                        return apply(
                                (Object[]) left,
                                (Object[]) right);
                    }
                }
                , null, false, false, null);
        final AbstractEnumerable child = new AbstractEnumerable() {
            public Enumerator enumerator() {
                return new Enumerator() {
                    public final Enumerator inputEnumerator = _inputEnumerable0.enumerator();

                    public void reset() {
                        inputEnumerator.reset();
                    }

                    public boolean moveNext() {
                        return inputEnumerator.moveNext();
                    }

                    public void close() {
                        inputEnumerator.close();
                    }

                    public Object current() {
                        final Object[] current = (Object[]) inputEnumerator.current();
                        final Object input_value = current[0];
                        final Object input_value0 = current[1];
                        final Object input_value1 = current[2];
                        final Object input_value2 = current[6];
                        return new Object[]{
                                input_value,
                                input_value0,
                                input_value1,
                                input_value2};
                    }

                };
            }

        };
        java.util.List accumulatorAdders = new java.util.LinkedList();
        accumulatorAdders.add(new Function2() {
                                  public Record1_0 apply(Record1_0 acc, Object[] in) {
                                      final java.math.BigDecimal input_value = in[3] == null ? (java.math.BigDecimal) null : SqlFunctions.toBigDecimal(in[3]);
                                      if (input_value != null) {
                                          final java.math.BigDecimal input_value0 = in[3] == null ? (java.math.BigDecimal) null : SqlFunctions.toBigDecimal(in[3]);
                                          acc.f0 = acc.f0.add(input_value0);
                                      }
                                      return acc;
                                  }

                                  public Record1_0 apply(Object acc, Object in) {
                                      return apply(
                                              (Record1_0) acc,
                                              (Object[]) in);
                                  }
                              }
        );
        AggregateLambdaFactory lambdaFactory = new BasicAggregateLambdaFactory(
                new Function0() {
                    public Object apply() {
                        java.math.BigDecimal a0s0;
                        a0s0 = java.math.BigDecimal.valueOf(0L);
                        Record1_0 record0;
                        record0 = new Record1_0();
                        record0.f0 = a0s0;
                        return record0;
                    }
                }
                ,
                accumulatorAdders);
        return child.groupBy(new Function1() {
                                 public java.util.List apply(Object[] a0) {
                                     return FlatLists.of(a0[0] == null ? (String) null : a0[0].toString(), a0[1] == null ? (String) null : a0[1].toString(), SqlFunctions.toInt(a0[2]));
                                 }

                                 public Object apply(Object a0) {
                                     return apply(
                                             (Object[]) a0);
                                 }
                             }
                , lambdaFactory.accumulatorInitializer()
                , lambdaFactory.accumulatorAdder()
                , lambdaFactory.resultSelector(new Function2() {
                                                   public Object[] apply(FlatLists.ComparableList key, Record1_0 acc) {
                                                       return new Object[]{
                                                               key.get(0) == null ? (String) null : key.get(0).toString(),
                                                               key.get(1) == null ? (String) null : key.get(1).toString(),
                                                               SqlFunctions.toInt(key.get(2)),
                                                               acc.f0};
                                                   }

                                                   public Object[] apply(Object key, Object acc) {
                                                       return apply(
                                                               (FlatLists.ComparableList) key,
                                                               (Record1_0) acc);
                                                   }
                                               }
                )).orderBy(new Function1() {
                               public String apply(Object[] v) {
                                   return v[0] == null ? (String) null : v[0].toString();
                               }

                               public Object apply(Object v) {
                                   return apply(
                                           (Object[]) v);
                               }
                           }
                , Functions.nullsComparator(false, false));
    }


    public Class getElementType() {
        return java.lang.Object[].class;
    }


}
