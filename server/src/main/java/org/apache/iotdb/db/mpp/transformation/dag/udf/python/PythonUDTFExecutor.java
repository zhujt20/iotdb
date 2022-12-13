package org.apache.iotdb.db.mpp.transformation.dag.udf.python;

import org.apache.iotdb.db.mpp.transformation.api.LayerPointReader;
import org.apache.iotdb.db.mpp.transformation.dag.udf.base.UDTFExecutor;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.udf.api.access.Row;
import org.apache.iotdb.udf.api.access.RowWindow;

import java.time.ZoneId;
import java.util.List;
import java.util.Map;

public class PythonUDTFExecutor extends UDTFExecutor {

  public PythonUDTFExecutor(String functionName, ZoneId zoneId) {
    super(functionName, zoneId);
  }

  @Override
  public void beforeStart(
      long queryId,
      float collectorMemoryBudgetInMB,
      List<String> childExpressions,
      List<TSDataType> childExpressionDataTypes,
      Map<String, String> attributes) {}

  @Override
  public void execute(Row row, boolean isCurrentRowNull) {}

  @Override
  public void execute(RowWindow rowWindow) {}

  @Override
  public void terminate() {}

  @Override
  public void beforeDestroy() {}

  @Override
  public LayerPointReader getPointCollector() {
    throw new UnsupportedOperationException();
  }

  /**
   * The strategy {@code MappableRowByRowAccessStrategy} is not supported by the Python UDF
   * framework.
   */
  @Override
  public void execute(Row row) {
    throw new UnsupportedOperationException();
  }

  /**
   * The strategy {@code MappableRowByRowAccessStrategy} is not supported by the Python UDF
   * framework.
   */
  @Override
  public Object getCurrentValue() {
    throw new UnsupportedOperationException();
  }
}
