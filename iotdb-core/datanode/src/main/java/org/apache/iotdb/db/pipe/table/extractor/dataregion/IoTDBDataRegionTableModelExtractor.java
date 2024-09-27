/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.db.pipe.table.extractor.dataregion;

import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.consensus.ConsensusFactory;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.pipe.extractor.dataregion.DataRegionListeningFilter;
import org.apache.iotdb.db.pipe.extractor.dataregion.IoTDBDataRegionExtractor;
import org.apache.iotdb.db.pipe.extractor.dataregion.historical.PipeHistoricalDataRegionTsFileExtractor;
import org.apache.iotdb.db.pipe.extractor.dataregion.realtime.PipeRealtimeDataRegionHeartbeatExtractor;
import org.apache.iotdb.db.pipe.extractor.dataregion.realtime.PipeRealtimeDataRegionHybridExtractor;
import org.apache.iotdb.db.pipe.extractor.dataregion.realtime.PipeRealtimeDataRegionLogExtractor;
import org.apache.iotdb.db.pipe.extractor.dataregion.realtime.PipeRealtimeDataRegionTsFileExtractor;
import org.apache.iotdb.db.storageengine.dataregion.wal.utils.WALMode;
import org.apache.iotdb.pipe.api.customizer.parameter.PipeParameterValidator;
import org.apache.iotdb.pipe.api.customizer.parameter.PipeParameters;
import org.apache.iotdb.pipe.api.exception.PipeException;

import org.apache.tsfile.utils.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;

import static org.apache.iotdb.commons.pipe.config.constant.PipeExtractorConstant.EXTRACTOR_EXCLUSION_DEFAULT_VALUE;
import static org.apache.iotdb.commons.pipe.config.constant.PipeExtractorConstant.EXTRACTOR_EXCLUSION_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeExtractorConstant.EXTRACTOR_INCLUSION_DEFAULT_VALUE;
import static org.apache.iotdb.commons.pipe.config.constant.PipeExtractorConstant.EXTRACTOR_INCLUSION_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeExtractorConstant.EXTRACTOR_MODE_DEFAULT_VALUE;
import static org.apache.iotdb.commons.pipe.config.constant.PipeExtractorConstant.EXTRACTOR_MODE_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeExtractorConstant.EXTRACTOR_MODE_QUERY_VALUE;
import static org.apache.iotdb.commons.pipe.config.constant.PipeExtractorConstant.EXTRACTOR_MODE_SNAPSHOT_VALUE;
import static org.apache.iotdb.commons.pipe.config.constant.PipeExtractorConstant.EXTRACTOR_REALTIME_ENABLE_DEFAULT_VALUE;
import static org.apache.iotdb.commons.pipe.config.constant.PipeExtractorConstant.EXTRACTOR_REALTIME_ENABLE_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeExtractorConstant.EXTRACTOR_REALTIME_MODE_BATCH_MODE_VALUE;
import static org.apache.iotdb.commons.pipe.config.constant.PipeExtractorConstant.EXTRACTOR_REALTIME_MODE_FILE_VALUE;
import static org.apache.iotdb.commons.pipe.config.constant.PipeExtractorConstant.EXTRACTOR_REALTIME_MODE_FORCED_LOG_VALUE;
import static org.apache.iotdb.commons.pipe.config.constant.PipeExtractorConstant.EXTRACTOR_REALTIME_MODE_HYBRID_VALUE;
import static org.apache.iotdb.commons.pipe.config.constant.PipeExtractorConstant.EXTRACTOR_REALTIME_MODE_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeExtractorConstant.EXTRACTOR_REALTIME_MODE_LOG_VALUE;
import static org.apache.iotdb.commons.pipe.config.constant.PipeExtractorConstant.EXTRACTOR_REALTIME_MODE_STREAM_MODE_VALUE;
import static org.apache.iotdb.commons.pipe.config.constant.PipeExtractorConstant.SOURCE_EXCLUSION_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeExtractorConstant.SOURCE_INCLUSION_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeExtractorConstant.SOURCE_MODE_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeExtractorConstant.SOURCE_REALTIME_ENABLE_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeExtractorConstant.SOURCE_REALTIME_MODE_KEY;
import static org.apache.iotdb.commons.pipe.datastructure.options.PipeInclusionOptions.hasAtLeastOneOption;
import static org.apache.iotdb.commons.pipe.datastructure.options.PipeInclusionOptions.optionsAreAllLegal;

public class IoTDBDataRegionTableModelExtractor extends IoTDBDataRegionExtractor {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(IoTDBDataRegionTableModelExtractor.class);

  @Override
  public void validate(final PipeParameterValidator validator) throws Exception {
    validator
        .validate(
            args -> optionsAreAllLegal((String) args),
            "The 'inclusion' string contains illegal path.",
            validator
                .getParameters()
                .getStringOrDefault(
                    Arrays.asList(EXTRACTOR_INCLUSION_KEY, SOURCE_INCLUSION_KEY),
                    EXTRACTOR_INCLUSION_DEFAULT_VALUE))
        .validate(
            args -> optionsAreAllLegal((String) args),
            "The 'inclusion.exclusion' string contains illegal path.",
            validator
                .getParameters()
                .getStringOrDefault(
                    Arrays.asList(EXTRACTOR_EXCLUSION_KEY, SOURCE_EXCLUSION_KEY),
                    EXTRACTOR_EXCLUSION_DEFAULT_VALUE))
        .validate(
            args -> hasAtLeastOneOption((String) args[0], (String) args[1]),
            "The pipe inclusion content can't be empty.",
            validator
                .getParameters()
                .getStringOrDefault(
                    Arrays.asList(EXTRACTOR_INCLUSION_KEY, SOURCE_INCLUSION_KEY),
                    EXTRACTOR_INCLUSION_DEFAULT_VALUE),
            validator
                .getParameters()
                .getStringOrDefault(
                    Arrays.asList(EXTRACTOR_EXCLUSION_KEY, SOURCE_EXCLUSION_KEY),
                    EXTRACTOR_EXCLUSION_DEFAULT_VALUE));

    final Pair<Boolean, Boolean> insertionDeletionListeningOptionPair =
        DataRegionListeningFilter.parseInsertionDeletionListeningOptionPair(
            validator.getParameters());
    if (insertionDeletionListeningOptionPair.getLeft().equals(false)
        && insertionDeletionListeningOptionPair.getRight().equals(false)) {
      return;
    }
    hasNoExtractionNeed = false;
    shouldExtractInsertion = insertionDeletionListeningOptionPair.getLeft();
    shouldExtractDeletion = insertionDeletionListeningOptionPair.getRight();

    if (insertionDeletionListeningOptionPair.getLeft().equals(true)
        && IoTDBDescriptor.getInstance()
            .getConfig()
            .getDataRegionConsensusProtocolClass()
            .equals(ConsensusFactory.RATIS_CONSENSUS)) {
      throw new PipeException(
          "The pipe cannot transfer data when data region is using ratis consensus.");
    }
    // TODO: Support deletion listening
    if (insertionDeletionListeningOptionPair.getRight().equals(true)) {
      throw new PipeException(
          "Table model extractor currently does not support deletion listening.");
    }

    // TODO: SQL matcher construction and validation

    constructHistoricalExtractor();
    constructRealtimeExtractor(validator.getParameters());

    historicalExtractor.validate(validator);
    realtimeExtractor.validate(validator);
  }

  private void constructHistoricalExtractor() {
    // Enable historical extractor by default
    historicalExtractor = new PipeHistoricalDataRegionTsFileExtractor();
  }

  private void constructRealtimeExtractor(final PipeParameters parameters)
      throws IllegalPathException {
    // Use heartbeat only extractor if disable realtime extractor
    if (!parameters.getBooleanOrDefault(
        Arrays.asList(EXTRACTOR_REALTIME_ENABLE_KEY, SOURCE_REALTIME_ENABLE_KEY),
        EXTRACTOR_REALTIME_ENABLE_DEFAULT_VALUE)) {
      realtimeExtractor = new PipeRealtimeDataRegionHeartbeatExtractor();
      LOGGER.info(
          "Pipe: '{}' is set to false, use heartbeat realtime extractor.",
          EXTRACTOR_REALTIME_ENABLE_KEY);
      return;
    }

    // Use heartbeat only extractor if enable snapshot mode
    final String extractorModeValue =
        parameters.getStringOrDefault(
            Arrays.asList(EXTRACTOR_MODE_KEY, SOURCE_MODE_KEY), EXTRACTOR_MODE_DEFAULT_VALUE);
    if (extractorModeValue.equalsIgnoreCase(EXTRACTOR_MODE_QUERY_VALUE)
        || extractorModeValue.equalsIgnoreCase(EXTRACTOR_MODE_SNAPSHOT_VALUE)) {
      realtimeExtractor = new PipeRealtimeDataRegionHeartbeatExtractor();
      LOGGER.info(
          "Pipe: '{}' is set to {}, use heartbeat realtime extractor.",
          EXTRACTOR_MODE_KEY,
          EXTRACTOR_MODE_SNAPSHOT_VALUE);
      return;
    }

    // Use hybrid mode by default
    if (!parameters.hasAnyAttributes(EXTRACTOR_REALTIME_MODE_KEY, SOURCE_REALTIME_MODE_KEY)) {
      checkWalEnable(parameters);
      realtimeExtractor = new PipeRealtimeDataRegionHybridExtractor();
      LOGGER.info(
          "Pipe: '{}' is not set, use hybrid mode by default.", EXTRACTOR_REALTIME_MODE_KEY);
      return;
    }

    switch (parameters.getStringByKeys(EXTRACTOR_REALTIME_MODE_KEY, SOURCE_REALTIME_MODE_KEY)) {
      case EXTRACTOR_REALTIME_MODE_FILE_VALUE:
      case EXTRACTOR_REALTIME_MODE_BATCH_MODE_VALUE:
        realtimeExtractor = new PipeRealtimeDataRegionTsFileExtractor();
        break;
      case EXTRACTOR_REALTIME_MODE_HYBRID_VALUE:
      case EXTRACTOR_REALTIME_MODE_LOG_VALUE:
      case EXTRACTOR_REALTIME_MODE_STREAM_MODE_VALUE:
        checkWalEnable(parameters);
        realtimeExtractor = new PipeRealtimeDataRegionHybridExtractor();
        break;
      case EXTRACTOR_REALTIME_MODE_FORCED_LOG_VALUE:
        checkWalEnable(parameters);
        realtimeExtractor = new PipeRealtimeDataRegionLogExtractor();
        break;
      default:
        checkWalEnable(parameters);
        realtimeExtractor = new PipeRealtimeDataRegionHybridExtractor();
        if (LOGGER.isWarnEnabled()) {
          LOGGER.warn(
              "Pipe: Unsupported extractor realtime mode: {}, create a hybrid extractor.",
              parameters.getStringByKeys(EXTRACTOR_REALTIME_MODE_KEY, SOURCE_REALTIME_MODE_KEY));
        }
    }
  }

  private void checkWalEnable(final PipeParameters parameters) throws IllegalPathException {
    if (Boolean.TRUE.equals(
            DataRegionListeningFilter.parseInsertionDeletionListeningOptionPair(parameters)
                .getLeft())
        && IoTDBDescriptor.getInstance().getConfig().getWalMode().equals(WALMode.DISABLE)) {
      throw new PipeException(
          "The pipe cannot transfer realtime insertion if data region disables wal. Please set 'realtime.mode'='batch' in source parameters when enabling realtime transmission.");
    }
  }

  //////////////////////////// APIs provided for detecting stuck ////////////////////////////

  @Override
  public boolean isStreamMode() {
    return realtimeExtractor instanceof PipeRealtimeDataRegionHybridExtractor
        || realtimeExtractor instanceof PipeRealtimeDataRegionLogExtractor;
  }
}
