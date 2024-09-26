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

package org.apache.iotdb.db.pipe.agent.plugin.dataregion;

import org.apache.iotdb.commons.pipe.agent.plugin.builtin.BuiltinPipePlugin;
import org.apache.iotdb.commons.pipe.agent.plugin.builtin.extractor.donothing.DoNothingExtractor;
import org.apache.iotdb.commons.pipe.agent.plugin.constructor.PipeExtractorConstructor;
import org.apache.iotdb.commons.pipe.agent.plugin.meta.DataNodePipePluginMetaKeeper;
import org.apache.iotdb.commons.pipe.config.constant.PipeExtractorConstant;
import org.apache.iotdb.commons.pipe.config.constant.SystemConstant;
import org.apache.iotdb.db.pipe.extractor.dataregion.IoTDBDataRegionExtractor;
import org.apache.iotdb.db.pipe.table.extractor.dataregion.IoTDBDataRegionTableModelExtractor;
import org.apache.iotdb.pipe.api.PipeExtractor;
import org.apache.iotdb.pipe.api.PipePlugin;
import org.apache.iotdb.pipe.api.customizer.parameter.PipeParameters;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Supplier;

class PipeDataRegionExtractorConstructor extends PipeExtractorConstructor {

  // TODO: consider refactor the plugin constructors to a more generic way
  protected final Map<String, Supplier<PipePlugin>> tableModelPluginConstructors = new HashMap<>();

  PipeDataRegionExtractorConstructor(DataNodePipePluginMetaKeeper pipePluginMetaKeeper) {
    super(pipePluginMetaKeeper);
  }

  @Override
  protected void initConstructors() {
    pluginConstructors.put(
        BuiltinPipePlugin.DO_NOTHING_EXTRACTOR.getPipePluginName(), DoNothingExtractor::new);
    pluginConstructors.put(
        BuiltinPipePlugin.IOTDB_EXTRACTOR.getPipePluginName(), IoTDBDataRegionExtractor::new);

    pluginConstructors.put(
        BuiltinPipePlugin.DO_NOTHING_SOURCE.getPipePluginName(), DoNothingExtractor::new);
    pluginConstructors.put(
        BuiltinPipePlugin.IOTDB_SOURCE.getPipePluginName(), IoTDBDataRegionExtractor::new);

    // Plugins for table model only
    tableModelPluginConstructors.put(
        BuiltinPipePlugin.DO_NOTHING_EXTRACTOR.getPipePluginName(), DoNothingExtractor::new);
    tableModelPluginConstructors.put(
        BuiltinPipePlugin.IOTDB_EXTRACTOR.getPipePluginName(),
        IoTDBDataRegionTableModelExtractor::new);

    tableModelPluginConstructors.put(
        BuiltinPipePlugin.DO_NOTHING_SOURCE.getPipePluginName(), DoNothingExtractor::new);
    tableModelPluginConstructors.put(
        BuiltinPipePlugin.IOTDB_SOURCE.getPipePluginName(),
        IoTDBDataRegionTableModelExtractor::new);
  }

  @Override
  public final PipeExtractor reflectPlugin(PipeParameters extractorParameters) {
    return extractorParameters
            .getStringOrDefault(
                SystemConstant.SESSION_MODEL_KEY, SystemConstant.SESSION_MODEL_TREE_VALUE)
            .equals(SystemConstant.SESSION_MODEL_TREE_VALUE)
        ? super.reflectPlugin(extractorParameters)
        : reflectTableModelPlugin(
            extractorParameters
                .getStringOrDefault(
                    Arrays.asList(
                        PipeExtractorConstant.EXTRACTOR_KEY, PipeExtractorConstant.SOURCE_KEY),
                    BuiltinPipePlugin.IOTDB_EXTRACTOR.getPipePluginName())
                .toLowerCase());
  }

  private PipeExtractor reflectTableModelPlugin(String pluginName) {
    return (PipeExtractor)
        tableModelPluginConstructors.getOrDefault(pluginName, () -> reflect(pluginName)).get();
  }
}
