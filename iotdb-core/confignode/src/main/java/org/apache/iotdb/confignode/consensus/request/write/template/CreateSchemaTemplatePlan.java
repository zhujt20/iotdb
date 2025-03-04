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

package org.apache.iotdb.confignode.consensus.request.write.template;

import org.apache.iotdb.confignode.consensus.request.ConfigPhysicalPlan;
import org.apache.iotdb.confignode.consensus.request.ConfigPhysicalPlanType;
import org.apache.iotdb.db.schemaengine.template.Template;

import org.apache.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;

public class CreateSchemaTemplatePlan extends ConfigPhysicalPlan {

  private byte[] templateData;

  public CreateSchemaTemplatePlan() {
    super(ConfigPhysicalPlanType.CreateSchemaTemplate);
  }

  public CreateSchemaTemplatePlan(final byte[] templateData) {
    this();
    this.templateData = templateData;
  }

  public byte[] getTemplateData() {
    return templateData;
  }

  public Template getTemplate() {
    final Template template = new Template();
    template.deserialize(ByteBuffer.wrap(templateData));
    return template;
  }

  @Override
  protected void serializeImpl(final DataOutputStream stream) throws IOException {
    stream.writeShort(getType().getPlanType());
    stream.writeInt(templateData.length);
    stream.write(templateData);
  }

  @Override
  protected void deserializeImpl(final ByteBuffer buffer) throws IOException {
    final int length = ReadWriteIOUtils.readInt(buffer);
    this.templateData = ReadWriteIOUtils.readBytes(buffer, length);
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final CreateSchemaTemplatePlan that = (CreateSchemaTemplatePlan) o;
    return Arrays.equals(that.templateData, templateData);
  }

  @Override
  public int hashCode() {
    return Arrays.hashCode(templateData);
  }
}
