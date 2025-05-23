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

package org.apache.iotdb.db.queryengine.plan.statement.metadata.pipe;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.auth.entity.PrivilegeType;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.auth.AuthorityChecker;
import org.apache.iotdb.db.queryengine.plan.analyze.QueryType;
import org.apache.iotdb.db.queryengine.plan.statement.IConfigStatement;
import org.apache.iotdb.db.queryengine.plan.statement.Statement;
import org.apache.iotdb.db.queryengine.plan.statement.StatementType;
import org.apache.iotdb.db.queryengine.plan.statement.StatementVisitor;
import org.apache.iotdb.rpc.TSStatusCode;

import java.util.Collections;
import java.util.List;

public class CreatePipePluginStatement extends Statement implements IConfigStatement {

  private final String pluginName;
  private final boolean ifNotExistsCondition;
  private final String className;
  private final String uriString;

  private boolean isTableModel = false;

  public CreatePipePluginStatement(
      String pluginName, boolean ifNotExistsCondition, String className, String uriString) {
    super();
    statementType = StatementType.CREATE_PIPEPLUGIN;
    this.pluginName = pluginName;
    this.ifNotExistsCondition = ifNotExistsCondition;
    this.className = className;
    this.uriString = uriString;
  }

  public void markIsTableModel(final boolean isTableModel) {
    this.isTableModel = isTableModel;
  }

  public boolean isTableModel() {
    return isTableModel;
  }

  public String getPluginName() {
    return pluginName;
  }

  public boolean hasIfNotExistsCondition() {
    return ifNotExistsCondition;
  }

  public String getClassName() {
    return className;
  }

  public String getUriString() {
    return uriString;
  }

  @Override
  public QueryType getQueryType() {
    return QueryType.WRITE;
  }

  @Override
  public List<PartialPath> getPaths() {
    return Collections.emptyList();
  }

  @Override
  public TSStatus checkPermissionBeforeProcess(String userName) {
    if (AuthorityChecker.SUPER_USER.equals(userName)) {
      return new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
    }
    return AuthorityChecker.getTSStatus(
        AuthorityChecker.checkSystemPermission(userName, PrivilegeType.USE_PIPE),
        PrivilegeType.USE_PIPE);
  }

  @Override
  public <R, C> R accept(StatementVisitor<R, C> visitor, C context) {
    return visitor.visitCreatePipePlugin(this, context);
  }
}
