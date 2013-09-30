/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.uima.tools.internal.uima.util;

import org.eclipse.jface.dialogs.IMessageProvider;
import org.eclipse.ui.forms.widgets.Form;

public class FormMessage {
  // static final public String DDE_MSG_TOP_DESCRIPTOR_IS_NOT_SET = "topMessageIsNotSet";

  /** ********************************************************************** */

  static public void setMessage(Form form, String message, int msgType) {
    form.setMessage(message, msgType);
  }

  static public void setMessageInfo(Form form, String message) {
    form.setMessage(message, IMessageProvider.INFORMATION);
  }

  static public void setMessageWarning(Form form, String message) {
    form.setMessage(message, IMessageProvider.WARNING);
  }

  static public void setMessageError(Form form, String message) {
    form.setMessage(message, IMessageProvider.ERROR);
  }

}
