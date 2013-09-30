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

package org.apache.uima.dde.internal.hover;

import org.eclipse.jface.text.AbstractHoverInformationControlManager;
import org.eclipse.jface.text.IInformationControlCreator;
import org.eclipse.jface.viewers.TreeViewer;
import org.eclipse.swt.events.MouseEvent;
import org.eclipse.swt.events.MouseMoveListener;
import org.eclipse.swt.graphics.Point;
import org.eclipse.swt.widgets.TreeItem;

public class HoverManager extends AbstractHoverInformationControlManager {

  private TreeViewer viewer;

  public HoverManager(TreeViewer viewer, IInformationControlCreator creator) {
    super(creator);
    this.viewer = viewer;
  }

  @Override
  protected void computeInformation() {
    Point pt = getHoverEventLocation();
    // Trace.err("x: " + pt.x + " y: " + pt.y);
    TreeItem item = viewer.getTree().getItem(pt);
    if (item != null) {
      // Trace.err("Hover");
      setInformation(item, item.getBounds());
    } else {
      // Trace.err("NOT Hover");
      setInformation(null, null);
    }
  }

}
