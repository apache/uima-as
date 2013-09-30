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
import org.eclipse.swt.graphics.Rectangle;
import org.eclipse.swt.widgets.TreeItem;

public class GenericHoverManager extends AbstractHoverInformationControlManager {

  // TODO Create a mapping from Control (used getSubjectControl()) to Owner
  //      so that this GenericHoverManager can be re-used with multiple controls.
  private IGenericHoverOwner owner;

  public GenericHoverManager(IGenericHoverOwner hoverOwner, IInformationControlCreator creator) {
    super(creator);
    this.owner = hoverOwner;
  }

  @Override
  protected void computeInformation() {
    Point pt = getHoverEventLocation();
    // Call the hover owner to set the information to be shown
    owner.computeInformation(this, pt);
  }
  
  /**
   * Sets the parameters of the information to be displayed. These are the information 
   * itself and the area for which the given information is valid. This so called 
   * subject area is a graphical region of the information control's subject control. 
   * This method calls <code>presentInformation()</code>
   * to trigger the presentation of the computed information.
   *
   * @param information the information
   * @param subjectArea the subject area
   */
  public void setDisplayedInformation(Object information, Rectangle subjectArea) {
    super.setInformation(information, subjectArea);
  }

}
