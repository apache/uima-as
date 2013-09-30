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

package org.apache.uima.tools.internal.ui.forms;

import org.apache.uima.tools.debug.util.Trace;
import org.eclipse.jface.fieldassist.DecoratedField;
import org.eclipse.jface.fieldassist.FieldDecoration;
import org.eclipse.jface.fieldassist.FieldDecorationRegistry;
import org.eclipse.jface.fieldassist.IControlCreator;
import org.eclipse.jface.resource.JFaceResources;
import org.eclipse.swt.SWT;
import org.eclipse.swt.layout.GridData;
import org.eclipse.swt.layout.GridLayout;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Layout;
import org.eclipse.swt.widgets.Spinner;
import org.eclipse.swt.widgets.Text;
import org.eclipse.ui.forms.FormColors;
import org.eclipse.ui.forms.widgets.FormToolkit;
import org.eclipse.ui.forms.widgets.TableWrapData;
import org.eclipse.ui.forms.widgets.TableWrapLayout;


public class FormSection2 extends FormSection {
  
  public static final int       MAX_DECORATION_WIDTH    = 8; // Bug in 3.2, Fixed in 3.3
  /*
   * Image id's for JFace
   */
  private static final String IMG_DEC_FIELD_CONTENT_PROPOSAL = "org.eclipse.jface.fieldassist.IMG_DEC_FIELD_CONTENT_PROPOSAL"; //$NON-NLS-1$

  private static final String IMG_DEC_FIELD_REQUIRED = "org.eclipse.jface.fieldassist.IMG_DEC_FIELD_REQUIRED"; //$NON-NLS-1$

  private static final String IMG_DEC_FIELD_ERROR = "org.eclipse.jface.fieldassist.IMG_DEC_FIELD_ERROR"; //$NON-NLS-1$

  private static final String IMG_DEC_FIELD_WARNING = "org.eclipse.jface.fieldassist.IMG_DEC_FIELD_WARNING"; //$NON-NLS-1$

  /***************************************************************************/

  static protected void registerFieldDecoration (String id, String description, String imageId) {
    FieldDecorationRegistry.getDefault().registerFieldDecoration(id, description,
            imageId, JFaceResources.getImageRegistry());
  }
  
  static public FieldDecoration registerFieldDecoration(String id, String description) {
    registerFieldDecoration(id, description, IMG_DEC_FIELD_ERROR);
    
    // FieldDecoration deco = FieldDecorationRegistry.getDefault().getFieldDecoration(id);
    // Trace.err("Width max: " + FieldDecorationRegistry.getDefault().getMaximumDecorationWidth());
    // return deco;
    
    return FieldDecorationRegistry.getDefault().getFieldDecoration(id);
  }
  
  public static DecoratedField createLabelAndDecoratedText (final FormToolkit toolkit, Composite parent,
          String labelText, String textText, int style, int textWidthHint, int textHeightHint, 
          boolean grabH, int indentLeft) 
  {
      Label label = toolkit.createLabel(parent, labelText);
      label.setForeground(toolkit.getColors().getColor(FormColors.TITLE));
      
      DecoratedField field = new DecoratedField(parent, style, new IControlCreator() {

        public Control createControl(Composite parent1, int controlStyle) {
          Text t = toolkit.createText(parent1, "", controlStyle);
          return t;
        }      
      });
      ((Text) field.getControl()).setText(textText);
      toolkit.adapt(field.getLayoutControl(), false, false);

      fillIntoGridOrTableLayout (parent, label, field.getLayoutControl(), 
    		  textWidthHint, textHeightHint, grabH, indentLeft);
      
      return field;
  } // createLabelAndDecoratedText
  
  public static DecoratedField createLabelAndGridLayoutDecoratedContainer (final FormToolkit toolkit, Composite parent,
          String labelText, int numColumns, int marginWidth, int marginHeight )
  { 
    Label label = toolkit.createLabel(parent, labelText);
    label.setForeground(toolkit.getColors().getColor(FormColors.TITLE));
    
    DecoratedField field = new DecoratedField(parent, SWT.WRAP, new IControlCreator() {

      public Control createControl(Composite parent1, int controlStyle) {
        Composite container = toolkit.createComposite(parent1, controlStyle);
        return container;
      }      
    });
    Composite container = (Composite) field.getControl();
    GridLayout layout   = new GridLayout();
    layout.numColumns = numColumns;
    layout.marginWidth  = marginWidth;
    layout.marginHeight = marginHeight;
    container.setLayout(layout);  
    toolkit.adapt(field.getLayoutControl(), false, false);

    FormSection.fillIntoGridOrTableLayout (parent, label, field.getLayoutControl(), 0, 0);
    
    return field;
  } // createLabelAndGridLayoutDecoratedContainer    


  /***************************************************************************/
  
  /**
   * Support left indentation
   * 
   * @param toolkit
   * @param parent
   * @param labelText
   * @param textText
   * @param textWidthHint
   * @param textHeightHint
   * @param indentLeft
   * @return Label
   */
  public static Label createLabelAndLabel (FormToolkit toolkit, Composite parent,
          String labelText, String textText, int textWidthHint, int textHeightHint, 
          int indentLeft) 
  {
      Label label = toolkit.createLabel(parent, labelText);
      label.setForeground(toolkit.getColors().getColor(FormColors.TITLE));
      Label text = toolkit.createLabel(parent, textText); //$NON-NLS-1$
      
      fillIntoGridOrTableLayout (parent, label, text, textWidthHint, textHeightHint,
              false, indentLeft);
      
      return text;
  } // createLabelAndLabel

  /***************************************************************************/
  
  static public Spinner createLabelAndSpinner(FormToolkit toolkit, Composite parent,
          String labelText, int style, int min, int max, boolean grabH) {
    return createLabelAndSpinner(toolkit, parent, labelText, style, min, max, grabH, 0);
  }

  static public Spinner createLabelAndSpinner(FormToolkit toolkit, Composite parent,
          String labelText, int style, int min, int max, boolean grabH, int indentLeft) {
    Label label = toolkit.createLabel(parent, labelText);
    return createLabelAndSpinner(toolkit, parent, label, style, min, max, grabH, indentLeft);
  }

  static public Spinner createLabelAndSpinner(FormToolkit toolkit, Composite parent,
          Label label, int style, int min, int max, boolean grabH) {
    return createLabelAndSpinner(toolkit, parent, label, style, min, max, grabH, 0);
  }

  static public Spinner createLabelAndSpinner(FormToolkit toolkit, Composite parent, Label label,
          int style, int min, int max, boolean grabH, int indentLeft) {
    label.setForeground(toolkit.getColors().getColor(FormColors.TITLE));
    Spinner control = new Spinner(parent, style);
    toolkit.adapt(control, false, false);
    control.setMinimum(min);
    control.setMaximum(max);
    fillIntoGridOrTableLayout(parent, label, control, 20, 0, grabH, indentLeft);

    return control;
  }

  /***************************************************************************/

  public static void fillIntoGridLayout(Composite parent, Label label, Control control,
          int widthHint, int heightHint, boolean grabH, int indentLeft) {
    Layout layout = parent.getLayout();
    if (layout instanceof GridLayout) {
      GridData gd;
      int span = ((GridLayout) layout).numColumns;
      int colspan = span - 1;
      gd = new GridData();
      if (control != null && (control.getStyle() & SWT.MULTI) > 0) {
        gd.verticalAlignment = SWT.BEGINNING;
      } else {
        gd.verticalAlignment = SWT.CENTER;
      }
      label.setLayoutData(gd);
      if (control == null) {
        gd.horizontalSpan = colspan;
      } else {
        // Set LayoutData for Control
        if (grabH) {
          gd = new GridData(GridData.HORIZONTAL_ALIGN_FILL);
        } else {
          gd = new GridData();
        }
        if (indentLeft > 0) {
          // Set indent
          gd.horizontalIndent = indentLeft;
        }
        gd.horizontalSpan = colspan;
        if (grabH) {
          // Grab horizontal ONLY when colspan is 1 (i.e., 2 colums)
          gd.grabExcessHorizontalSpace = (colspan == 1);
        } else {
          gd.grabExcessHorizontalSpace = false;
        }
        gd.widthHint = widthHint;
        // gd.verticalIndent = 4;
        if (control != null && (control.getStyle() & SWT.MULTI) > 0) {
          gd.heightHint = heightHint;
        }
        control.setLayoutData(gd);
      }
    }
  } // fillIntoGridLayout

  public static void fillIntoTableLayout(Composite parent, Label label, Control control,
		  int heightHint, boolean grabH, int indentLeft) {
    Layout layout = parent.getLayout();

    if (layout instanceof TableWrapLayout) {
      TableWrapData td;
      int span = ((TableWrapLayout) layout).numColumns;
      int colspan = span - 1;
      td = new TableWrapData();
      td.valign = TableWrapData.TOP; // MIDDLE;
      label.setLayoutData(td);
      if (control == null) {
        td.colspan = colspan;
      } else {
        // Set LayoutData for Control
        if (grabH) {
          td = new TableWrapData(TableWrapData.FILL);
        } else {
          td = new TableWrapData();
        }
        if (indentLeft > 0) {
          // Set indent
          td.indent = indentLeft;
        }
        td.colspan = colspan;
        if (grabH) {
          // Grab horizontal ONLY when colspan is 1 (i.e., 2 colums)
          td.grabHorizontal = (colspan == 1);
        } else {
          td.grabHorizontal = false;
        }
        if (heightHint > 0) {
            td.heightHint = heightHint;
        }
        control.setLayoutData(td);
      }
    }
  } // fillIntoTableLayout

  public static void fillIntoGridOrTableLayout(Composite parent, Label label, Control control,
          int widthHint, int heightHint, boolean grabH, int indentLeft) {
    Layout layout = parent.getLayout();
    if (layout instanceof GridLayout) {
      fillIntoGridLayout(parent, label, control, widthHint, heightHint, grabH, indentLeft);
    } else if (layout instanceof TableWrapLayout) {
      fillIntoTableLayout(parent, label, control, heightHint, grabH, indentLeft);
    }
  } // fillIntoGridOrTableLayout

}
