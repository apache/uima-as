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

import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CCombo;
import org.eclipse.swt.custom.ScrolledComposite;
import org.eclipse.swt.layout.GridData;
import org.eclipse.swt.layout.GridLayout;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Layout;
import org.eclipse.swt.widgets.Text;
import org.eclipse.ui.forms.FormColors;
import org.eclipse.ui.forms.widgets.ColumnLayout;
import org.eclipse.ui.forms.widgets.FormToolkit;
import org.eclipse.ui.forms.widgets.Section;
import org.eclipse.ui.forms.widgets.TableWrapData;
import org.eclipse.ui.forms.widgets.TableWrapLayout;

/**
 * 
 *
 * TODO To change the template for this generated type comment go to
 * Window - Preferences - Java - Code Style - Code Templates
 */
public class FormSection {
       	
	public static class LabelAndObject {
		public Label	label;
		public Object	object;
	}
	
    public static Section createGridDataSection (FormToolkit toolkit, Composite parent,
            			int sectionStyle, String title, String description,
            			int marginWidth, int marginHeight,
            			int layoutStyle, int horizontalSpan, int verticalSpan) 
    {	
        // Create Section
        Section section = toolkit.createSection(parent, sectionStyle);
        section.setText(title);
        section.setDescription(description);
        section.marginWidth  = marginWidth;
        section.marginHeight = marginHeight;
        // toolkit.createCompositeSeparator(section);
        
	    GridData layoutData = new GridData(layoutStyle);
	    layoutData.horizontalSpan = horizontalSpan;
	    layoutData.verticalSpan   = verticalSpan;
	    section.setLayoutData(layoutData);
        
        return section;
    } // createGridDataSection   
    
    public static Section createTableWrapDataSection (FormToolkit toolkit, Composite parent,
            				int sectionStyle, String title, String description, 
	            			int marginWidth, int marginHeight,
	            			int align, int valign, int rowspan, int colspan) 
	{	
	    // Create Section
	    Section section = toolkit.createSection(parent, sectionStyle);
	    section.setText(title);
	    section.setDescription(description);
	    section.marginWidth  = marginWidth;
	    section.marginHeight = marginHeight;
	    toolkit.createCompositeSeparator(section);
	    
	    TableWrapData layoutData = new TableWrapData(align, valign, rowspan, colspan); 
        layoutData.grabHorizontal = true;
        layoutData.grabVertical   = true;
        section.setLayoutData(layoutData);
	    
//        final SectionPart spart = new SectionPart(section);
//        mform.addPart(spart);
//        spart.initialize (mform);  // Need this code. Otherwise, exception in SectionPart !!!
        
	    return section;
	} // createTableWrapDataSection

    public static Label createLabelAndLabel (FormToolkit toolkit, Composite parent,
	        String labelText, String textText, int textWidthHint, int textHeightHint) 
	{
	    Label label = toolkit.createLabel(parent, labelText);
	    label.setForeground(toolkit.getColors().getColor(FormColors.TITLE));
	    Label text = toolkit.createLabel(parent, textText); //$NON-NLS-1$
	    
	    FormSection.fillIntoGridOrTableLayout (parent, label, text, textWidthHint, textHeightHint);
	    
	    return text;
	} // createLabelAndLabel

    public static Text createLabelAndText (FormToolkit toolkit, Composite parent,
	        String labelText, String textText, int style, int textWidthHint, int textHeightHint) 
	{
	    Label label = toolkit.createLabel(parent, labelText);
	    label.setForeground(toolkit.getColors().getColor(FormColors.TITLE));
	    Text text = toolkit.createText(parent, textText, style); //$NON-NLS-1$
	    
	    FormSection.fillIntoGridOrTableLayout (parent, label, text, textWidthHint, textHeightHint);
	    
	    return text;
	} // createLabelAndText

    public static Text createLabelAndText (FormToolkit toolkit, Composite parent,
            Label label, String textText, int style, int textWidthHint, int textHeightHint) 
    {
        label.setForeground(toolkit.getColors().getColor(FormColors.TITLE));
        Text text = toolkit.createText(parent, textText, style); //$NON-NLS-1$
        
        FormSection.fillIntoGridOrTableLayout (parent, label, text, textWidthHint, textHeightHint);
        
        return text;
    } // createLabelAndText
    
    public static CCombo createLabelAndCCombo (FormToolkit toolkit, Composite parent,
            String labelText, int style) 
    {
        Label label = toolkit.createLabel(parent, labelText);
        label.setForeground(toolkit.getColors().getColor(FormColors.TITLE));
        CCombo ccombo = new CCombo (parent, style);
        toolkit.adapt(ccombo);
        
        FormSection.fillIntoGridOrTableLayout (parent, label, ccombo, 10, 0);
        
        return ccombo;
    } // createLabelAndCCombo
    
    public static void fillIntoGridLayout (Composite parent, 
    						Label label, Control control,
    						int widthHint, int heightHint) 
    {
    	Layout layout = parent.getLayout();
    	if (layout instanceof GridLayout) {
    		GridData gd;
    		int span = ((GridLayout) layout).numColumns;
    		int tspan = span - 1;
    		gd = new GridData();
    		if (control != null && (control.getStyle() & SWT.MULTI) > 0 ) {
    			gd.verticalAlignment = SWT.BEGINNING;
    		} else {
    			gd.verticalAlignment = SWT.CENTER;                
    		}            
    		if (control == null) {
    			gd.horizontalSpan = tspan;
    		}
    		label.setLayoutData(gd);
    		
    		if (control != null) {
    			gd = new GridData(GridData.HORIZONTAL_ALIGN_FILL);
    			gd.horizontalSpan = tspan;
    			gd.grabExcessHorizontalSpace = (tspan == 1);
    			gd.widthHint = widthHint;
    			// gd.verticalIndent = 4;
    			if (control != null && (control.getStyle() & SWT.MULTI) > 0 ) {
    				gd.heightHint = heightHint;
    			}
    			control.setLayoutData(gd);
    		}
    	}
    } // fillIntoGridLayout

	public static void fillIntoTableLayout (Composite parent, 
							Label label, Control control, int heightHint) 
	{
		Layout layout = parent.getLayout();

		if (layout instanceof TableWrapLayout) {
			TableWrapData td;
			int span = ((TableWrapLayout) layout).numColumns;
			int tspan = span - 1;
			td = new TableWrapData();
			td.valign = TableWrapData.TOP; // MIDDLE;
			if (control == null) {
				td.colspan = tspan;
			}
			label.setLayoutData(td);
			
			if (control != null) {
				td = new TableWrapData(TableWrapData.FILL);
				td.colspan = tspan;
				td.grabHorizontal = (tspan == 1);
				if (heightHint > 0) {
					td.heightHint = heightHint;
				}
				control.setLayoutData(td);
			}
		}
	} // fillIntoTableLayout
	   
	public static void fillIntoGridOrTableLayout (Composite parent, 
			Label label, Control control,
			int widthHint, int heightHint) 
	{
		Layout layout = parent.getLayout();
		if (layout instanceof GridLayout) {
			fillIntoGridLayout (parent, label, control, widthHint, heightHint);
		} else if (layout instanceof TableWrapLayout) {
			fillIntoTableLayout (parent, label, control, heightHint);
		}
	} // fillIntoGridOrTableLayout

    public static Composite createColumnLayoutContainer ( FormToolkit toolkit, Composite parent,
                                    int minNumColumns, int maxNumColumns )
    {   
        Composite container = toolkit.createComposite(parent, SWT.WRAP);
        ColumnLayout layout   = new ColumnLayout();
        layout.minNumColumns  = minNumColumns;
        layout.maxNumColumns  = maxNumColumns;
        container.setLayout(layout);    
        // toolkit.paintBordersFor(container);      
        
        return container;
    } // createColumnLayoutContainer    
    
    public static Composite createGridLayoutContainer ( FormToolkit toolkit, Composite parent,
							int numColumns, int marginWidth, int marginHeight )
	{	
		Composite container = toolkit.createComposite(parent, SWT.WRAP);
		GridLayout layout 	= new GridLayout();
		layout.numColumns	= numColumns;
		layout.marginWidth 	= marginWidth;
		layout.marginHeight = marginHeight;
		container.setLayout(layout);	
		// toolkit.paintBordersFor(container);		
		
		return container;
	} // createGridLayoutContainer    

    public static ScrolledComposite createGridLayoutScrolledContainer ( FormToolkit toolkit, Composite parent,
            int numColumns, int marginWidth, int marginHeight )
    {   
        // Composite container = toolkit.createComposite(parent, SWT.WRAP);
        ScrolledComposite container = new ScrolledComposite(parent, 
                SWT.WRAP|SWT.V_SCROLL|SWT.H_SCROLL);
        toolkit.adapt(container);
        GridLayout layout   = new GridLayout();
        layout.numColumns   = numColumns;
        layout.marginWidth  = marginWidth;
        layout.marginHeight = marginHeight;
        container.setLayout(layout);    
//      toolkit.paintBordersFor(container);      
        
        return container;
    } // createGridLayoutContainer    

    
    public static Composite createTableLayoutContainer ( FormToolkit toolkit, Composite parent,
                            		int numColumns, boolean makeColumnsEqualWidth, 
                                    int topMargin, int bottomMargin,
                                    int leftMargin, int rightMargin)
    {	
    	Composite container = toolkit.createComposite(parent);
    	TableWrapLayout layout 	= new TableWrapLayout();
    	layout.numColumns	= numColumns;
    	layout.makeColumnsEqualWidth = makeColumnsEqualWidth;
    	layout.topMargin    = topMargin;
    	layout.bottomMargin = bottomMargin;
        layout.leftMargin   = leftMargin;
        layout.rightMargin  = rightMargin;
    	
    	container.setLayout(layout);
    	
    	return container;
    } // createTableLayoutContainer    
    
    public static ScrolledComposite createTableLayoutScrolledContainer ( FormToolkit toolkit, Composite parent,
            int numColumns, boolean makeColumnsEqualWidth, 
            int topMargin, int bottomMargin,
            int leftMargin, int rightMargin)
    {   
//      Composite container = toolkit.createComposite(parent);
        ScrolledComposite container = new ScrolledComposite(parent, SWT.WRAP|SWT.V_SCROLL);
        toolkit.adapt(container);
        TableWrapLayout layout  = new TableWrapLayout();
        layout.numColumns   = numColumns;
        layout.makeColumnsEqualWidth = makeColumnsEqualWidth;
        layout.topMargin    = topMargin;
        layout.bottomMargin = bottomMargin;
        layout.leftMargin   = leftMargin;
        layout.rightMargin  = rightMargin;
        
        container.setLayout(layout);
        
        return container;
    } // createTableLayoutScrolledContainer    
    
}
