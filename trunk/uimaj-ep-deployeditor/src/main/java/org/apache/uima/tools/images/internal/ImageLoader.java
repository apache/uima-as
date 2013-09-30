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

package org.apache.uima.tools.images.internal;

import org.eclipse.jface.resource.ImageDescriptor;
import org.eclipse.jface.resource.ImageRegistry;
import org.eclipse.swt.SWT;
import org.eclipse.swt.SWTError;
import org.eclipse.swt.graphics.Image;
import org.eclipse.swt.widgets.Display;
import org.eclipse.ui.plugin.AbstractUIPlugin;

/**
 *  A helper class to load images/icons from other plug-ins. 
 *
 */
public class ImageLoader 
{
    static final public String PLUGIN_ID_O_E_SEARCH = "org.eclipse.search";
    static final public String ICON_SEARCH_EXPANDALL    = "icons/full/elcl16/expandall.gif";
    static final public String ICON_SEARCH_EXPANDALL_E  = "icons/full/elcl16/expandall.gif";
    static final public String ICON_SEARCH_EXPANDALL_D  = "icons/full/dlcl16/expandall.gif";
    static final public String ICON_SEARCH_COLLAPSEALL    = "icons/full/elcl16/collapseall.gif";
    static final public String ICON_SEARCH_COLLAPSEALL_E  = "icons/full/elcl16/collapseall.gif";
    static final public String ICON_SEARCH_COLLAPSEALL_D  = "icons/full/dlcl16/collapseall.gif";
    static final public String ICON_SEARCH_PREV  = "icons/full/elcl16/search_prev.gif";
    static final public String ICON_SEARCH_NEXT  = "icons/full/elcl16/search_next.gif";
    
    // Icons from "org.eclipse.ui"
    static final public String PLUGIN_ID_O_E_UI = "org.eclipse.ui";
    static final public String ICON_FOLDER      = "icons/full/obj16/fldr_obj.gif";
    static final public String ICON_FILE        = "icons/full/obj16/file_obj.gif";
    static final public String ICON_ERROR_TASK  = "icons/full/obj16/error_tsk.gif";
    static final public String ICON_UI_VIEW_MENU    = "icons/full/elcl16/view_menu.gif";

    static final public String PLUGIN_ID_O_E_JUNIT = "org.eclipse.jdt.junit";
    static final public String ICON_DOC_OK         = "icons/full/obj16/testok.gif";
    static final public String ICON_DOC_ERROR      = "icons/full/obj16/testerr.gif";
    static final public String ICON_JUNIT_TEST     = "icons/full/obj16/test.gif";
        
    static final public String PLUGIN_ID_O_E_PDE_UI     = "org.eclipse.pde.ui";
    static final public String ICON_ERROR_LOG           = "icons/view16/error_log.gif";
    static final public String ICON_PDE_UI_REFRESH      = "icons/elcl16/refresh.gif";

    static final public String PLUGIN_ID_O_E_DEBUG_UI     = "org.eclipse.debug.ui";
    static final public String ICON_DEBUG_UI_RUNLAST_E    = "icons/full/elcl16/runlast_co.gif";
    static final public String ICON_DEBUG_UI_RUNLAST_D    = "icons/full/dlcl16/runlast_co.gif";
    static final public String ICON_DEBUG_UI_TERMINATE_E  = "icons/full/elcl16/terminate_co.gif";
    static final public String ICON_DEBUG_UI_TERMINATE_D  = "icons/full/dcl16/terminate_co.gif";

    // From "org.eclipse.ui.ide"
    static final public String PLUGIN_ID_O_E_UI_IDE       = "org.eclipse.ui.ide";
    static final public String ICON_UI_IDE_IMPORT_PREF    = "icons/full/obj16/importpref_obj.gif";
    static final public String ICON_UI_IDE_FILTER         = "icons/full/elcl16/filter_ps.gif";

    // "org.eclipse.jdt.ui"
    static final public String PLUGIN_ID_O_E_JDT_UI = "org.eclipse.jdt.debug.ui";
    static final public String ICON_JDT_UI_HIERARCHY      = "icons/full/elcl16/hierarchy_co.gif";

    // "org.eclipse.jdt.debug.ui"
    static final public String PLUGIN_ID_O_E_JDT_DEBUG_UI = "org.eclipse.jdt.debug.ui";
    static final public String ICON_JDT_DEBUG_UI_FEATURE  = "icons/full/elcl16/final_co.gif";

    /*************************************************************************/
    
    
    static final public String ICON_UIMA_TOOLS_TYPE             = "icons/full/obj16/type.gif";
    static final public String ICON_UIMA_TOOLS_TYPE_ERROR       = "icons/full/obj16/type_error.gif";
    static final public String ICON_UIMA_TOOLS_FEATURE          = ICON_JDT_DEBUG_UI_FEATURE;
    static final public String ICON_UIMA_TOOLS_FEATURE_ERROR    = ICON_ERROR_TASK;
    
    /*************************************************************************/
    
    static private ImageLoader    instance;
    protected ImageRegistry         imageRegistry;
    
    static public ImageLoader getInstance()
    {
        if (instance == null) {
            instance = new ImageLoader();
        }
        return instance;
    }

    public ImageDescriptor registerImageDescriptor (String key, 
                                        String pluginId, String iconFileName) 
    {
        ImageDescriptor imgDesc = AbstractUIPlugin.imageDescriptorFromPlugin(pluginId, iconFileName);
        if (imgDesc != null) {
            // Register 
            getImageRegistry().remove(key);
            getImageRegistry().put(key, imgDesc);
        }
        return imgDesc;
    }
    
    public ImageDescriptor getImageDescriptor(String key) {
        return getImageRegistry().getDescriptor(key);
    }

    public Image getImage(String key) {
        return getImageRegistry().get(key);
    }
    
    public void initializeImageRegistry ()
    {
        // Register COMMON Images
        ImageLoader.getInstance().registerImageDescriptor(ImageLoader.ICON_SEARCH_EXPANDALL_E, ImageLoader.PLUGIN_ID_O_E_SEARCH, ImageLoader.ICON_SEARCH_EXPANDALL_E);
        ImageLoader.getInstance().registerImageDescriptor(ImageLoader.ICON_SEARCH_EXPANDALL_D, ImageLoader.PLUGIN_ID_O_E_SEARCH, ImageLoader.ICON_SEARCH_EXPANDALL_D);
        ImageLoader.getInstance().registerImageDescriptor(ImageLoader.ICON_SEARCH_COLLAPSEALL_E, ImageLoader.PLUGIN_ID_O_E_SEARCH, ImageLoader.ICON_SEARCH_COLLAPSEALL_E);
        ImageLoader.getInstance().registerImageDescriptor(ImageLoader.ICON_SEARCH_COLLAPSEALL_D, ImageLoader.PLUGIN_ID_O_E_SEARCH, ImageLoader.ICON_SEARCH_COLLAPSEALL_D);
        ImageLoader.getInstance().registerImageDescriptor(ImageLoader.ICON_SEARCH_PREV, ImageLoader.PLUGIN_ID_O_E_SEARCH, ImageLoader.ICON_SEARCH_PREV);
        ImageLoader.getInstance().registerImageDescriptor(ImageLoader.ICON_SEARCH_NEXT, ImageLoader.PLUGIN_ID_O_E_SEARCH, ImageLoader.ICON_SEARCH_NEXT);

        ImageLoader.getInstance().registerImageDescriptor(ImageLoader.ICON_PDE_UI_REFRESH, ImageLoader.PLUGIN_ID_O_E_PDE_UI, ImageLoader.ICON_PDE_UI_REFRESH);

        ImageLoader.getInstance().registerImageDescriptor(ImageLoader.ICON_UI_IDE_IMPORT_PREF, ImageLoader.PLUGIN_ID_O_E_UI_IDE, ImageLoader.ICON_UI_IDE_IMPORT_PREF);
        ImageLoader.getInstance().registerImageDescriptor(ImageLoader.ICON_UI_IDE_FILTER, ImageLoader.PLUGIN_ID_O_E_UI_IDE, ImageLoader.ICON_UI_IDE_FILTER);
      
        ImageLoader.getInstance().registerImageDescriptor(ImageLoader.ICON_UI_VIEW_MENU, ImageLoader.PLUGIN_ID_O_E_UI, ImageLoader.ICON_UI_VIEW_MENU);

        ImageLoader.getInstance().registerImageDescriptor(ImageLoader.ICON_UIMA_TOOLS_FEATURE, ImageLoader.PLUGIN_ID_O_E_JDT_DEBUG_UI, ImageLoader.ICON_UIMA_TOOLS_FEATURE);

        ImageLoader.getInstance().registerImageDescriptor(ImageLoader.ICON_JDT_UI_HIERARCHY, ImageLoader.PLUGIN_ID_O_E_JDT_UI, ImageLoader.ICON_JDT_UI_HIERARCHY);
        
    }
    
    public void dispose() {
        if (imageRegistry != null) {
            imageRegistry.dispose();
        }        
    }
    
    /*************************************************************************/

    /*************************************************************************/
    
    static public Image createImage (String pluginId, String iconFileName) 
    {
        ImageDescriptor imgDesc = AbstractUIPlugin.imageDescriptorFromPlugin(pluginId, iconFileName);
        if (imgDesc != null) {
            return imgDesc.createImage();
        }
        return null;
    }

    /*************************************************************************/
    
    protected ImageRegistry createImageRegistry() 
    {        
        // If we are in the UI Thread use that
        if (Display.getCurrent() != null) {
            return new ImageRegistry(Display.getCurrent());
        } else {
            // Invalid thread access if it is not the UI Thread 
            // and the workbench is not created.
            throw new SWTError(SWT.ERROR_THREAD_INVALID_ACCESS);
        }
    }

    protected ImageRegistry getImageRegistry() 
    {
        if (imageRegistry == null) {
            imageRegistry = createImageRegistry();
            // initializeImageRegistry(imageRegistry);
        }
        return imageRegistry;
    }

    /*************************************************************************/
    
}
