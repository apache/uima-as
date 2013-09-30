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

import java.net.URL;

import org.eclipse.jface.resource.ImageDescriptor;
import org.eclipse.jface.resource.ImageRegistry;
import org.eclipse.swt.SWT;
import org.eclipse.swt.SWTError;
import org.eclipse.swt.graphics.Image;
import org.eclipse.swt.widgets.Display;


/**
 * 
 *
 */
public class ImageRegistryUtil {

    static private ImageRegistryUtil    instance = null;
    private ImageRegistry               imageRegistry = null;
    
    protected ImageRegistryUtil() {
        super();
        // TODO Auto-generated constructor stub
    }
    
    static public ImageRegistryUtil getInstance()
    {
        if (instance == null) {
            instance = new ImageRegistryUtil();
        }
        return instance;
    }
    
    public ImageDescriptor getImageDescriptor(String key) {
        return getImageRegistry().getDescriptor(key);
    }

    public Image getImage(String key) {
        return getImageRegistry().get(key);
    }
    
    /*************************************************************************/
    
    public static final String IMG_FORM_BG      = "formBg";
    public static final String IMG_LARGE        = "large";
    public static final String IMG_HORIZONTAL   = "th_horizontal.gif";
    public static final String IMG_VERTICAL     = "vertical";
    public static final String IMG_SAMPLE       = "sample";
    public static final String IMG_HELP         = "help";
    public static final String IMG_VIEW_MENU    = "view_menu1";
    public static final String IMG_TYPE         = "type";
    public static final String IMG_TAE          = "tae";
    public static final String IMG_FEATURE      = "feature";
    public static final String IMG_REFRESH      = "refresh";
    public static final String IMG_ADD_TYPES    = "add types";
    public static final String IMG_IMPORT_TYPE  = "import";
    public static final String IMG_EXPORT_TYPE  = "export";
    public static final String IMG_EDIT_TYPE    = "edit";
    public static final String IMG_DELETE       = "delete";
    public static final String IMG_REPOSITORY   = "repository";
    public static final String IMG_REPOSITORY_BIG  = "repository_big";
    public static final String IMG_SEARCH       = "search.gif";
    public static final String IMG_TS_FOLDER    = "ts_folder";
    public static final String IMG_PEAR         = "pear";
    public static final String IMG_XML          = "xml";
    public static final String IMG_CHECK_IN     = "checkIn";
    public static final String IMG_REPOSITORY_SYNC  = "repositorySync";
    public static final String IMG_EXPAND_ALL   = "expandall.gif";
    public static final String IMG_COLLAPSE_ALL = "collapseall.gif";
    public static final String IMG_FULL_NAME    = "filter_history.gif";
    public static final String IMG_FILTER       = "filter_history.gif";
    public static final String IMG_RUN          = "o.e.debug.ui.elcl16/run_exc.gif";
    public static final String IMG_PAUSE        = "o.e.debug.ui.elcl16/suspend_co.gif";
    public static final String IMG_STOP         = "o.e.debug.ui.elcl16/terminate_co.gif";
    public static final String IMG_FILE_OBJ     = "o.e.jdt.ui.icons.obj16/file_obj.gif";
    public static final String IMG_SEARCH_NEXT  = "o.e.search.icons.elcl16/search_next.gif";
    public static final String IMG_SEARCH_PREV  = "o.e.search.icons.elcl16/search_prev.gif";
    public static final String IMG_IMPORT_PREF  = "o.e.ui.ide.obj16/importpref_obj.gif";
    
    protected void initializeImageRegistry(ImageRegistry registry) {
        registerImage(registry, IMG_FORM_BG,    "form_banner.gif");
        registerImage(registry, IMG_LARGE,      "large_image.gif");
        registerImage(registry, IMG_HORIZONTAL, "th_horizontal.gif");
        registerImage(registry, IMG_VERTICAL,   "th_vertical.gif");
        registerImage(registry, IMG_SAMPLE,     "sample.gif");
        registerImage(registry, IMG_HELP,       "help.gif");
        registerImage(registry, IMG_VIEW_MENU,  "view_menu.gif");
        registerImage(registry, IMG_TAE,        "tae.gif");
        registerImage(registry, IMG_TYPE,       "type.gif");
        registerImage(registry, IMG_FEATURE,    "final_co.gif");
        registerImage(registry, IMG_REFRESH,    "refresh.gif");
        registerImage(registry, IMG_ADD_TYPES,  "type_Plus.gif");
        registerImage(registry, IMG_IMPORT_TYPE,"import_wiz.gif");
        registerImage(registry, IMG_EXPORT_TYPE,"export_wiz.gif");
        registerImage(registry, IMG_EDIT_TYPE,  "editor.gif");
        registerImage(registry, IMG_DELETE,     "delete_obj.gif");
        registerImage(registry, IMG_REPOSITORY, "repo_rep.gif");
        registerImage(registry, IMG_REPOSITORY_BIG,"newlocation_wizban.gif");
        registerImage(registry, IMG_SEARCH,     "search.gif");
        
        registerImage(registry, IMG_TS_FOLDER,     "ts_folder.gif");
        registerImage(registry, IMG_PEAR,          "pear.gif");
        registerImage(registry, IMG_XML,            "xml.gif");
        registerImage(registry, IMG_CHECK_IN,       "checkin_action.gif");
        registerImage(registry, IMG_REPOSITORY_SYNC, "cvs_synch.gif");
        
        registerImage(registry, IMG_EXPAND_ALL,     IMG_EXPAND_ALL);
        registerImage(registry, IMG_COLLAPSE_ALL,   IMG_COLLAPSE_ALL);
        registerImage(registry, IMG_FULL_NAME,      IMG_FULL_NAME);
        registerImage(registry, IMG_RUN,            IMG_RUN);           
        registerImage(registry, IMG_PAUSE,          IMG_PAUSE);
        registerImage(registry, IMG_STOP,           IMG_STOP);
        registerImage(registry, IMG_FILE_OBJ,       IMG_FILE_OBJ);
        registerImage(registry, IMG_SEARCH_NEXT,    IMG_SEARCH_NEXT);
        registerImage(registry, IMG_SEARCH_PREV,    IMG_SEARCH_PREV);
        registerImage(registry, IMG_IMPORT_PREF,    IMG_IMPORT_PREF);
    }

    private void registerImage(ImageRegistry registry, String key,
            String fileName) {
        try {
            URL url = this.getClass().getClassLoader().getResource("com/ibm/uima/etools/image/icons/" + fileName);
            if (url!=null) {
                ImageDescriptor desc = ImageDescriptor.createFromURL(url);
                registry.put(key, desc);
            }
        } catch (Exception e) {
            // Trace.trace("Cannot find image for: " + fileName);
            e.printStackTrace();
        }
    }

    private void registerImage_1 (ImageRegistry registry, String key,
            String fileName) {
        try {
            ImageDescriptor desc = ImageDescriptor.createFromFile(getClass(), fileName);
            if (desc == null) {
                System.out.println("Cannot find: " + fileName);   
                return;
            }
            registry.put(key, desc);
        } catch (Exception e) {
            // Trace.trace("Cannot find image for: " + fileName);
            e.printStackTrace();
        }
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
            initializeImageRegistry(imageRegistry);
        }
        return imageRegistry;
    }


}
