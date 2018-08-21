/*
 * Copyright (c) 2015-2018 Open Baton (http://openbaton.org)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.openbaton.plugin.utils;

import org.openbaton.monitoring.interfaces.VirtualisedResourcesPerformanceManagement;
import org.openbaton.vim.drivers.interfaces.ClientInterfaces;

/** Created by gca on 14/12/16. */
public class Utils {

  public static String checkInterface(Class clazz) {
    for (Class interf : clazz.getSuperclass().getInterfaces()) {
      if (interf.getName().equals(ClientInterfaces.class.getName())) {
        return "vim-drivers";
      } else if (interf
          .getName()
          .equals(VirtualisedResourcesPerformanceManagement.class.getName())) {
        return "monitor";
      }
    }
    return "unknown-interface";
  }
}
