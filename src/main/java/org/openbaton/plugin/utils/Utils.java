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
