/*
 * Copyright (c) 2015 Fraunhofer FOKUS
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.openbaton.plugin;

import org.openbaton.catalogue.nfvo.ManagerCredentials;
import org.openbaton.plugin.utils.Utils;
import org.openbaton.registration.Registration;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class PluginStarter {

  private static Properties properties;
  private static ThreadPoolExecutor executor;

  private static String getFinalName(Class clazz, String name) throws IOException {
    getProperties(clazz);
    String inte = Utils.checkInterface(clazz);
    if (inte.equals("unknown-interface")) // no interface found
    {
      throw new RuntimeException(
          "The plugin class "
              + clazz.getSimpleName()
              + " needs to extend or VimDriver or Monitoring classes");
    }
    return inte + "." + properties.getProperty("type", "unknown") + "." + name;
  }

  private static void getProperties(Class clazz) throws IOException {
    properties = new Properties();
    properties.load(clazz.getResourceAsStream("/plugin.conf.properties"));
    System.getenv()
        .forEach(
            (k, v) -> {
              if (properties.containsKey(k.toLowerCase())) properties.put(k.toLowerCase(), v);
            });
  }

  public static void registerPlugin(
      Class clazz, String name, String brokerIp, int port, int consumers)
      throws IOException, NoSuchMethodException, IllegalAccessException, InvocationTargetException,
          InstantiationException, TimeoutException, InterruptedException {
    getProperties(clazz);
    executor =
        new ThreadPoolExecutor(
            consumers,
            Integer.MAX_VALUE,
            Long.parseLong(properties.getProperty("keep-alive", "120000")),
            TimeUnit.MILLISECONDS,
            new SynchronousQueue<>());
    String username = properties.getProperty("username", "openbaton-manager-user");
    String password = properties.getProperty("password", "openbaton");
    String virtualHost = properties.getProperty("virtual-host", "/");
    registerPlugin(clazz, name, brokerIp, port, consumers, username, password, virtualHost);
  }

  @SuppressWarnings("unchecked")
  private static void registerPlugin(
      Class clazz,
      String name,
      final String brokerIp,
      final int port,
      int consumers,
      final String username,
      final String password,
      final String virtualHost)
      throws IOException, InstantiationException, IllegalAccessException, InvocationTargetException,
          NoSuchMethodException, TimeoutException, InterruptedException {
    String pluginId = getFinalName(clazz, name);
    final Registration registration = new Registration();
    final ManagerCredentials managerCredentials =
        registration.registerPluginToNfvo(
            brokerIp, port, username, password, virtualHost, pluginId);
    Runtime.getRuntime()
        .addShutdownHook(
            new Thread(
                () -> {
                  try {
                    registration.deregisterPluginFromNfvo(
                        brokerIp,
                        port,
                        username,
                        password,
                        virtualHost,
                        managerCredentials.getRabbitUsername(),
                        managerCredentials.getRabbitPassword());
                    executor.shutdown();
                  } catch (IOException | TimeoutException e) {
                    e.printStackTrace();
                  }
                }));
    if (properties == null) {
      getProperties(clazz);
    }
    ExecutorService executor = Executors.newFixedThreadPool(consumers);
    for (int i = 0; i < consumers; i++) {
      PluginListener pluginListener = new PluginListener();
      pluginListener.setExecutor(PluginStarter.executor);
      pluginListener.setDurable(true);
      pluginListener.setPluginId(pluginId);
      pluginListener.setPluginInstance(clazz.getConstructor().newInstance());
      pluginListener.setBrokerIp(brokerIp);
      pluginListener.setBrokerPort(port);
      pluginListener.setUsername(managerCredentials.getRabbitUsername());
      pluginListener.setPassword(managerCredentials.getRabbitPassword());
      pluginListener.setVirtualHost(virtualHost);

      executor.execute(pluginListener);
    }
  }
}
