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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeoutException;

/**
 * Created by lto on 09/09/15.
 */
public class PluginStarter {

  protected static final Logger log = LoggerFactory.getLogger(PluginStarter.class);
  private static Map<String, PluginListener> plugins = new HashMap<String, PluginListener>();
  private static Properties properties;
  private static ExecutorService executor;

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
  }

  public static void registerPlugin(
      Class clazz, String name, String brokerIp, int port, int consumers)
      throws IOException, NoSuchMethodException, IllegalAccessException, InvocationTargetException,
          InstantiationException, TimeoutException, InterruptedException {
    registerPlugin(clazz, name, brokerIp, port, consumers, true);
  }

  public static void registerPlugin(
      Class clazz, String name, String brokerIp, int port, int consumers, boolean durable)
      throws IOException, NoSuchMethodException, IllegalAccessException, InvocationTargetException,
          InstantiationException, TimeoutException, InterruptedException {
    getProperties(clazz);
    String username = properties.getProperty("username", "openbaton-manager-user");
    String password = properties.getProperty("password", "openbaton");
    String virtualHost = properties.getProperty("virtual-host", "/");
    registerPlugin(
        clazz, name, brokerIp, port, consumers, username, password, virtualHost, durable);
  }

  protected static void registerPlugin(
      Class clazz,
      String name,
      final String brokerIp,
      final int port,
      int consumers,
      final String username,
      final String password,
      final String virtualHost,
      boolean queueDurable)
      throws IOException, InstantiationException, IllegalAccessException, InvocationTargetException,
          NoSuchMethodException, TimeoutException, InterruptedException {
    String pluginId = getFinalName(clazz, name);
    final Registration registration = new Registration();
    final ManagerCredentials managerCredentials =
        registration.registerPluginToNfvo(
            brokerIp, port, username, password, virtualHost, pluginId);
    Runtime.getRuntime()
        .addShutdownHook(
            new Thread() {
              @Override
              public void run() {
                try {
                  registration.deregisterPluginFromNfvo(
                      brokerIp,
                      port,
                      username,
                      password,
                      virtualHost,
                      managerCredentials.getRabbitUsername(),
                      managerCredentials.getRabbitPassword());
                } catch (IOException e) {
                  e.printStackTrace();
                } catch (TimeoutException e) {
                  e.printStackTrace();
                }
              }
            });
    // registration.registerPluginToNfvo(factory, pluginId);
    if (properties == null) {
      getProperties(clazz);
    }
    executor = Executors.newFixedThreadPool(consumers);
    for (int i = 0; i < consumers; i++) {
      PluginListener pluginListener = new PluginListener();
      pluginListener.setDurable(queueDurable);
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
