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

package org.openbaton.plugin;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonArray;
import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.JsonPrimitive;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Consumer;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;
import java.io.IOException;
import java.io.Serializable;
import java.io.UnsupportedEncodingException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeoutException;
import org.apache.commons.codec.binary.Base64;
import org.openbaton.catalogue.nfvo.PluginAnswer;
import org.openbaton.catalogue.nfvo.images.BaseNfvImage;
import org.openbaton.catalogue.nfvo.networks.BaseNetwork;
import org.openbaton.catalogue.nfvo.viminstances.BaseVimInstance;
import org.openbaton.exceptions.NotFoundException;
import org.openbaton.nfvo.common.configuration.NfvoGsonDeserializerImage;
import org.openbaton.nfvo.common.configuration.NfvoGsonDeserializerNetwork;
import org.openbaton.nfvo.common.configuration.NfvoGsonDeserializerVimInstance;
import org.openbaton.nfvo.common.configuration.NfvoGsonSerializerVimInstance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PluginListener implements Runnable {

  private static final String exchange = "openbaton-exchange";
  private String pluginId;
  private Object pluginInstance;
  private Logger log;
  private Channel channel;
  private Gson gson =
      new GsonBuilder()
          .registerTypeHierarchyAdapter(byte[].class, new ByteArrayToBase64TypeAdapter())
          .registerTypeAdapter(BaseVimInstance.class, new NfvoGsonDeserializerVimInstance())
          .registerTypeAdapter(BaseNetwork.class, new NfvoGsonDeserializerNetwork())
          .registerTypeAdapter(BaseNfvImage.class, new NfvoGsonDeserializerImage())
          .registerTypeAdapter(BaseVimInstance.class, new NfvoGsonSerializerVimInstance())
          .setPrettyPrinting()
          .create();
  private String brokerIp;
  private int brokerPort;
  private String username;
  private String password;
  private String virtualHost;
  private Connection connection;
  private ThreadPoolExecutor executor;

  public void setDurable(boolean durable) {
    this.durable = durable;
  }

  private boolean durable = true;

  public void setPluginId(String pluginId) {
    this.pluginId = pluginId;
  }

  public void setPluginInstance(Object pluginInstance) {
    this.pluginInstance = pluginInstance;
    log = LoggerFactory.getLogger(pluginInstance.getClass().getName());
  }

  public void setExecutor(ThreadPoolExecutor executor) {
    this.executor = executor;
  }

  public ThreadPoolExecutor getExecutor() {
    return executor;
  }

  private static class ByteArrayToBase64TypeAdapter
      implements JsonSerializer<byte[]>, JsonDeserializer<byte[]> {
    public byte[] deserialize(JsonElement json, Type typeOfT, JsonDeserializationContext context)
        throws JsonParseException {
      return Base64.decodeBase64(json.getAsString());
    }

    public JsonElement serialize(byte[] src, Type typeOfSrc, JsonSerializationContext context) {
      return new JsonPrimitive(Base64.encodeBase64String(src));
    }
  }

  @Override
  public void run() {

    try {
      initRabbitMQ();
    } catch (IOException | TimeoutException e) {
      e.printStackTrace();
      return;
    }
    try {

      Consumer consumer =
          new DefaultConsumer(channel) {
            @Override
            public void handleDelivery(
                String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body)
                throws IOException {
              AMQP.BasicProperties props =
                  new AMQP.BasicProperties.Builder()
                      .correlationId(properties.getCorrelationId())
                      .build();

              executor.execute(
                  () -> {
                    String message;
                    try {
                      message = new String(body, "UTF-8");
                    } catch (UnsupportedEncodingException e) {
                      e.printStackTrace();
                      return;
                    }
                    log.trace("Received message");
                    log.trace("Message content received: " + message);

                    PluginAnswer answer = new PluginAnswer();

                    try {
                      answer.setAnswer(executeMethod(message));
                    } catch (InvocationTargetException e) {
                      e.getTargetException().printStackTrace();
                      answer.setException(e.getTargetException());
                    } catch (Exception e) {
                      e.printStackTrace();
                      answer.setException(e);
                    }

                    String response;
                    try {
                      response = gson.toJson(answer);

                      log.trace("Answer is: " + response);
                      log.trace("Reply queue is: " + properties.getReplyTo());

                      channel.basicPublish(
                          exchange, properties.getReplyTo(), props, response.getBytes());

                    } catch (Throwable e) {
                      e.printStackTrace();
                      answer.setException(e);
                      log.trace("Answer is: " + answer);
                      log.trace("Reply queue is: " + properties.getReplyTo());
                      try {
                        channel.basicPublish(
                            exchange,
                            properties.getReplyTo(),
                            props,
                            gson.toJson(answer).getBytes());

                      } catch (IOException ex) {
                        log.error(
                            String.format(
                                "Thread %s got an exception: %s",
                                Thread.currentThread().getName(), e.getMessage()));
                        e.printStackTrace();
                      }
                    }
                  });

              channel.basicAck(envelope.getDeliveryTag(), false);
              log.trace(String.format("Ack %d", envelope.getDeliveryTag()));

              synchronized (this) {
                this.notify();
              }
            }
          };

      channel.basicConsume(pluginId, false, consumer);

      // Wait and be prepared to consume the message from RPC client.
      while (true) {
        synchronized (consumer) {
          try {
            consumer.wait();
          } catch (InterruptedException e) {
            log.info("Ctrl-c received");
            System.exit(0);
          }
        }
      }
    } catch (IOException e) {
      e.printStackTrace();
    } finally {
      if (connection != null)
        try {
          connection.close();
        } catch (IOException ignored) {
        }
    }

    try {
      if (channel == null || connection == null) System.exit(3);
      channel.close();
      connection.close();
    } catch (IOException | TimeoutException e) {
      e.printStackTrace();
    }
  }

  private Serializable executeMethod(String pluginMessageString)
      throws InvocationTargetException, IllegalAccessException, NotFoundException {

    JsonObject pluginMessageObject = gson.fromJson(pluginMessageString, JsonObject.class);

    List<Object> params = new ArrayList<>();

    for (JsonElement param : pluginMessageObject.get("parameters").getAsJsonArray()) {
      Object p = gson.fromJson(param, Object.class);
      if (p != null) {
        params.add(p);
      }
    }

    Class pluginClass = pluginInstance.getClass();

    log.trace("There are " + params.size() + " parameters");
    String methodName = pluginMessageObject.get("methodName").getAsString();
    log.trace("Looking for method: " + methodName);

    for (Method m : pluginClass.getMethods()) {
      log.trace(
          "Method checking is: "
              + m.getName()
              + " with "
              + m.getParameterTypes().length
              + " parameters");
      byte[] avoid = new byte[0];
      if (m.getName().equals(methodName)
          && m.getParameterTypes().length == params.size()
          && !m.getParameterTypes()[m.getParameterTypes().length - 1]
              .getCanonicalName()
              .equals(avoid.getClass().getCanonicalName())) {
        if (!m.getReturnType().equals(Void.class)) {
          if (params.size() != 0) {
            params =
                getParameters(
                    pluginMessageObject.get("parameters").getAsJsonArray(), m.getParameterTypes());
            for (Object p : params) {
              log.trace("param class is: " + p.getClass());
            }
            return (Serializable) m.invoke(pluginInstance, params.toArray());
          } else {
            return (Serializable) m.invoke(pluginInstance);
          }
        } else {
          if (params.size() != 0) {
            params =
                getParameters(
                    pluginMessageObject.get("parameters").getAsJsonArray(), m.getParameterTypes());
            for (Object p : params) {
              log.trace("param class is: " + p.getClass());
            }
            m.invoke(pluginInstance, params.toArray());
          } else {
            m.invoke(pluginInstance);
          }

          return null;
        }
      }
    }

    throw new NotFoundException("method not found");
  }

  private List<Object> getParameters(JsonArray parameters, Class<?>[] parameterTypes) {
    List<Object> res = new LinkedList<>();
    for (int i = 0; i < parameters.size(); i++) {
      res.add(gson.fromJson(parameters.get(i), parameterTypes[i]));
    }
    return res;
  }

  private void initRabbitMQ() throws IOException, TimeoutException {
    ConnectionFactory factory = new ConnectionFactory();
    factory.setHost(brokerIp);
    factory.setPort(brokerPort);
    factory.setPassword(password);
    factory.setUsername(username);
    factory.setVirtualHost(virtualHost);

    connection = factory.newConnection();
    channel = connection.createChannel();

    channel.queueDeclare(pluginId, this.durable, false, true, null);
    channel.queueBind(pluginId, exchange, pluginId);

    channel.basicQos(1);
  }

  public void setBrokerIp(String brokerIp) {
    this.brokerIp = brokerIp;
  }

  public void setBrokerPort(int brokerPort) {
    this.brokerPort = brokerPort;
  }

  public void setUsername(String username) {
    this.username = username;
  }

  public void setPassword(String password) {
    this.password = password;
  }

  public void setVirtualHost(String virtualHost) {
    this.virtualHost = virtualHost;
  }
}
