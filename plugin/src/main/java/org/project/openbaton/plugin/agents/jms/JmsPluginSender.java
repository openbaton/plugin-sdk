package org.project.openbaton.plugin.agents.jms;

import org.project.openbaton.plugin.interfaces.agents.PluginSender;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Scope;
import org.springframework.jms.core.JmsTemplate;
import org.springframework.jms.core.MessageCreator;
import org.springframework.stereotype.Service;

import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.Session;
import java.io.Serializable;

/**
 * Created by lto on 14/08/15.
 */
@Service
@Scope("prototype")
public class JmsPluginSender implements PluginSender {

    @Autowired
    private JmsTemplate jmsTemplate;
    private Logger log = LoggerFactory.getLogger(this.getClass());

    @Override
    public void send(String destination, Serializable message) {
        log.debug("sending to destination " + destination + " message: " + message);
        jmsTemplate.send(destination, getMessageCreator(message));
    }

    private MessageCreator getMessageCreator(final Serializable message) {
        if (message instanceof String)
        return new MessageCreator() {
            @Override
            public Message createMessage(Session session) throws JMSException {
                return session.createTextMessage((String) message);
            }
        };
        else return new MessageCreator() {
            @Override
            public Message createMessage(Session session) throws JMSException {
                return session.createObjectMessage(message);
            }
        };
    }
}