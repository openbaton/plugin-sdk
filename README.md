  <img src="https://raw.githubusercontent.com/openbaton/openbaton.github.io/master/images/openBaton.png" width="250"/>
  
  Copyright © 2015-2016 [Open Baton](http://openbaton.org). Licensed under [Apache v2 License](http://www.apache.org/licenses/LICENSE-2.0).

[![Build Status](https://travis-ci.org/openbaton/plugin-sdk.svg?branch=develop)](https://travis-ci.org/openbaton/plugin-sdk)

# Open Baton Plugin SDK

This project **plugin-sdk** contains modules that are needed to implement a plugin for the Open Baton framework.

Please refer to our [documentation][vim-plugin] for more information about this project. Below there are just few details about the way you can use it.

## How to use the Plugin SDK

An Open Baton Plugin is a separate application that accepts requests from the NFVO to allocate and update resources on VIM. Plugin SDK provides an interface to develop the plugins to work with Openbaton NFVO or VNFM of your choice. SDK allows the communication through rabbitmq broker via AMQP messaging.

In order to create a VIM plugin for OpenBaton system you need to add to your gradle build file:

```gradle
buildscript {
    dependencies {
        classpath("org.springframework.boot:spring-boot-gradle-plugin:1.2.6.RELEASE")
    }
}

apply plugin: 'spring-boot'

project.ext{
	mainClass = 'path.to.the.StarterClass'
}

apply plugin: 'java'
apply plugin: 'maven'
repositories {
    mavenCentral()
    maven {
        url "http://193.175.132.176:8081/nexus/content/groups/public"
    }
}

dependencies {
    compile 'org.openbaton:plugin-sdk:2.0.0'
}
```

Than create a class that implement ClientInterfaces and the inherited methods.
Create another starter class and set the path to it in a variable mainClass.

the starter class can be like the following:

```java
import org.openbaton.plugin.PluginStarter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StarterClass {

    private static Logger log = LoggerFactory.getLogger(PluginStarter.class);

    public static void main(String[] args) {
        log.info("params are: pluginName registryIp registryPort\ndefault is openstack-plugin localhost 1099");

        PluginStartup.startPluginRecursive("./path-to-folder", true, "broker-ip", "5672", 15, "admin", "openbaton", "15672");
}
```

Then, compile & run it

## Issue tracker

Issues and bug reports should be posted to the GitHub Issue Tracker of this project

# What is Open Baton?

Open Baton is an open source project providing a comprehensive implementation of the ETSI Management and Orchestration (MANO) specification and the TOSCA Standard.

Open Baton provides multiple mechanisms for interoperating with different VNFM vendor solutions. It has a modular architecture which can be easily extended for supporting additional use cases. 

It integrates with OpenStack as standard de-facto VIM implementation, and provides a driver mechanism for supporting additional VIM types. It supports Network Service management either using the provided Generic VNFM and Juju VNFM, or integrating additional specific VNFMs. It provides several mechanisms (REST or PUB/SUB) for interoperating with external VNFMs. 

It can be combined with additional components (Monitoring, Fault Management, Autoscaling, and Network Slicing Engine) for building a unique MANO comprehensive solution.

## Source Code and documentation

The Source Code of the other Open Baton projects can be found [here][openbaton-github] and the documentation can be found [here][openbaton-doc] .

## News and Website

Check the [Open Baton Website][openbaton]
Follow us on Twitter @[openbaton][openbaton-twitter].

## Licensing and distribution
Copyright © [2015-2016] Open Baton project

Licensed under the Apache License, Version 2.0 (the "License");

you may not use this file except in compliance with the License.
You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

## Support
The Open Baton project provides community support through the Open Baton Public Mailing List and through StackOverflow using the tags openbaton.

## Supported by
  <img src="https://raw.githubusercontent.com/openbaton/openbaton.github.io/master/images/fokus.png" width="250"/><img src="https://raw.githubusercontent.com/openbaton/openbaton.github.io/master/images/tu.png" width="150"/>

[fokus-logo]: https://raw.githubusercontent.com/openbaton/openbaton.github.io/master/images/fokus.png
[openbaton]: http://openbaton.org
[openbaton-doc]: http://openbaton.org/documentation
[openbaton-github]: http://github.org/openbaton
[openbaton-logo]: https://raw.githubusercontent.com/openbaton/openbaton.github.io/master/images/openBaton.png
[openbaton-mail]: mailto:users@openbaton.org
[openbaton-twitter]: https://twitter.com/openbaton
[tub-logo]: https://raw.githubusercontent.com/openbaton/openbaton.github.io/master/images/tu.png
[vim-plugin]: http://openbaton.github.io/documentation/vim-plugin/
