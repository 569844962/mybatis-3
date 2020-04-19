/**
 *    Copyright 2009-2019 the original author or authors.
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */
package org.apache.ibatis.builder.xml;

import org.apache.ibatis.builder.BaseBuilder;
import org.apache.ibatis.builder.BuilderException;
import org.apache.ibatis.datasource.DataSourceFactory;
import org.apache.ibatis.executor.ErrorContext;
import org.apache.ibatis.executor.loader.ProxyFactory;
import org.apache.ibatis.io.Resources;
import org.apache.ibatis.io.VFS;
import org.apache.ibatis.logging.Log;
import org.apache.ibatis.mapping.DatabaseIdProvider;
import org.apache.ibatis.mapping.Environment;
import org.apache.ibatis.parsing.XNode;
import org.apache.ibatis.parsing.XPathParser;
import org.apache.ibatis.plugin.Interceptor;
import org.apache.ibatis.reflection.DefaultReflectorFactory;
import org.apache.ibatis.reflection.MetaClass;
import org.apache.ibatis.reflection.ReflectorFactory;
import org.apache.ibatis.reflection.factory.ObjectFactory;
import org.apache.ibatis.reflection.wrapper.ObjectWrapperFactory;
import org.apache.ibatis.session.*;
import org.apache.ibatis.transaction.TransactionFactory;
import org.apache.ibatis.type.JdbcType;

import javax.sql.DataSource;
import java.io.InputStream;
import java.io.Reader;
import java.util.Properties;

/**
 * @author Clinton Begin
 * @author Kazuki Shimizu
 */
public class XMLConfigBuilder extends BaseBuilder {

  private boolean parsed;
  private final XPathParser parser;
  private String environment;
  private final ReflectorFactory localReflectorFactory = new DefaultReflectorFactory();

  public XMLConfigBuilder(Reader reader) {
    this(reader, null, null);
  }

  public XMLConfigBuilder(Reader reader, String environment) {
    this(reader, environment, null);
  }

  public XMLConfigBuilder(Reader reader, String environment, Properties props) {
    this(new XPathParser(reader, true, props, new XMLMapperEntityResolver()), environment, props);
  }

  public XMLConfigBuilder(InputStream inputStream) {
    this(inputStream, null, null);
  }

  public XMLConfigBuilder(InputStream inputStream, String environment) {
    this(inputStream, environment, null);
  }

  /**
   * 构造XMLConfigBuilder
   * @param inputStream 输入流
   * @param environment environment环境
   * @param props Properties properties对象
   */
  public XMLConfigBuilder(InputStream inputStream, String environment, Properties props) {
    this(new XPathParser(inputStream, true, props, new XMLMapperEntityResolver()), environment, props);
  }

  /**
   * 构造XMLConfigBuilder
   * @param parser XPathParser
   * @param environment environment环境
   * @param props properties对象
   */
  private XMLConfigBuilder(XPathParser parser, String environment, Properties props) {
    //调用父类（BaseBuilder）构造函数，创建Configuration：会进行别名注册
    super(new Configuration());
    ErrorContext.instance().resource("SQL Mapper Configuration");
    //Configuration设置properties
    this.configuration.setVariables(props);
    this.parsed = false;
    this.environment = environment;
    this.parser = parser;
  }

  /**
   * 解析mybatis-config.xml
   * @return configuration
   */
  public Configuration parse() {
    if (parsed) {
      throw new BuilderException("Each XMLConfigBuilder can only be used once.");
    }
    parsed = true;
    //解析configuration节点<configuration></configuration>
    parseConfiguration(parser.evalNode("/configuration"));
    return configuration;
  }

  /**
   * 解析configuration下子节点的属性
   * @param root configuration节点
   */
  private void parseConfiguration(XNode root) {
    try {
      //issue #117 read properties first
      // 1> 解析properties节点信息
      propertiesElement(root.evalNode("properties"));
      //2> 解析settings节点信息
      Properties settings = settingsAsProperties(root.evalNode("settings"));
      //2.1> 指定 VFS 的实现
      loadCustomVfs(settings);
      //2.2> 加载setting logImpl配置， 指定MyBatis 所用日志的具体实现
      loadCustomLogImpl(settings);
      //2.3> 解析并注册别名
      typeAliasesElement(root.evalNode("typeAliases"));
      //2.4 解析并加载插件到拦截器
      pluginElement(root.evalNode("plugins"));
      //2.5 解析并加载对象工厂objectFactory
      objectFactoryElement(root.evalNode("objectFactory"));
      //2.6 解析并加载objectWrapperFactory
      objectWrapperFactoryElement(root.evalNode("objectWrapperFactory"));
      //2.7 解析并加载reflectorFactory
      reflectorFactoryElement(root.evalNode("reflectorFactory"));
      //2.8设置settings属性值
      settingsElement(settings);
      // read it after objectFactory and objectWrapperFactory issue #631
      //2.9解析environments节点信息，并设置environment属性
      environmentsElement(root.evalNode("environments"));
      //2.10解析databaseIdProvider节点（数据库厂商信息）
      databaseIdProviderElement(root.evalNode("databaseIdProvider"));
      //2.11解析typeHandlers节点
      typeHandlerElement(root.evalNode("typeHandlers"));
      //2.12 解析mappers节点
      mapperElement(root.evalNode("mappers"));
    } catch (Exception e) {
      throw new BuilderException("Error parsing SQL Mapper Configuration. Cause: " + e, e);
    }
  }

  /**
   * 读取settings的子节点属性
   * @param context settings节点
   * @return Properties
   */
  private Properties settingsAsProperties(XNode context) {
    if (context == null) {
      return new Properties();
    }
    //1> 获取<settings>下子节点<setting>的属性值
    Properties props = context.getChildrenAsProperties();
    //2> 获取Configuration的元信息
    MetaClass metaConfig = MetaClass.forClass(Configuration.class, localReflectorFactory);
    // Check that all settings are known to the configuration class
    //3> 检查<setting>属性配置在Configuration中是否存在相应的setter方法
    for (Object key : props.keySet()) {
      if (!metaConfig.hasSetter(String.valueOf(key))) {
        throw new BuilderException("The setting " + key + " is not known.  Make sure you spelled it correctly (case sensitive).");
      }
    }
    return props;
  }

  private void loadCustomVfs(Properties props) throws ClassNotFoundException {
    String value = props.getProperty("vfsImpl");
    if (value != null) {
      String[] clazzes = value.split(",");
      for (String clazz : clazzes) {
        if (!clazz.isEmpty()) {
          @SuppressWarnings("unchecked")
          Class<? extends VFS> vfsImpl = (Class<? extends VFS>)Resources.classForName(clazz);
          configuration.setVfsImpl(vfsImpl);
        }
      }
    }
  }

  private void loadCustomLogImpl(Properties props) {
    Class<? extends Log> logImpl = resolveClass(props.getProperty("logImpl"));
    configuration.setLogImpl(logImpl);
  }

  /**
   * 解析别名typeAliases
   */
  private void typeAliasesElement(XNode parent) {
    if (parent != null) {
      for (XNode child : parent.getChildren()) {
        //1> 如果是package节点，则将包下的所有类注册到别名中
        if ("package".equals(child.getName())) {
          String typeAliasPackage = child.getStringAttribute("name");
          configuration.getTypeAliasRegistry().registerAliases(typeAliasPackage);
        } else {
        //2> 如果是typeAlias节点，则将每一个typeAlias节点的信息注册到别名中
          String alias = child.getStringAttribute("alias");
          String type = child.getStringAttribute("type");
          try {
            //2.1> 根据typeAlias节点的type和alias信息注册别名
            Class<?> clazz = Resources.classForName(type);
            if (alias == null) {
              typeAliasRegistry.registerAlias(clazz);
            } else {
              typeAliasRegistry.registerAlias(alias, clazz);
            }
          } catch (ClassNotFoundException e) {
            throw new BuilderException("Error registering typeAlias for '" + alias + "'. Cause: " + e, e);
          }
        }
      }
    }
  }

  /**
   *加载plugins插件到拦截器中
   */
  private void pluginElement(XNode parent) throws Exception {
    if (parent != null) {
      for (XNode child : parent.getChildren()) {
        //1> 获取拦截器的类全路径
        String interceptor = child.getStringAttribute("interceptor");
        //2> 获取plugin节点下property节点的信息
        Properties properties = child.getChildrenAsProperties();
        //3> 创建拦截器实例
        Interceptor interceptorInstance = (Interceptor) resolveClass(interceptor).newInstance();
        //4> 拦截器设置properties熟悉
        interceptorInstance.setProperties(properties);
        //5> configuration添加拦截器
        configuration.addInterceptor(interceptorInstance);
      }
    }
  }

  /**
   * 解析并加载对象工厂objectFactory
   */
  private void objectFactoryElement(XNode context) throws Exception {
    if (context != null) {
      //1> 获取type属性的值（类的全路径）
      String type = context.getStringAttribute("type");
      //2> 获取property节点信息name和value值
      Properties properties = context.getChildrenAsProperties();
      //3> 构建ObjectFactory对象
      ObjectFactory factory = (ObjectFactory) resolveClass(type).newInstance();
      //4> ObjectFactory设置property属性
      factory.setProperties(properties);
      //5> configuration设置ObjectFactory
      configuration.setObjectFactory(factory);
    }
  }

  /**
   * 解析并加载objectWrapperFactory
   */
  private void objectWrapperFactoryElement(XNode context) throws Exception {
    if (context != null) {
      //1> 获取type属性的值（类的全路径）
      String type = context.getStringAttribute("type");
      //2> 构建ObjectWrapperFactory对象
      ObjectWrapperFactory factory = (ObjectWrapperFactory) resolveClass(type).newInstance();
      //3> configuration设置ObjectWrapperFactory
      configuration.setObjectWrapperFactory(factory);
    }
  }

  /**
   * 解析并加载reflectorFactory
   */
  private void reflectorFactoryElement(XNode context) throws Exception {
    if (context != null) {
      //1> 获取type属性的值（类的全路径）
       String type = context.getStringAttribute("type");
      //2> 构建ReflectorFactory对象
       ReflectorFactory factory = (ReflectorFactory) resolveClass(type).newInstance();
      //3> configuration设置ReflectorFactory
       configuration.setReflectorFactory(factory);
    }
  }

    /**
   * 解析properties节点
   * Properties 是一个Hashtable
   * @param context propertise节点xnode
   */
  private void propertiesElement(XNode context) throws Exception {
    if (context != null) {
      //1.获取propertises下propertise属性
      Properties defaults = context.getChildrenAsProperties();
      //2> 获取resource属性
      String resource = context.getStringAttribute("resource");
      //3> 获取url属性,远程文件
      String url = context.getStringAttribute("url");
      if (resource != null && url != null) {
        throw new BuilderException("The properties element cannot specify both a URL and a resource based property file reference.  Please specify one or the other.");
      }
      if (resource != null) {
        //4> resource存在，则读取resource文件中的配置信息
        defaults.putAll(Resources.getResourceAsProperties(resource));
      } else if (url != null) {
        //5> url存在，则获取url文件中的配置
        defaults.putAll(Resources.getUrlAsProperties(url));
      }
      //6> 获取configuration中的Properties
      Properties vars = configuration.getVariables();
      if (vars != null) {
        //6.1> Properties的defaults加入从configuration获取的properties
        defaults.putAll(vars);
      }
      //7> XPathParser 设置Properties属性
      parser.setVariables(defaults);
      //8> configuration设置Properties属性
      configuration.setVariables(defaults);
    }
  }

  /**
   * 设置setting属性值
   * @param props Properties对象
   */
  private void settingsElement(Properties props) {
    configuration.setAutoMappingBehavior(AutoMappingBehavior.valueOf(props.getProperty("autoMappingBehavior", "PARTIAL")));
    configuration.setAutoMappingUnknownColumnBehavior(AutoMappingUnknownColumnBehavior.valueOf(props.getProperty("autoMappingUnknownColumnBehavior", "NONE")));
    configuration.setCacheEnabled(booleanValueOf(props.getProperty("cacheEnabled"), true));
    configuration.setProxyFactory((ProxyFactory) createInstance(props.getProperty("proxyFactory")));
    configuration.setLazyLoadingEnabled(booleanValueOf(props.getProperty("lazyLoadingEnabled"), false));
    configuration.setAggressiveLazyLoading(booleanValueOf(props.getProperty("aggressiveLazyLoading"), false));
    configuration.setMultipleResultSetsEnabled(booleanValueOf(props.getProperty("multipleResultSetsEnabled"), true));
    configuration.setUseColumnLabel(booleanValueOf(props.getProperty("useColumnLabel"), true));
    configuration.setUseGeneratedKeys(booleanValueOf(props.getProperty("useGeneratedKeys"), false));
    configuration.setDefaultExecutorType(ExecutorType.valueOf(props.getProperty("defaultExecutorType", "SIMPLE")));
    configuration.setDefaultStatementTimeout(integerValueOf(props.getProperty("defaultStatementTimeout"), null));
    configuration.setDefaultFetchSize(integerValueOf(props.getProperty("defaultFetchSize"), null));
    configuration.setMapUnderscoreToCamelCase(booleanValueOf(props.getProperty("mapUnderscoreToCamelCase"), false));
    configuration.setSafeRowBoundsEnabled(booleanValueOf(props.getProperty("safeRowBoundsEnabled"), false));
    configuration.setLocalCacheScope(LocalCacheScope.valueOf(props.getProperty("localCacheScope", "SESSION")));
    configuration.setJdbcTypeForNull(JdbcType.valueOf(props.getProperty("jdbcTypeForNull", "OTHER")));
    configuration.setLazyLoadTriggerMethods(stringSetValueOf(props.getProperty("lazyLoadTriggerMethods"), "equals,clone,hashCode,toString"));
    configuration.setSafeResultHandlerEnabled(booleanValueOf(props.getProperty("safeResultHandlerEnabled"), true));
    configuration.setDefaultScriptingLanguage(resolveClass(props.getProperty("defaultScriptingLanguage")));
    configuration.setDefaultEnumTypeHandler(resolveClass(props.getProperty("defaultEnumTypeHandler")));
    configuration.setCallSettersOnNulls(booleanValueOf(props.getProperty("callSettersOnNulls"), false));
    configuration.setUseActualParamName(booleanValueOf(props.getProperty("useActualParamName"), true));
    configuration.setReturnInstanceForEmptyRow(booleanValueOf(props.getProperty("returnInstanceForEmptyRow"), false));
    configuration.setLogPrefix(props.getProperty("logPrefix"));
    configuration.setConfigurationFactory(resolveClass(props.getProperty("configurationFactory")));
  }

  /**
   * 解析environments节点信息
   */
  private void environmentsElement(XNode context) throws Exception {
    if (context != null) {
      if (environment == null) {
        //1>获取environments节点default属性值
        environment = context.getStringAttribute("default");
      }
      for (XNode child : context.getChildren()) {
        //2>获取environment节点id值
        String id = child.getStringAttribute("id");
        //3> 判断id是否为environments节点的default值
        if (isSpecifiedEnvironment(id)) {
          //4>解析transactionManager节点，创建TransactionFactory对象
          TransactionFactory txFactory = transactionManagerElement(child.evalNode("transactionManager"));
          //5>解析dataSource节点，创建DataSourceFactory对象
          DataSourceFactory dsFactory = dataSourceElement(child.evalNode("dataSource"));
          DataSource dataSource = dsFactory.getDataSource();
          //6.创建Environment.Builder对象，并设置transactionFactory和dataSource
          Environment.Builder environmentBuilder = new Environment.Builder(id)
              .transactionFactory(txFactory)
              .dataSource(dataSource);
          //7.configuration设置environment属性
          configuration.setEnvironment(environmentBuilder.build());
        }
      }
    }
  }

  /**
   * 解析databaseIdProvider节点
   */
  private void databaseIdProviderElement(XNode context) throws Exception {
    DatabaseIdProvider databaseIdProvider = null;
    if (context != null) {
      //1> 获取type属性值
      String type = context.getStringAttribute("type");
      // awful patch to keep backward compatibility
      if ("VENDOR".equals(type)) {
          type = "DB_VENDOR";
      }
      //2> 获取databaseIdProvider节点下的property节点信息，并创建DatabaseIdProvider对象
      Properties properties = context.getChildrenAsProperties();
      databaseIdProvider = (DatabaseIdProvider) resolveClass(type).newInstance();
      databaseIdProvider.setProperties(properties);
    }
    //3> 获取databaseId，并设置configuration的databaseId
    Environment environment = configuration.getEnvironment();
    if (environment != null && databaseIdProvider != null) {
      String databaseId = databaseIdProvider.getDatabaseId(environment.getDataSource());
      configuration.setDatabaseId(databaseId);
    }
  }

  /**
   * 解析transactionManager节点，返回TransactionFactory
   */
  private TransactionFactory transactionManagerElement(XNode context) throws Exception {
    if (context != null) {
      //1> 获取transactionManager节点的type值
      String type = context.getStringAttribute("type");
      //2> 获取transactionManager下properties子节点属性
      Properties props = context.getChildrenAsProperties();
      //3> 创建TransactionFactory对象并设置properties属性
      TransactionFactory factory = (TransactionFactory) resolveClass(type).newInstance();
      factory.setProperties(props);
      return factory;
    }
    throw new BuilderException("Environment declaration requires a TransactionFactory.");
  }

  /**
   * 解析dataSource节点，返回DataSourceFactory
   */
  private DataSourceFactory dataSourceElement(XNode context) throws Exception {
    if (context != null) {
      //1> 获取dataSource节点的type值
      String type = context.getStringAttribute("type");
      //2> 获取dataSource下properties子节点属性
      Properties props = context.getChildrenAsProperties();
      //3> 创建DataSourceFactory对象并设置properties属性
      DataSourceFactory factory = (DataSourceFactory) resolveClass(type).newInstance();
      factory.setProperties(props);
      return factory;
    }
    throw new BuilderException("Environment declaration requires a DataSourceFactory.");
  }

  /**
   * 解析typeHandlers节点，并对typehandler进行注册
   */
  private void typeHandlerElement(XNode parent) {
    if (parent != null) {
      for (XNode child : parent.getChildren()) {
        //1> 从指定包中注册TypeHandler
        if ("package".equals(child.getName())) {
          String typeHandlerPackage = child.getStringAttribute("name");
          typeHandlerRegistry.register(typeHandlerPackage);
        } else {
          //2>解析typeHandler节点，获取javaType，jdbcType，handler属性值
          String javaTypeName = child.getStringAttribute("javaType");
          String jdbcTypeName = child.getStringAttribute("jdbcType");
          String handlerTypeName = child.getStringAttribute("handler");
          //3> 获取javaTypeName为别名对应的类，如不存在，则返回javaTypeName指定的类
          Class<?> javaTypeClass = resolveClass(javaTypeName);
          //4> 获取jdbcTypeName对应的JdbcType
          JdbcType jdbcType = resolveJdbcType(jdbcTypeName);
          //5> 获取handlerTypeName为别名对应的类，如不存在，则返回handlerTypeName指定的类
          Class<?> typeHandlerClass = resolveClass(handlerTypeName);
          //6> 注册TypeHandler
          if (javaTypeClass != null) {
            if (jdbcType == null) {
              typeHandlerRegistry.register(javaTypeClass, typeHandlerClass);
            } else {
              typeHandlerRegistry.register(javaTypeClass, jdbcType, typeHandlerClass);
            }
          } else {
            typeHandlerRegistry.register(typeHandlerClass);
          }
        }
      }
    }
  }

  private void mapperElement(XNode parent) throws Exception {
    if (parent != null) {
      for (XNode child : parent.getChildren()) {
        //1> 如果存在package节点，则对package包进行扫描，
        if ("package".equals(child.getName())) {
          String mapperPackage = child.getStringAttribute("name");
          configuration.addMappers(mapperPackage);
        } else {
          String resource = child.getStringAttribute("resource");
          String url = child.getStringAttribute("url");
          String mapperClass = child.getStringAttribute("class");
          if (resource != null && url == null && mapperClass == null) {
            ErrorContext.instance().resource(resource);
            InputStream inputStream = Resources.getResourceAsStream(resource);
            XMLMapperBuilder mapperParser = new XMLMapperBuilder(inputStream, configuration, resource, configuration.getSqlFragments());
            mapperParser.parse();
          } else if (resource == null && url != null && mapperClass == null) {
            ErrorContext.instance().resource(url);
            InputStream inputStream = Resources.getUrlAsStream(url);
            XMLMapperBuilder mapperParser = new XMLMapperBuilder(inputStream, configuration, url, configuration.getSqlFragments());
            mapperParser.parse();
          } else if (resource == null && url == null && mapperClass != null) {
            Class<?> mapperInterface = Resources.classForName(mapperClass);
            configuration.addMapper(mapperInterface);
          } else {
            throw new BuilderException("A mapper element may only specify a url, resource or class, but not more than one.");
          }
        }
      }
    }
  }

  /**
   * 判断default与当前id是否相等
   * environment（default）为空抛出异常
   * id为空抛出异常
   */
  private boolean isSpecifiedEnvironment(String id) {
    if (environment == null) {
      throw new BuilderException("No environment specified.");
    } else if (id == null) {
      throw new BuilderException("Environment requires an id attribute.");
    } else if (environment.equals(id)) {
      return true;
    }
    return false;
  }

}
