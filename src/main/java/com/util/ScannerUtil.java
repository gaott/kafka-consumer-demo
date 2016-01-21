package com.util;

import java.io.File;
import java.io.FileFilter;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.net.URL;
import java.net.URLDecoder;
import java.util.LinkedHashSet;
import java.util.Properties;
import java.util.Set;

import com.job.annotation.Parameter;


/**
 * 扫描文件
 *
 */
public class ScannerUtil {
  
  /**
   * 依据 Parameter 注解，将properties文件中的内容赋值给Config类中的public static变量。
   * 
   */
  public static boolean load(Class<?> config, String filename) {

    Properties properties = new Properties();

    String propertyFilePath = Thread.currentThread().getContextClassLoader().getResource(filename).getFile();
    try {

      try (InputStream fis = new FileInputStream(propertyFilePath)) {
        properties.load(fis);
      }

      Field[] fields = config.getFields();
      for (Field field : fields) {

        if (!Modifier.isStatic(field.getModifiers())) {
          continue;
        }

        Parameter param = field.getAnnotation(Parameter.class);
        String paramName = param == null ? field.getName() : param.value();

        if (!properties.containsKey(paramName)) {
          continue;
        }

        Class<?> type = field.getType();
        String value = properties.getProperty(paramName);

        if (type == Integer.class || type == int.class) {
          field.set(config, Integer.parseInt(value));
        } else if (type == Long.class || type == long.class) {
          field.set(config, Long.parseLong(value));
        } else {
          field.set(config, value);
        }
      }
      return true;
    } catch (IllegalArgumentException | IllegalAccessException | IOException e) {
      return false;
    }
  }

  /**
   * 扫描一个包以及所有子包的类。
   * 
   * @param packageName
   * @return
   */
  public static Set<Class<?>> getAllClassesFrom(String packageName) {

    Set<Class<?>> classes = new LinkedHashSet<Class<?>>();

    String packagePath = getFilePath(packageName);
    if (packagePath == null) {
      return classes;
    }

    File packageFile = new File(packagePath);
    if (!packageFile.exists() || !packageFile.isDirectory()) {
      return classes;
    }

    File[] files = packageFile.listFiles(new FileFilter() {
      public boolean accept(File file) {
        return (file.isDirectory()) || (file.getName().endsWith(".class"));
      }
    });

    // 循环所有文件
    for (File file : files) {

      // 如果是目录 则继续扫描
      if (file.isDirectory()) {
        Set<Class<?>> subClasses = getAllClassesFrom(packageName + "." + file.getName());
        classes.addAll(subClasses);
        continue;
      }

      // 如果是java类文件 去掉后面的.class 只留下类名
      String className = file.getName().substring(0, file.getName().length() - 6);

      // 匿名内部类不添加
      if (className.indexOf("$") == -1) {

        try {
          classes.add(Class.forName(packageName + '.' + className));
        } catch (ClassNotFoundException e) {
          e.printStackTrace();
        }
      }
    }
    return classes;
  }

  private static String getFilePath(String packageName) {

    try {
      URL url =
          Thread.currentThread().getContextClassLoader().getResource(packageName.replace(".", "/"));

      if (url == null) {
        return null;
      }

      String protocol = url.getProtocol();
      if ("file".equals(protocol)) {
        String filePath = URLDecoder.decode(url.getFile(), "UTF-8");
        return filePath;
      }
      return null;
    } catch (IOException e) {
      return null;
    }

  }

}
