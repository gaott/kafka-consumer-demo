package com;

import com.job.Engine;

import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

/**
 * 
 * 项目入口
 *
 */
public class StartUp {
  
  private static ApplicationContext context = null;

  public static synchronized ApplicationContext getContext(){
    if (context == null){
      context = new ClassPathXmlApplicationContext("classpath:applicationContext.xml");
    }
    return context;
  }

  public static void main(String[] args) {
    Engine consumerEngine = (Engine) getContext().getBean("engine");
    consumerEngine.process();
  }

}
