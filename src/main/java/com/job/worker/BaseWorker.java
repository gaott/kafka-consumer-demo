package com.job.worker;

import static org.iq80.leveldb.impl.Iq80DBFactory.bytes;

import org.iq80.leveldb.DB;
import org.iq80.leveldb.DBException;
import org.json.JSONException;
import org.json.JSONObject;
import org.springframework.context.annotation.Scope;

import com.util.Log;


@Scope("prototype")
public abstract class BaseWorker implements Runnable {
  
  public String topic;

  public String message;
  
  public DB leveldb;
  
  // 当前任务处理状态
  private boolean isWorking = true;

  public boolean getWorkingStatus() {
    return isWorking;
  }

  public void setProcessorWorking(boolean status) {
    isWorking = status;
  }

  @Override
  public void run() {
    if (!isWorking) {
      Log.e(name() + " is working now !");
      return;
    }
    
    if (message == null || message.trim().isEmpty()) {
      Log.e(name() + " Invalid Message");
    }
    
    Log.i(name() + " is running.");
    try {
      
      handle();
      
    } catch (Exception e) {
      Log.e(e);
      if (e instanceof JSONException) {
        return;
      }
      
      // 任务处理失败后，保存消息至LevelDB
      try {
        saveMessage();
      } catch (DBException | JSONException e1) {
        Log.f(String.format("[SAVE_MESSAGE_FAIL] - %s - %s", topic, message));
        Log.e(e1);
      }
      return;
    }
    Log.i(name() + " has finished.");
  }
  
  /**
   * 在此方法中，完成任务处理逻辑
   */
  public abstract void handle() throws Exception;

  /**
   * worker 名称
   */
  public abstract String name();
  
  /**
   * 保存消息
   * 
   * @return levelDB 存储 message 对应的key
   * 
   * @throws DBException
   * @throws JSONException
   */
  public String saveMessage() throws DBException, JSONException{
    String key = String.format("%s_%s_%s", topic, this.getClass().getSimpleName(), System.currentTimeMillis());
    
    leveldb.put(bytes(key), bytes(new JSONObject()
                                  .put("topic", topic)
                                  .put("value", message)
                                  .toString()));
    return key;
  }

  /**
   * 删除 levelDB 中指定key对应的消息
   * 
   * @param key
   */
  public void deleteMessage(String key){
    leveldb.delete(bytes(key));
  }
  
}
