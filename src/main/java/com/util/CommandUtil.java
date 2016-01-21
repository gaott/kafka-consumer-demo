package com.util;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

public class CommandUtil {

  public static List<String> processCommand(String cmdStr) {
    String[] realcmd = new String[] {"/bin/bash", "-c", cmdStr};
    ProcessBuilder pb = new ProcessBuilder(realcmd);
    pb.redirectErrorStream(true);
    Process process = null;

    List<String> result = new ArrayList<String>();
    try {
      process = pb.start();
      BufferedReader input = new BufferedReader(new InputStreamReader(process.getInputStream()));
      String line = "";
      while ((line = input.readLine()) != null) {
        result.add(line);
      }
      input.close();
    } catch (IOException e) {
    }

    return result;
  }
}
