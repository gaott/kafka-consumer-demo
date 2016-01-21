package com.util;

public class Pair<F, S> {

  private F f;
  private S s;

  public Pair(F f, S s) {
    this.f = f;
    this.s = s;
  }

  public F first(){
    return f;
  }

  public S Second(){
    return s;
  }
}
