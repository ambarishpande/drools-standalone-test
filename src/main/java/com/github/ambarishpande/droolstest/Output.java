package com.github.ambarishpande.droolstest;

import java.io.Serializable;

import com.datatorrent.cep.schema.Transaction;

public class Output implements Serializable
{
  public void emit(Object t)
  {
    System.out.println(((Transaction) t).getTransId());
  }
}