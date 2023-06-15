package com.sarwarbhuiyan.kafka.tools;

import java.time.Instant;
import picocli.CommandLine.ITypeConverter;

public class DateTimeConverter implements ITypeConverter<Instant> {
  
  public DateTimeConverter() {}

  @Override
  public Instant convert(String value) throws Exception {
    Instant result = Instant.parse(value);
    
    return result;
  }
  
}
