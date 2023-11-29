package com.nucleodb.spring.query;

import com.nucleocore.library.database.tables.table.DataEntry;
import org.springframework.data.repository.core.EntityInformation;
import org.springframework.data.repository.core.EntityMetadata;

public interface NDBEntityMetadata<T extends DataEntry, ID> extends EntityInformation<T, ID>, EntityMetadata<T>{
  @Override
  default ID getId(T entity) {
    return (ID) entity.getKey();
  }
}