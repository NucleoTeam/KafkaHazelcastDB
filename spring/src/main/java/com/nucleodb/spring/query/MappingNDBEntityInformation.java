package com.nucleodb.spring.query;

import com.nucleocore.library.database.tables.table.DataEntry;
import org.springframework.data.mapping.PersistentEntity;
import org.springframework.data.mapping.PersistentProperty;
import org.springframework.data.repository.core.support.PersistentEntityInformation;

public class MappingNDBEntityInformation<T extends DataEntry, ID> extends PersistentEntityInformation<T, ID> implements NDBEntityMetadata<T, ID> {

  public MappingNDBEntityInformation(PersistentEntity<T, ? extends PersistentProperty<?>> persistentEntity) {
    super(persistentEntity);
  }
}
		