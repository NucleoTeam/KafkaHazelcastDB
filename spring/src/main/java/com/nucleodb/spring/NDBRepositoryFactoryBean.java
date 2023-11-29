package com.nucleodb.spring;

import com.nucleocore.library.NucleoDB;
import com.nucleocore.library.database.utils.exceptions.IncorrectDataEntryClassException;
import com.nucleocore.library.database.utils.exceptions.MissingDataEntryConstructorsException;
import org.springframework.data.repository.core.support.RepositoryFactoryBeanSupport;
import org.springframework.data.repository.core.support.RepositoryFactorySupport;
import org.springframework.data.repository.Repository;
import org.springframework.lang.NonNull;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;


public class NDBRepositoryFactoryBean<T extends Repository<S, ID>, S, ID>
    extends RepositoryFactoryBeanSupport<T, S, ID>{

  private @NonNull String[] scanPackages;
  private @NonNull String kafkaServers;
  private @NonNull NucleoDB.DBType dbType;


  private static @Nullable NucleoDB nucleoDB = null;

  /**
   * Creates a new {@link RepositoryFactoryBeanSupport} for the given repository interface.
   *
   * @param repositoryInterface must not be {@literal null}.
   */
  protected NDBRepositoryFactoryBean(Class<? extends T> repositoryInterface) throws IncorrectDataEntryClassException, MissingDataEntryConstructorsException {
    super(repositoryInterface);
  }
  @Override
  protected RepositoryFactorySupport createRepositoryFactory() {
    if(nucleoDB==null) {
      try {
        nucleoDB = new NucleoDB(
            kafkaServers,
            dbType,
            scanPackages
        );
      } catch (IncorrectDataEntryClassException e) {
        throw new RuntimeException(e);
      } catch (MissingDataEntryConstructorsException e) {
        throw new RuntimeException(e);
      }
    }
    return new NDBRepositoryFactory(nucleoDB);
  }


  @Override
  public boolean isSingleton() {
    return true;
  }

  @Override
  public void afterPropertiesSet() {
    super.afterPropertiesSet();
    Assert.notNull(nucleoDB, "NucleoDB must not be null");
  }

  public void setScanPackages(@NonNull String[] scanPackages) {
    this.scanPackages = scanPackages;
  }

  public void setKafkaServers(@NonNull String kafkaServers) {
    this.kafkaServers = kafkaServers;
  }

  public void setDbType(@NonNull NucleoDB.DBType dbType) {
    this.dbType = dbType;
  }
  //  @Bean
//  public NucleoDB createNucleoDB(ApplicationContext ctx) throws IncorrectDataEntryClassException, MissingDataEntryConstructorsException {
//
//    return
//
//  }
}
