package com.nucleocore.spring.repository.config;

import com.nucleocore.library.NucleoDB;
import com.nucleocore.library.database.tables.annotation.Table;
import com.nucleocore.spring.repository.NDBRepositoryFactoryBean;
import com.nucleocore.spring.repository.NDBRepository;
import org.springframework.beans.factory.support.BeanDefinitionBuilder;
import org.springframework.data.repository.config.AnnotationRepositoryConfigurationSource;
import org.springframework.data.repository.config.RepositoryConfigurationExtensionSupport;
import org.springframework.data.repository.config.XmlRepositoryConfigurationSource;
import org.springframework.data.repository.core.RepositoryMetadata;

import java.lang.annotation.Annotation;
import java.util.Collection;
import java.util.Collections;
import java.util.Optional;

public class NDBRepositoryConfigurationExtension extends RepositoryConfigurationExtensionSupport{
  @Override
  public String getModuleName() {
    return "NucleoDB";
  }

  @Override
  protected String getModulePrefix() {
    return "nucleodb";
  }


  @Override
  public String getRepositoryFactoryBeanClassName() {
    return NDBRepositoryFactoryBean.class.getName();
  }

  @Override
  public void postProcess(BeanDefinitionBuilder builder, XmlRepositoryConfigurationSource config) {

  }

  @Override
  public void postProcess(BeanDefinitionBuilder builder, AnnotationRepositoryConfigurationSource config) {
    Optional<String> kafkaServers = config.getAttribute("kafkaServers");
    if(kafkaServers.isPresent()){
      builder.addPropertyValue("kafkaServers", kafkaServers.get());
    }
    Optional<NucleoDB.DBType> dbType = config.getAttribute("dbType", NucleoDB.DBType.class);
    if(dbType.isPresent()){
      builder.addPropertyValue("dbType", dbType.get());
    }
    Optional<String[]> scanPackages = config.getAttribute("scanPackages", String[].class);
    if(scanPackages.isPresent()){
      builder.addPropertyValue("scanPackages", scanPackages.get());
    }
  }

  @Override
  protected Collection<Class<? extends Annotation>> getIdentifyingAnnotations() {
    return Collections.singleton(Table.class);
  }

  @Override
  protected Collection<Class<?>> getIdentifyingTypes() {
    return Collections.singleton(NDBRepository.class);
  }

  @Override
  protected boolean useRepositoryConfiguration(RepositoryMetadata metadata) {
    return !metadata.isReactiveRepository();
  }
}