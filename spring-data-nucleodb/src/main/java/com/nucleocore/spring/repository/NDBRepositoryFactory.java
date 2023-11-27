package com.nucleocore.spring.repository;

import com.nucleocore.library.NucleoDB;
import com.nucleocore.library.database.utils.Serializer;
import com.nucleocore.library.examples.anime.definitions.AnimeDE;
import com.nucleocore.spring.repository.query.MappingNDBEntityInformation;
import org.springframework.data.projection.ProjectionFactory;
import org.springframework.data.repository.core.EntityInformation;
import org.springframework.data.repository.core.NamedQueries;
import org.springframework.data.repository.core.RepositoryInformation;
import org.springframework.data.repository.core.RepositoryMetadata;
import org.springframework.data.repository.core.support.RepositoryFactorySupport;
import org.springframework.data.repository.query.QueryLookupStrategy;
import org.springframework.data.repository.query.QueryMethod;
import org.springframework.data.repository.query.QueryMethodEvaluationContextProvider;
import org.springframework.data.repository.query.RepositoryQuery;
import org.springframework.expression.spel.standard.SpelExpressionParser;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Optional;

public class NDBRepositoryFactory extends RepositoryFactorySupport{
  private static final SpelExpressionParser EXPRESSION_PARSER = new SpelExpressionParser();

  private final NucleoDB nucleoDB;

  /**
   * Create a new {@link NDBRepositoryFactory} with the given {@link NucleoDB}.
   *
   * @param nucleoDB must not be {@literal null}
   */
  public NDBRepositoryFactory(NucleoDB nucleoDB) {

    Assert.notNull(nucleoDB, "NucleoDB must not be null");

    this.nucleoDB = nucleoDB;
  }

  @Override
  public <T, ID> EntityInformation<T, ID> getEntityInformation(Class<T> domainClass) {
    //return new MappingNDBEntityInformation();
    return null;
  }

  @Override
  protected Object getTargetRepository(RepositoryInformation metadata) {
    return getTargetRepositoryViaReflection(metadata, nucleoDB, metadata.getDomainType());
  }

  @Override
  protected Class<?> getRepositoryBaseClass(RepositoryMetadata metadata) {
    return NDBRepositoryImpl.class;
  }


  @Override
  protected Optional<QueryLookupStrategy> getQueryLookupStrategy(
      @Nullable QueryLookupStrategy.Key key,
      QueryMethodEvaluationContextProvider evaluationContextProvider
  ) {
    return Optional.of(new NDBQueryLookupStrategy(nucleoDB, evaluationContextProvider));
  }

  private static class NDBQueryLookupStrategy implements QueryLookupStrategy {

    private final QueryMethodEvaluationContextProvider evaluationContextProvider;
    private final NucleoDB nucleoDB;

    NDBQueryLookupStrategy(NucleoDB nucleoDB, QueryMethodEvaluationContextProvider evaluationContextProvider) {
      this.nucleoDB = nucleoDB;
      this.evaluationContextProvider = evaluationContextProvider;
    }

    @Override
    public RepositoryQuery resolveQuery(
        Method method,
        RepositoryMetadata metadata,
        ProjectionFactory factory,
        NamedQueries namedQueries
    ) {

      return new RepositoryQuery(){
        @Override
        public Object execute(Object[] parameters) {
          Serializer.log(method.getName());
          Serializer.log(parameters);

          return null;
        }

        @Override
        public QueryMethod getQueryMethod() {
          return new QueryMethod(method, metadata, factory);
        }
      };
    }
  }

}