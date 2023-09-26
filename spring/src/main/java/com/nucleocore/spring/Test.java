package com.nucleocore.spring;




import com.nucleocore.library.database.utils.Serializer;
import org.neo4j.cypherdsl.core.Statement;
import org.neo4j.cypherdsl.core.StatementCatalog;
import org.neo4j.cypherdsl.parser.CypherParser;

public class Test{
  public static void main(String[] args) {
    Statement statement = CypherParser.parse("MATCH (n:anime{name:'test'})-[:relation1]->(:actor) DELETE n");
    StatementCatalog catalog = statement.getCatalog();
    catalog.getNodeLabels().forEach(i-> Serializer.log(i.value()));
    catalog.getAllFilters().forEach(i->Serializer.log(i));
    Serializer.log(catalog.getRelationshipTypes());
    for (StatementCatalog.Filter filter : catalog.getAllFilters()) {
      if(filter instanceof StatementCatalog.PropertyFilter){
        Serializer.log(((StatementCatalog.PropertyFilter) filter).right());
      }
    }
  }
}
