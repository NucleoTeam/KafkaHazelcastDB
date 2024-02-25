## Overview
NucleoDB is an innovative in-memory, embedded database system designed to provide high-speed data management and processing. Its in-memory architecture ensures rapid access and manipulation of data, making it ideal for applications where performance and quick response times are critical. Being embedded, NucleoDB seamlessly integrates into various applications, offering a streamlined and efficient way to handle data within the application's own environment. This design approach not only simplifies the application architecture but also enhances overall system performance by reducing the need for external data calls.

 * Spring Repository
   * https://github.com/NucleoTeam/NucleoDB-Spring

### Installation

##### Dependencies

* Kafka Cluster
  * /docker/kafka/docker-compose.yml

##### Import library
```groovy
repositories {
    mavenCentral()
    maven { url "https://nexus.synload.com/repository/maven-repo-releases/" }
}
dependencies {
    implementation 'com.nucleodb:library:1.15.11'
}
```

##### Initializing DB
```java
import com.nucleodb.library.NucleoDB;

class Application{
  public static void main(String[] args) {
    NucleoDB nucleoDB = new NucleoDB(
        NucleoDB.DBType.NO_LOCAL,
        "com.package.string"
    );
  }
}
```

```java

import java.io.Serializable;

@Table(tableName = "author", dataEntryClass = AuthorDE.class)
public class Author implements Serializable {
  @Index
  String name;
  public Author(String name) {
    this.name = name;
  }
  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }
}

public class AuthorDE extends DataEntry<Author>{
  
}
```

##### Read data for read only
```java
class Application {
  public static void main(String[] args) {
    Set<DataEntry> first = nucleoDB.getTable(Author.class).get("name", "test");
  }
}
```

##### Read data for updating or deleting without needing to use .copy()
```java
class Application {
  public static void main(String[] args) {
    Set<DataEntry> first = nucleoDB.getTable(Author.class).get("name", "test", new DataEntryProjection(){{
      setWritable(true);
      setLockUntilWrite(true);
    }});
  }
}
```

##### Create data entry

```java
class Application{
  public static void main(String[] args) {
    AuthorDE test = new AuthorDE(new Author("test"));
    nucleoDB.getTable(Author.class).saveSync(test);
  }
}
```

##### Update data entry

```java
class Application{
  public static void main(String[] args) {
    // read data 
    // AuthorDE author = AuthorDE()
    // author.copy(AuthorDE.class, true); // lock entry for updating
    author.getData().setName("Edgar Allen Poe");
    nucleoDB.getTable(Author.class).saveSync(author);
  }
}
```

##### Delete data

```java
class Application{
  public static void main(String[] args) { 
    // read data 
    // AuthorDE author = AuthorDE()
    // author.copy(AuthorDE.class, true); // lock entry for deletion
    nucleoDB.getTable(Author.class).deleteSync(author);
  }
}
```
