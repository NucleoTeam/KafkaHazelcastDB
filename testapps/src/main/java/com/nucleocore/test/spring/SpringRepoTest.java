package com.nucleocore.test.spring;

import com.nucleocore.library.NucleoDB;
import com.nucleocore.library.database.utils.exceptions.IncorrectDataEntryClassException;
import com.nucleocore.library.database.utils.exceptions.MissingDataEntryConstructorsException;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;

@SpringBootApplication
@ComponentScan(value = {"com.nucleocore.spring.repository", "com.nucleocore.test.spring"})
public class SpringRepoTest{
    public static void main(String[] args){
        new SpringApplicationBuilder().sources(SpringRepoTest.class).run(args);
    }
    @Bean
    public NucleoDB createNucleoDB() throws IncorrectDataEntryClassException, MissingDataEntryConstructorsException {
        return new NucleoDB(
            "127.0.0.1:19092,127.0.0.1:29092,127.0.0.1:39092",
            NucleoDB.DBType.ALL,
            "com.nucleocore.test.common",
            "com.nucleocore.library.database.tables.connection"
        );

    }
}
