package com.nucleocore.test.spring;

import com.nucleocore.library.NucleoDB;
import com.nucleocore.spring.repository.config.EnableNDBRepositories;
import com.nucleocore.spring.repository.NDBConfiguration;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;

@SpringBootApplication
@EnableNDBRepositories(
    dbType = NucleoDB.DBType.ALL,
    kafkaServers = "127.0.0.1:19092,127.0.0.1:29092,127.0.0.1:39092",
    scanPackages = {
        "com.nucleocore.test.domain"
    },
    basePackages = "com.nucleocore.test.spring.repo"
)
@ComponentScan(basePackages = {"com.nucleocore.spring.repository", "com.nucleocore.test.spring"})
public class SpringRepoTest{
    public static void main(String[] args){
        SpringApplication.run(SpringRepoTest.class);
    }
    @Bean
    public NDBConfiguration createNDBConfig(){
        return new NDBConfiguration("com.nucleocore.test.spring.repo");
    }
}
