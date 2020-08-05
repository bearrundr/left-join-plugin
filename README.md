## 소개
join plugin

## 실행방법

7.6.2 기준 java 11 사용

### 자바 퍼미션 추가

${JAVA_HOME}/conf/security/java.policy 아래 내용 추가

```
permission java.lang.RuntimePermission "createClassLoader";
```

### idea 실행 환경

```
Main class: org.elasticsearch.bootstrap.Elasticsearch

VM options: -Des.path.conf=<join-plugin>\config -Des.path.home=<join-plugin> -Dlog4j2.disable.jmx=true
```

