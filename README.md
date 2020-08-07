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

### left join 쿼리

기존 _search 쿼리와 동일하게 요청하면 되지만, child 데이터를 추가 하기위해서는 join 필드가 필요합니다.   

#### join 필드


| 필드명 | 설명 |
| --- | --- |
|index | 서브 인덱스명 |
|parent | 메인 쿼리의 문서의 필드명 |
|child | 메인쿼리와 조인할 필드명 |
|should | 서브 인덱스명에서 검색시 조건 추가 |
|minimum_should_match | 서브 인덱스명에서 검색시 최소 매칭 값 |


문법)
```
GET /parent/_left
{
  "query": {
    "bool": {
      "should": [
        {
          "match": {
            "column1": "a"
          }
        }
      ]
    }
  },
  "join": [  
    {
      "index": "child",
      "parent": "fk",
      "child": "pk",
      "should": [
        {
          "term": {
            "column3": "b"
          }
        }
      ],
      "minimum_should_match": 0
    }
  ]
}
```