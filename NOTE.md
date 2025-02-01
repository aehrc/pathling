





### For build to work (with Java17):

```bash
export MAVEN_OPTS="-Xmx4g -ea -Duser.timezone=UTC --add-exports=java.base/sun.nio.ch=ALL-UNNAMED --add-opens=java.base/java.net=ALL-UNNAMED"
```

Possibly change the way test data are generated to use 'exec' instead of java and pass the options there.
