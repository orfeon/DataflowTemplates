mvn archetype:generate \
      -DarchetypeGroupId=org.apache.beam \
      -DarchetypeArtifactId=beam-sdks-java-maven-archetypes-examples \
      -DarchetypeVersion=2.6.0 \
      -DgroupId=net.orfeon.cloud \
      -DartifactId=templates \
      -Dversion="0.1" \
      -Dpackage=net.orfeon.cloud.dataflow \
      -DinteractiveMode=false