java -Xmx16G -XX:+UseConcMarkSweepGC tools.CreateDB GRAPH_NAME FOLDER_TO_SAVE_GRAPH
java -classpath .:../lib/* -Xmx16G -XX:+UseConcMarkSweepGC console.SkinnerCmd FOLDER_TO_SAVE_GRAPH