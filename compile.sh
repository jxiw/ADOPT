JAVA_FILES=$(find . -name "*.java")
javac -d bin -sourcepath src/ -classpath .:lib/* $JAVA_FILES
cd bin