# SparkDeveloperHomeWork7
- Построить модель классификации Ирисов Фишера и сохранить её.  
Описание набора данных: https://ru.wikipedia.org/wiki/%D0%98%D1%80%D0%B8%D1%81%D1%8B_%D0%A4%D0%B8%D1%88%D0%B5%D1%80%D0%B0  
Набор данных в формате CSV: https://www.kaggle.com/arshid/iris-flower-dataset  
Набор данных в формате LIBSVM: https://github.com/apache/spark/blob/master/data/mllib/iris_libsvm.txt  
Должен быть предоставлен код построения модели (ноутбук или программа)  
- Разработать приложение, которое читает из одной темы Kafka (например, "input") CSV-записи с четырьмя признаками ирисов,  
и возвращает в другую тему (например, "predictition") CSV-записи с теми же признаками и классом ириса.  
Должен быть предоставлен код программы.  
**(Используя DStream)**  
***  
 - Kafka через docker-compose (https://github.com/Gorini4/kafka_scala_example)   
 - Создание топиков  
./kafka-topics.sh -create -topic input -bootstrap-server localhost:29092  
./kafka-topics.sh -create -topic predictition -bootstrap-server localhost:29092
- Iris.scala код построения модели,сохранение в папку src/main/resources/modelLast  
- Отправка в топик input  записей для классификации  
src/main/resources/send//output.sh | ./kafka-console-producer.sh --bootstrap-server localhost:29092 --topic input
- ./kafka-console-consumer.sh --bootstrap-server localhost:29092 --topic predictition
