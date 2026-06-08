# "Apache Kafka" project for module 1: `Kafka Basics`

### Message to Reviewer
```txt
Ревьювер, привет!

В этом репозитории ты найдешь мой проект для: Модуль 1. Базовая эксплуатация Apache Kafka.

У меня к тебе есть 2 пожелания:
1. Пожалуйста, будь строг к ревью, ибо так я буду учиться и прогрессировать, даже если мне будет тяжело. Если у тебя нет критичных замечаний (конечно же есть, но вдруг), пожалуйста, добавь комментарии, что было бы здорово поправить, даже если это не критично.

2. В реальном мире мы бы куда-то хотели приземлить наш поток данных. Скажем, если у нас довольно много данных, например, 10 млн счетчиков генерят payload каждые 20 секунд. Для простоты расчетов возьмем payload = 50 bytes, это примерно 1.5 GB/min (30 mln payloads / min x 50 bytes). Куда бы ты приземлил такой поток, если цель агрегировать данные, передавать агрегаты в приложение, а также параллельным потоком их анализировать. Я думал про Clickhouse, так как он хорошо работает с большими time-series датасетами. Я мыслю сл стороны Data Engineer. Этот вопрос, наверно, за рамками курса, но мне было бы очень интересно услышать твое мнение.
```

### Repo structure
```
project/
├── kafka_app/
│   ├── producer.py ---> Kafka-producer class
│   └── consumer.py ---> Kafka-consumers classes
│   └── topic.txt ---> generate Kafka-topic and view topic info
│
├── utils/
│   └── energy_payload.py ---> business logic to generate payload
│
├── run_producer.py ---> run Kafka-producer
├── run_single_consumer.py ---> run SingleMessageConsumer
└── run_batch_consumer.py ---> run BatchMessageConsumer
│
├── docker-compose.yml ---> launch Kafka-cluster
```

### Application Launch
1. Запустить Kafka-cluster через terminal:
```bash
docker compose up -d
```
2. Создать топик выполнив команды из `kafka_app/topic.txt`
3. Запустить Producer из: `run_producer.py`
4. Запустить SingleMessageConsumer из `run_single_consumer.py`
5. Запустить BatchMessageConsumer из `run_batch_consumer.py`


### Classes Descriptions

#### KafkaProducer
Задает Kafka-Producer, имеет следующие методы:
1. delivery_report - выводит на экран инфо о доставке сообщений:
    - ошибку, если сообщение не было доставлено
    - если сообщение было доставлено - topic, partition и offset

2. send - отправляет сообщение в указанный topic. параметры:
    ⚠️ сериализует сообщение из JSON
    - topic - название топика, куда отправляем
    - value - само сообщение
    - key - ключ сообщения (опционально)

#### KafkaConsumer
Общий класс для консьюмеров. Методы:
1. deserialize_json - принимает сообщение и десериализует его в json
2. close - закрывает консьюмера

#### SingleMessageConsumer
Класс для обработки сообщений по одному с автокоммитом. Методы:

1. consume_one
    - считывает по одному сообщению
    - десериализует его
    - выводит полученное сообщение на консоль
    - если возникли проблемы с десериализацей -> выводит на консоль
    - коммитит оффсет автоматически


#### BatchMessageConsumer
Класс для обработки сообщений batch'ем (пачкой). Методы:
1. consume_batch
    - Принимает аргументом batch_size (опционально, по умолчанию 10) и timeout (опционально, по умолчанияю 1.0)
    - считывает сообщение batch'ем
    - обрабатывает сообщения в цикле
        - десериализует сообщение
        - выводит полученное сообщение на консоль
        - если возникли проблемы с десериализацей -> выводит на консоль
        - добавляет сообщение в список
    - возвращает список прочитанных сообщений

