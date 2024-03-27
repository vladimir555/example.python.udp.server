Разработать программу под ОС Linux, реализующую функционал:
- worker процессы: должны принимать UDP сообщения формата json на порт с одним номером (структуру см. далее)
- master процесс:
- агрегирует собранную информацию от всех worker-ов
- считает и записывает метрики по полученным данным в файл за интервалы: 10 сек, 60
сек.
Формат входных сообщений:
{"A1":<integer>, "A2":<integer>, "A3":<integer>}
Формат записи в файл:
{
"timestamp": <метка времени в секундах>,
"count_type": <"10s" или "60s" - если запись за 10 или 60 сек. соответственно>,
"A1_sum": <сумма значений A1 за интервал count_type>,
"A2_max": <максимальное A2 за интервал count_type>,
"A3_min": <минимальное A3 за интервал count_type>
}
Допустим, сообщения приходили первые 10 сек:
{"A1":1, "A2":10, "A3":100}
{"A1":2, "A2":12, "A3":102}
{"A1":3, "A2":30, "A3":130}
Вторые 10 сек:
{"A1":4, "A2":20, "A3":200}
{"A1":5, "A2":13, "A3":103}
{"A1":6, "A2":40, "A3":140}
Записанный файл:
{"timestamp":123456710, "count_type":"10s", "A1_sum": 6, "A2_max":30, "A3_min": 100}
{"timestamp":123456720, "count_type":"10s", "A1_sum": 15, "A2_max":40, "A3_min": 103}
{"timestamp":123456730, "count_type":"10s", "A1_sum": 0, "A2_max":0, "A3_min": 0}
{"timestamp":123456740, "count_type":"10s", "A1_sum": 0, "A2_max":0, "A3_min": 0}
{"timestamp":123456750, "count_type":"10s", "A1_sum": 0, "A2_max":0, "A3_min": 0}
{"timestamp":123456760, "count_type":"10s", "A1_sum": 0, "A2_max":0, "A3_min": 0}
{"timestamp":123456760, "count_type":"60s", "A1_sum": 21, "A2_max":40, "A3_min": 100}
Программа должна исполняться под управлением Python и эффективно использовать процес-
сорное время (на каждом ядре), выполняя операции ввода/вывода и подсчетов по метрикам.


Develop a program for Linux OS that implements the following functionality:
- worker processes: must receive UDP messages in json format on a port with one number
rum (see below for structure)
- master process:
- aggregates collected information from all workers
- counts and writes metrics based on the received data to a file at intervals: 10 seconds, 60
sec.
Input message format:
{"A1":<integer>, "A2":<integer>, "A3":<integer>}
File recording format:
{
"timestamp": <time stamp in seconds>,
"count_type": <"10s" or "60s" - if the recording is for 10 or 60 seconds. accordingly>,
"A1_sum": <sum of A1 values ​​for the interval count_type>,
"A2_max": <maximum A2 per interval count_type>,
"A3_min": <minimum A3 for interval count_type>
}
Let’s say the messages arrived within the first 10 seconds:
{"A1":1, "A2":10, "A3":100}
{"A1":2, "A2":12, "A3":102}
{"A1":3, "A2":30, "A3":130}
Second 10 sec:
{"A1":4, "A2":20, "A3":200}
{"A1":5, "A2":13, "A3":103}
{"A1":6, "A2":40, "A3":140}
Recorded file:
{"timestamp":123456710, "count_type":"10s", "A1_sum": 6, "A2_max":30, "A3_min": 100}
{"timestamp":123456720, "count_type":"10s", "A1_sum": 15, "A2_max":40, "A3_min": 103}
{"timestamp":123456730, "count_type":"10s", "A1_sum": 0, "A2_max":0, "A3_min": 0}
{"timestamp":123456740, "count_type":"10s", "A1_sum": 0, "A2_max":0, "A3_min": 0}
{"timestamp":123456750, "count_type":"10s", "A1_sum": 0, "A2_max":0, "A3_min": 0}
{"timestamp":123456760, "count_type":"10s", "A1_sum": 0, "A2_max":0, "A3_min": 0}
{"timestamp":123456760, "count_type":"60s", "A1_sum": 21, "A2_max":40, "A3_min": 100}
The program must run under Python and make efficient use of the process.
waste time (on each core), performing I/O operations and calculations based on metrics.
