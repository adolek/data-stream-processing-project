# Data Stream Processing Project
 
This project is a real-time streaming application using Kafka for stream processing. It aims to predict the prices of 3 stocks (Google, Meta and Amazon) using global index data streams of each of 3 regions (Europe, Asia and America). 

The objective is to compare the performances of online (or adapative) regression models with batch regression models.

The technologies and libraries used are:
- Kafka
- River
- Scikit-learn

## Getting Started

These instructions will get you a copy of the project up and running on your local machine.

1. Clone the repository to your local machine
```bash
git clone https://github.com/adolek/data-stream-processing-project
```

2. Install the necessary Python packages 
```bash
pip install -r requirements.txt
```

3. Make sure Kafka is running on your local machine
```bash
bin/zookeeper-server-start.sh config/zookeeper.properties
bin/kafka-server-start.sh config/server.properties
```

4. Run in separate terminals the following scripts:
```bash
python3 stock-producer.py
python3 stock-consumer.py
```

5. Play with the notebook with batch and online regression models
```bash
batch_and_online_learning.ipynb
```

## Authors

* **Adrien Oleksiak** - *M2DS IP Paris, Ecole Polytechnique* 

* **Elia Lejzerowicz** - *M2DS IP Paris, Ecole Polytechnique* 

## License

This project is licensed under the MIT License - see the [LICENSE.md](LICENSE.md) file for details.
