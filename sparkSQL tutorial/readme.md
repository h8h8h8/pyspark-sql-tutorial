# jdbc

For using .db files you need to have the jdbc connection for sqlite we use jar. files for necessary download please visit
https://repo1.maven.org/maven2/org/xerial/sqlite-jdbc/3.28.0/


# pyspark

PySpark is a Spark library written in Python to run Python application using Apache Spark capabilities, using PySpark we can run applications parallelly on the distributed cluster (multiple nodes).

## Installation

pip install pyspark


```bash
pip install pyspark
```

## Usage

```python
import pyspark
from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql import SparkSession

# Creating spark session
spark=SparkSession.builder.master('local[*]').getOrCreate()

# For connecting db
sc = SparkContext.getOrCreate()
sqlCtx = SQLContext(sc)
df_ratings=sqlCtx.read.format("jdbc").options(url ="jdbc:sqlite:movielens-small.db", driver="org.sqlite.JDBC", dbtable="ratings").load()


# Sample spark sql usage
df_ratings= spark.sql("SELECT * from df_ratings")
```

## Contributing
Pull requests are welcome. For major changes, please open an issue first to discuss what you would like to change.

Please make sure to update tests as appropriate.

## License
[MIT](https://samplelicense.com/licenses/mit/)


