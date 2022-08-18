lst=('a','b','a','b')
sep=map(lambda lst = lst.split(',') )

from pyspark.sql.session import sparksession

spark=sparksession.bulder().appName("test").createOrDelete()

