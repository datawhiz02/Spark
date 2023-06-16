def countWords(sc):
    import re, nltk
        
    from nltk.corpus import stopwords
    
    stopwords = stopwords.words('english')
    
    file = 'book1.txt'
    
    lines = sc.textFile(file)
    counts = lines.flatMap(lambda x: x.split(' ')) \
            .map(lambda x: re.sub(r'[^\w\s]', '', x)) \
            .map(lambda x: (x.lower(),1)) \
            .filter(lambda x: x[0] not in stopwords + ['']) \
            .reduceByKey(lambda a,b: a+b) \
            .sortBy(lambda pair: pair[1], ascending=False)
    
#     counts.saveAsTextFile("C:/DIC-Project/HW2/output")
    return counts

if __name__ == '__main__':
    from pyspark.context import SparkContext
    sc = SparkContext('local', 'test')
    
    results.saveAsTextFile("C:/DIC-Project/HW2/output")